#==============================================================================
"""
    process_harvest_queue.py

    Written by C. Thomas
    Development start date 12-28-19

    Grabs HarvestInstance records from the HarvestQueue and 
    processes each one.  Meant to be scalable such that many
    processing nodes could be running this script at the same
    time.

"""
#==============================================================================

import boto3
import os, io, sys, re
import time, datetime
import argparse, requests
import hashlib
import pprint
import urllib.parse

import atexit 
from sys import exit

sys.path.append(os.getcwd())
from proc_mongo import *
from sqs_utils import *
from s3_utils import *
from ad_utils import *
from validate_schema import *


#==============================================================================
#    C O N F I G
#==============================================================================
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/757768816950/HARVEST_INSTANCE_PROCESSING.fifo'
cwd = os.path.dirname(os.path.realpath(__file__))
LOGFILE = cwd + '/logs/process_harvest_queue.log'
MAX_URL_LEN = 512
DEBUG = True


#==============================================================================
#    P A R S E R
#==============================================================================
parser = argparse.ArgumentParser(description='Process HarvestInstance records in the Harvest Instance processing queue.')
parser.add_argument('--num_to_process', help='Number of messages to process.')
parser.add_argument('--max_sleeps', help='Number of sleeps to do before we quit.')
parser.add_argument('--force', help='Update the page even if the entry already exists.')


#==============================================================================
#    G L O B A L S
#==============================================================================


#==============================================================================
#    S H U T D O W N  H A N D L E R
#==============================================================================
def shutdownHandler(dbh, errors):
  """
  shutdownHandler()
  """

  print('SIGINT or CTRL-C detected. Exiting gracefully.')
  print("Running handleProcErrors()...")
  handleProcErrors(dbh, errors) 

  exit(0)


#==============================================================================
#    P R O C E S S  P A G E
#==============================================================================
def processPage(dbh, sqs, full_id, full_url, added_dt, page_type='', lang=''):
  """
  processPage()
  """
  page_id = ''
  page_host = ''

  try:
    if (not fullURLExists(dbh, full_id)):

      added_str = added_dt.strftime('%Y-%m-%d %H:%M:%S')

      #===== Start an insert object
      insert = {};
      insert['id'] = full_id
      insert['url'] = full_url

      #===== Get PageID
      page_url = getPageFromURL(full_url)
      page_id = getMD5HashFromString(page_url)
      if (not pageExists(dbh, page_id)):
        writeToLog(LOGFILE, "Inserting Page: " + page_url)
        result = insertPageInfo(dbh, page_id, page_url, added_str);
      insert['page_id'] = page_id

      #===== Get HostID
      host_url = getHostFromURL(full_url)
      host_id = getMD5HashFromString(host_url)
      if (not hostExists(dbh, host_id)):
        writeToLog(LOGFILE, "Inserting Host: " + host_url)
        result = insertHostInfo(dbh, host_id, host_url, added_str)
      if (not pageExists(dbh, host_id)):
        writeToLog(LOGFILE, "Creating Page entry for Host: " + host_url)
        result = insertPageInfo(dbh, host_id, host_url, added_str);
      insert['host_id'] = host_id
 
      #===== Set the rest of the insert
      if (page_type and page_type == 'publ'):
        insert['publ'] = True
      elif (page_type and page_type == 'link'):
        insert['link'] = True
      insert['lang'] = lang
      insert['added_dt'] = added_dt

      #===== Insert the record
      result = insertFullURLInfo(dbh, insert);
      writeToLog(LOGFILE, "Inserted FullURL: " + full_url)

      #===== Queue Page URL in the HTML Queue
      if (page_id and page_url):
        writeToLog(LOGFILE, "Queuing Page " + page_id + " in HTML queue...")
        sendMessageToHTMLQueue(sqs, page_id, page_url, str(0), page_id)
      elif (not page_id):
        writeToLog(LOGFILE, "PageID " + page_id + " missing page_id!")
      elif (not page_url):
        writeToLog(LOGFILE, "PageID " + page_id + " missing page_url!")

      #===== Queue Host URL in HTML Queue
      if (host_url and host_id):
        writeToLog(LOGFILE, "Queuing Host " + host_id + " in HTML queue...")
        sendMessageToHTMLQueue(sqs, host_id, host_url, str(0), host_id)
      elif (not host_id):
        writeToLog(LOGFILE, "PageID " + page_id + " missing host_id!")
      elif (not host_url):
        writeToLog(LOGFILE, "PageID " + page_id + " missing host_url!")
     

    else:
      writeToLog(LOGFILE, "FullID: " + full_id + " already in DB.")

  except:
    raise

  return


#==============================================================================
#    I N C R E M E N T  E R R O R
#==============================================================================
def incrementError(errors, data_date, err_type):
  """
  incrementError()
  """

  try:
    if (not data_date in errors):
      errors[data_date] = {}

    if (err_type in errors[data_date]):
      errors[data_date][err_type] += 1
    else:
      errors[data_date][err_type] = 1
  except Exception as ex:
    raise

  return


#==============================================================================
#    H A N D L E  P R O C  E R R O R S
#==============================================================================
def handleProcErrors(dbh, errors):
  """
  handleProcErrors()
  """

  result = ''

  #===== Set up db object
  db = dbh['au_mongo']

  try:

    writeToLog(LOGFILE, "Updating SystemInfo collection with processing errors...")
    writeToLog(LOGFILE, pprint.pformat(errors))

    #==== Save the contents of the dict to the DB
    for data_date in list(errors.keys()):
      day_errors = errors[data_date]
      date_obj = getDatetimeObj(data_date + " 00:00:00")
      result = db.SystemInfo.update({'type': 'harvest_process_errors', \
                                     'data_date': date_obj}, \
                                    {'$inc': day_errors},
                                    upsert=True)
      #===== Reset the dictionary
      errors.clear()
      
  except Exception as ex:
    writeToLog(LOGFILE, "ERROR handleProcErrors() : " + str(ex))
    raise

  return(result)


#==============================================================================
#    M A I N
#==============================================================================

def main(args):
  """
  main()

  """

  loop = True
  start = ''
  end = ''
  processed = 0
  sleeps = 0

  #===== Create SQS client
  sqs = getSQSClient()

  #===== Create s3 client
  s3_obj = getS3Client()

  #===== Get the log directory
  cwd = os.path.dirname(os.path.realpath(__file__))
  logfile = cwd + '/logs/process_harvest_queue.log'

  #===== Start the processing timer
  start = datetime.datetime.now()

  #===== Begin processing loop
  while(loop):

    #===== Attempt to get a message
    message, receipt_handle = getMessageFromQueue(sqs, QUEUE_URL)

    #===== Process this message
    if (message):

      try:

        #===== Reset our sleeps counter
        sleeps = 0

        #===== Just delete it.  Worry about it later.
        deleteMessage(sqs, QUEUE_URL, receipt_handle)

        #===== Get info we need from message
        harvest_id = message['MessageAttributes']['id']['StringValue']
        writeToLog(logfile, "Processing " + harvest_id + "...")

        #===== Pull this HarvestInstance
        harvest_instance = getHarvestInstanceInfo(dbh, harvest_id)

        #===== Process it, if we got one
        if (harvest_instance):

          #===== Get the HarvestInstance record         
          inst_record = harvest_instance[harvest_id]

          #===== Skip if this instance was procesed
          if ('process_dt' in inst_record and \
              inst_record['process_dt'] and \
              not args.force):
            writeToLog(LOGFILE, "HarvestInstance " + harvest_id + " previously-processed.")
            continue # skip

          #===== Get the data_date for this record
          data_date = inst_record['harvest_dt'].strftime('%Y-%m-%d')
          incrementError(errors, data_date, 'harvest_instances')

          #===== Attempt to validate the schema
          validated, validation_errors = validateSchema(harvest_schema, inst_record)
          if (validated == False):
            writeToLog(LOGFILE, "HarvestInstance " + harvest_id + " failed validation.")
            writeToLog(LOGFILE, pprint.pformat(inst_record))
            writeToLog(LOGFILE, validation_errors)
            incrementError(errors, data_date, 'schema_validation_failure')
            continue # skip

          #===== Get info from HarvestInstance record
          element_url = inst_record['element_url']
          publisher_url = inst_record['publisher_url']
          linked_url = inst_record['linked_url']
          profile = inst_record['profile_name']
          harvest_dt = inst_record['harvest_dt']
          creative_type = inst_record['creative_type']

          #===== If we have no linked_url, but it's a video, queue it
          #      in the video queue
          if ((not linked_url) and (creative_type == 'video')):
            writeToLog(LOGFILE, "Adding harvest_id: " + harvest_id + " to Video queue...")
            dedup_id = harvest_id 
            sendMessageToVideoQueue(sqs, harvest_id, str(0), dedup_id)
            continue

          #===== See if a critical element is missing
          if (not element_url):
            incrementError(errors, data_date, 'no_element_url')
            writeToLog(logfile, "HarvestInstance " + harvest_id + " missing ElementURL.")
            updateHarvestProcDateTime(dbh, harvest_id, '1984-01-01 00:00:00')
            continue 
          if (not publisher_url):
            incrementError(errors, data_date, 'no_publisher_url')
            writeToLog(logfile, "HarvestInstance " + harvest_id + " missing PublisherURL.")
            updateHarvestProcDateTime(dbh, harvest_id, '1984-01-01 00:00:00')
            continue 
          if (not linked_url):
            incrementError(errors, data_date, 'no_linked_url')
            writeToLog(logfile, "HarvestInstance " + harvest_id + " missing LinkedURL.")
            updateHarvestProcDateTime(dbh, harvest_id, '1984-01-01 00:00:00')
            continue 

          #===== Try to get ad URL from Google stuff
          if (linked_url and ('googleadservices' in linked_url) or ('doubleclick' in linked_url)):
            fixed_url = fixGoogleURL(linked_url)
            if (fixed_url and (fixed_url != linked_url)):
              linked_url = fixed_url
            else:
              incrementError(errors, data_date, 'linked_url_unresolved')
              writeToLog(logfile, "HarvestInstance " + harvest_id + \
                                  " linked_url unresolved.");
              updateHarvestProcDateTime(dbh, harvest_id, '1984-01-01 00:00:00');
              continue 

          #===== Check URL lengths
          if (len(element_url) > MAX_URL_LEN):
            incrementError(errors, data_date, 'element_url_too_long')
            writeToLog(logfile, "HarvestInstance " + harvest_id + \
                                " element_url exceeds max length.");
            updateHarvestProcDateTime(dbh, harvest_id, '1984-01-01 00:00:00');
            continue 
          if (len(publisher_url) > MAX_URL_LEN):
            incrementError(errors, data_date, 'publisher_url_too_long')
            writeToLog(logfile, "HarvestInstance " + harvest_id + \
                                " publisher_url exceeds max length.");
            updateHarvestProcDateTime(dbh, harvest_id, '1984-01-01 00:00:00');
            continue 
          if (len(linked_url) > MAX_URL_LEN):
            incrementError(errors, data_date, 'linked_url_too_long')
            writeToLog(logfile, "HarvestInstance " + harvest_id + \
                                " linked_url exceeds max length.");
            updateHarvestProcDateTime(dbh, harvest_id, '1984-01-01 00:00:00');
            continue 

          #===== Handle image elements
          element_id = getMD5HashFromString(element_url)
          if (not elementExists(dbh, element_id)):
            if (not creative_type == 'video'):
              writeToLog(LOGFILE, "Adding element_id: " + element_id + " to Element queue...")
              dt_str = harvest_dt.strftime('%Y-%m-%d %H:%M:%S')
              retries = str(0)
              dedup_id = element_id
              sendMessageToElementQueue(sqs, \
                                        element_id, \
                                        element_url, \
                                        dt_str, \
                                        retries, 
                                        dedup_id)

          #===== Process Publisher Page
          publisher_id = getMD5HashFromString(publisher_url)
          processPage(dbh, sqs, publisher_id, publisher_url, harvest_dt, 'publ')
          publ_page_id = getFullURLPageID(dbh, publisher_id)
          publ_host_id = getFullURLHostID(dbh, publisher_id)

          #===== Process Linked Page
          linked_id = getMD5HashFromString(linked_url)
          processPage(dbh, sqs, linked_id, linked_url, harvest_dt, 'link')
          link_page_id = getFullURLPageID(dbh, linked_id)
          link_host_id = getFullURLHostID(dbh, linked_id)

          #===== Create new InstanceID
          harvest_dt_str = inst_record['harvest_dt'].strftime('%Y-%m-%d %H:%M:%S')
          strval = element_id + publisher_id + linked_id + profile
          strval = strval + harvest_dt_str
          instance_id = getMD5HashFromString(strval); 

          #===== Save an Instance record to the DB
          instance = {}
          instance['id'] = instance_id
          instance['crawl_id'] = inst_record['crawl_id']
          instance['device_info'] = inst_record['device_info']
          instance['device_name'] = inst_record['device_info']['name']
          instance['dt'] = inst_record['harvest_dt']
          instance['element_width'] = inst_record['creative_width']
          instance['element_height'] = inst_record['creative_height']
          instance['element_type'] = inst_record['creative_type']
          instance['platform'] = inst_record['environment']
          instance['profile'] = inst_record['profile_name']
          instance['record_version'] = inst_record['version']
          instance['element_id'] = element_id
          instance['harvest_id'] = harvest_id
          instance['linked_id'] = linked_id
          instance['link_page_id'] = link_page_id
          instance['link_host_id'] = link_host_id
          instance['harvest_id'] = harvest_id
          instance['publisher_id'] = publisher_id
          instance['publ_page_id'] = publ_page_id
          instance['publ_host_id'] = publ_host_id
          result = updateInstanceInfo(dbh, instance)
          writeToLog(LOGFILE, "Updated InstanceID: " + instance_id)
          processed += 1

          #===== Update
          if (processed % 1000 == 0):
            print(str(processed) + " instances processed...")

          #===== Mark the HarvestInstance as processed
          process_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
          updateHarvestProcDateTime(dbh, harvest_id, process_dt)

        else:
          data_date = datetime.datetime.now().strftime('%Y-%m-%d')
          incrementError(errors, data_date, 'no_record')
          writeToLog(LOGFILE, "Didn't get HarvestInstance for " + harvest_id)

        #===== Set a tentative end time
        if (not end):
          end = datetime.datetime.now()

      except Exception as e:
        writeToLog(logfile, str(e))
        handleProcErrors(dbh, errors)
        raise

    else:
      print("No messages in queue.  Sleeping.")
      time.sleep(15)
      sleeps += 1

      #==== Bail if we've slept too long
      if (args.max_sleeps and sleeps >= int(args.max_sleeps)):
        print("Hit " + str(sleeps) + " consecutive sleeps.  Exiting.")
        end = datetime.datetime.now()
        elapsed = end - start
        elapsed_secs = int(elapsed.total_seconds())
        instances_per_min = processed/(elapsed_secs/60.0)
        print("Processing time: " + str(elapsed_secs) + "s.")
        writeToLog(LOGFILE, "Processing time: " + str(elapsed_secs) + "s.")
        print("Processing rate:  " + str(round(instances_per_min, 2)) +  " instances per minute.")
        writeToLog(LOGFILE, "Processing rate:  " + \
                            str(round(instances_per_min, 2)) +  \
                            " instances per minute.")
        loop = False

  #===== Save accumulated errors to the DB
  handleProcErrors(dbh, errors)

  return


if __name__ == '__main__':

  errors = {}

  #===== Get DB handle
  dbh = getDBHandle()

  atexit.register(shutdownHandler, dbh, errors)

  args = parser.parse_args()
  print(vars(args))
  main(args)
  exit
