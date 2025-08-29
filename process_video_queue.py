#==============================================================================
"""
process_video_queue.py

Written by C. Thomas
Development began 2020-01-26

"""
#==============================================================================
import boto3
import sys,os
import argparse
import time, re, requests
import hashlib
import urllib.request
import magic

sys.path.append(os.getcwd())
from proc_mongo import *
from sqs_utils import *
from s3_utils import *
from ad_utils import *


#==============================================================================
#    C O N F I G
#==============================================================================
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/757768816950/VIDEO_PROCESSING.fifo'
S3_BUCKET = 'thousandx-image-data'
cwd = os.path.dirname(os.path.realpath(__file__))
LOGFILE = cwd + '/logs/process_video_queue.log'
REQUEST_TIMEOUT = 10
MAX_RETRIES = 1 
DEBUG = True



#==============================================================================
#    G L O B A L S
#==============================================================================


#==============================================================================
#    P A R S E R
#==============================================================================
parser = argparse.ArgumentParser(description='Process elements in the video processing queue.')
parser.add_argument('--num_to_process', help='Number of messages to process.')
parser.add_argument('--max_sleeps', help='Maximum consecutive sleeps before exit.')
parser.add_argument('--force', help='Force processing, even if the element is in the DB.')


#==============================================================================
#    G E T  V I D E O  F I L E  C H E C K S U M
#==============================================================================
def getVideoFileChecksum(filename):
  """
  getVideoFileChecksum()
  """
  
  checksum = ''
  blocksize = 2**20

  try:
 
    m = hashlib.md5()
    with open(filename, "rb" ) as f:
      while True:
        buf = f.read(blocksize)
        if not buf:
          break
        m.update(buf)
    checksum = m.hexdigest() 

  except Exception as ex:
    raise

  return(checksum)


#==============================================================================
#    G E T  V I D E O  A D  I N F O
#==============================================================================
def getVideoAdInfo(dbh, begin_dt='', end_dt=''):
  """
  getVideoAdInfo()
  Gets HarvestInstance records that match the search criteria provided.
  
  dbh: A database handle.
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """
  
  print("getVideoAdInfo(" + begin_dt + "," + end_dt + ")")
  
  instances = {}
  query = {}

  try:
    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    if (begin_dt and end_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['harvest_dt'] = {'$gte': begin_obj, '$lte': end_obj}
    elif (begin_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      query['harvest_dt'] = {'$gte': begin_obj}
    elif (end_dt):
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['harvest_dt'] = {'$lte': end_obj}

    #===== We want videos
    query['creative_type'] = 'video'

    #===== Do the query
    cursor = db.HarvestInstance.find(query)

    #===== Store the results
    for entry in cursor:
      instance_id = entry['id']
      if (instance_id):
        instances[instance_id] = entry

  except:
    raise

  return(instances)


#==============================================================================
#    M A I N
#==============================================================================
def main(args):
  """
  """
  loop = True
  processed = 0
  inserted = 0
  failed = 0
  skipped = 0
  sleeps = 0
  start = 0
  end = 0

  #===== Get DB handle
  dbh = getDBHandle()

  #===== Create SQS client
  sqs = getSQSClient()

  #===== Create s3 client
  s3_obj = getS3Client()

  #===== Start our timer
  start = datetime.datetime.now()

  #===== Begin processing loop
  while(loop):

    #===== Attempt to get a message
    message, receipt_handle = getMessageFromQueue(sqs, QUEUE_URL)

    #===== Process this message
    if (message and receipt_handle):

      processed += 1

      try:

        #===== Reset our sleeps counter
        sleeps = 0

        #===== Just delete it.  Worry about it later.
        deleteMessage(sqs, QUEUE_URL, receipt_handle)

        #===== Get info we need
        harvest_id = message['MessageAttributes']['harvest_id']['StringValue']
        print("Getting HarvestInstance info for " + harvest_id)
        entries = getHarvestInstanceInfo(dbh, harvest_id)
        if (not entries or (not harvest_id in entries)):
          print("Error: No HarvestInstance found for " + harvest_id)
          continue

        harvest_entry = entries[harvest_id]
        video_url = harvest_entry['element_url']
        video_width = harvest_entry['creative_width']
        video_height = harvest_entry['creative_height']
        harvest_dt = harvest_entry['harvest_dt']
        element_id = md5_hash = getMD5HashFromString(video_url)
        bare_url = video_url.split('?')[0].split('#')[0] # remove query string
        extension = getFileExtension(bare_url)

        #===== Create the temp filename
        temp_filename = '/tmp/' + element_id + extension

        #===== Skip if we already have this Element in the DB
        if (elementExists(dbh, element_id) and not args.force):
          print("Info: ElementID " + element_id + " already exists in DB.  Skipping...")
          skipped += 1
          continue

        try:
          rsp = urllib.request.urlretrieve(video_url, temp_filename) 
        except Exception as url_ex:
          print("Error: Couldn't get file for " + video_url + " " + str(url_ex))
          failed += 1
          pass
          continue

        #===== Try to get an extension using magic
        if (not extension):
          mime_type = magic.from_file(temp_filename, mime=True)
          (file_type, extension) = mime_type.split('/')
          if (extension):
            extension = '.' + extension

        #===== Create the s3 filename
        file_key = element_id[0:4];
        s3_filename = file_key + "/" + element_id + extension

        #===== Write temp file to S3
        s3 = boto3.client('s3')
        s3.upload_file(temp_filename, 'thousandx-image-data', s3_filename)
        print("Wrote " + temp_filename + ' to ' + s3_filename)
        checksum = getVideoFileChecksum(temp_filename)
   
        #===== Write the Element to the DB
        insert = {}
        insert["element_id"] = element_id
        insert["url"] = video_url
        insert["checksum"] = checksum
        insert["width"] = video_width
        insert["height"] = video_height
        insert["status"] =  "live"
        insert["added_dt"] = harvest_dt
        insert["type"] = "video"
        insert["local_url"] = s3_filename
        insert["format"] = extension.replace('.', '')
        insert["mime_type"] = "video/" + extension.replace('.', '')
        updateElementInfo(dbh, insert)
        print("Wrote ElementID: " + element_id + " to DB.")
        inserted += 1

        #===== Set tentative end time
        end = datetime.datetime.now()

        #===== Bail if we set a number to process and we've hit it
        if (args.num_to_process and (processed >= int(args.num_to_process))):
          print(str(processed) + " processed.  Exiting...")
          end = datetime.datetime.now()
          loop=False

      except Exception as ex:
        raise

    else:
      print("No messages in queue.  Sleeping.")
      time.sleep(15)
      sleeps += 1

      #===== Bail if we've slept too long
      if (args.max_sleeps and sleeps >= int(args.max_sleeps)):
        print("Hit " + str(sleeps) + " consecutive sleeps.  Exiting.")
        end = datetime.datetime.now()
        elapsed = end - start
        elapsed_secs = int(elapsed.total_seconds())
        instances_per_min = processed/(elapsed_secs/60.0)

        report =  "Processed " + str(processed) + " video ads.\n" + \
                  "Processing time: " + str(elapsed_secs) + "s.\n" + \
                  "Processing rate:  " + str(instances_per_min) + \
                  " instances per minute."
        print(report)
        writeToLog(LOGFILE, report)
        loop = False

  return


if __name__ == '__main__':
  args = parser.parse_args()
  print(vars(args))
  main(args)
  exit

