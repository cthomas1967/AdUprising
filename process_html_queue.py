#==============================================================================
"""
process_html_queue.py

Written by C. Thomas
Development began 2019-12-16

"""
#==============================================================================
import boto3
import time
import argparse
import sys, os, re
import requests

sys.path.append(os.getcwd())
from proc_mongo import *
from sqs_utils import *
from s3_utils import *
from ad_utils import *


#==============================================================================
#    C O N F I G
#==============================================================================
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/757768816950/HTML_RETRIEVAL.fifo'
S3_BUCKET = 'thousandx-html-data'
REQUEST_TIMEOUT = 10
MAX_RETRIES = 1 
MIN_SIZE = 256
DEBUG = True


#==============================================================================
#    G L O B A L S
#==============================================================================
script_dir = os.getcwd()
LOGFILE = script_dir + '/logs/process_html_queue.log'


#==============================================================================
#    P A R S E R
#==============================================================================
parser = argparse.ArgumentParser(description='Do HTML harvesting for pages in the HTML processing queue.')
parser.add_argument('--page_id', help='A PageID to manually process.')
parser.add_argument('--num_to_process', help='Number of messages to process.')
parser.add_argument('--max_sleeps', help='Bail after sleeping longer than this.')
parser.add_argument('--force', help='Update the page even if the entry already exists.')


#==============================================================================
#    G E T  U R L  C O N T E N T
#==============================================================================
def getURLContent(url):
  """
  """
  raw_html = ''
  status_code = ''

  headers = {'User-Agent': \
             'Mozilla/5.0 (Macintosh; \
             Intel Mac OS X 10_10_1) \
             AppleWebKit/537.36 \
             (KHTML, like Gecko) \
             Chrome/39.0.2171.95 Safari/537.36'}

  try:

    #===== Standard get
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    if (response):
      status_code = response.status_code
      encoding = response.encoding
      raw_html = response.content

  except requests.exceptions.ConnectionError as ce:
    raw_html = ''
    writeToLog(LOGFILE, "ERROR: " + str(ce))
    pass
  except Exception as e:
    raw_html = ''
    writeToLog(LOGFILE, "ERROR: " + str(e))
    pass 

  return(raw_html, status_code)


#==============================================================================
#    W R I T E  R A W  H T M L  F I L E
#==============================================================================
def writeRawHtmlFile(s3_obj, file_key, outfile, raw_html):
  """
  writeRawHTMLFile()
  Arg1: An S3 Object.
  Arg2: A file key (first four chars of the PageID)
  Arg3: The full output filepath.
  Arg4: The raw HTML code to write to the file.
  """

  try:

    #===== Create the directory if it doesn't exist
    if (not objectExists(s3_obj, S3_BUCKET, file_key)):
      createDir(s3_obj, S3_BUCKET, file_key)

    #===== Write the file
    response = s3_obj.put_object(Body=raw_html, \
                                 Bucket=S3_BUCKET, \
                                 Key=outfile)
  except:
    raise

  return(response)


#==============================================================================
#    H A N D L E  H T T P  F A I L U R E
#==============================================================================
def handleHTTPFailure(sqs, dbh, page_id, page_url, http_code, retries):
  """
  handleHTTPFailure()
  """

  writeToLog(LOGFILE, "FAILURE: Unable to get HTML for " + page_url + " : " + str(http_code) + ".");

  #===== Requeue for retrial
  if (retries < MAX_RETRIES):
    tries = str(retries + 1)
    writeToLog(LOGFILE, "Requeuing with " + tries + " tries...")
    dedup_id = page_id
    message_id = sendMessageToHTMLQueue(sqs, page_id, page_url, tries, dedup_id)
  else:
    writeToLog(LOGFILE, "Marking as offline...")
    updatePageReachability(dbh, page_id, 'offline')
    

  return


#==============================================================================
#    M A I N 
#==============================================================================
def main(args):
  """
  main()

  """

  loop = True
  processed = 0
  start = 0
  end = 0
  sleeps = 0

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

    if (args.page_id):
      message = True
      receipt_handle = True
      loop = False
    else:
      #===== Attempt to get a message
      message, receipt_handle = getMessageFromQueue(sqs, QUEUE_URL)

    if (message):
      try:

        #===== Reset our sleep counter
        sleep = 0

        if (args.page_id):
          msg_page_id = args.page_id
          msg_page_url = getPageURL(dbh, msg_page_id)
          msg_tries = 0
        else:
          #===== Just delete it.  Worry about it later.
          deleteMessage(sqs, QUEUE_URL, receipt_handle)

          #===== Get info we need
          msg_page_id = message['MessageAttributes']['page_id']['StringValue']
          msg_page_url = message['MessageAttributes']['page_url']['StringValue']
          msg_tries = int(message['MessageAttributes']['tries']['StringValue'])

        writeToLog(LOGFILE, "Got " + msg_page_id + " for processing...")

        #===== Don't re-acquire the HTML if it's already in S3
        key = msg_page_id[0:4];
        outfile = key + "/" + msg_page_id + ".html";
        if (args.force):
          writeToLog(LOGFILE, "Forcing " + msg_page_id)
        elif (objectExists(s3_obj, S3_BUCKET, outfile) and
              getObjectSize(s3_obj, S3_BUCKET, outfile) >= MIN_SIZE):
          writeToLog(LOGFILE, "PageID: " + msg_page_id + " already in S3.  Skipping...")
          #updatePageReachability(dbh, msg_page_id, 'online')
          continue

        #===== Check reachability
        reachability = getPageReachability(dbh, msg_page_url)
        if (reachability and reachability == 'offline'):
          writeToLog(LOGFILE, "PageID: " + msg_page_id + " already found to be offline.  Skipping..")
          continue

        time.sleep(1) # play nice

        #===== Get the HTML
        (raw_html, http_status) = getURLContent(msg_page_url)

        #===== These codes mean something's gacked, so
        #      skip right to failing
        if (http_status == 204 or \
            http_status == 302 or \
            http_status == 400 or \
            http_status == 404 or \
            http_status == 406 or \
            http_status == 408 or \
            http_status == 500):   
          handleHTTPFailure(sqs, dbh, msg_page_id, msg_page_url, http_status, msg_tries) 
          continue

        #===== Write HTML to file
        elif (raw_html and (sys.getsizeof(raw_html) > MIN_SIZE)):
          response = writeRawHtmlFile(s3_obj, key, outfile, raw_html)
          writeToLog(LOGFILE, "SUCCESS: Wrote " + outfile + " to S3.")
          updatePageReachability(dbh, msg_page_id, 'online')

          #===== Inform the NLP system this page needs to be parsed
          writeToLog(LOGFILE, "Queuing " + msg_page_id + " for NLP processing...")
          tries = str(0)
          dedup_id = msg_page_id
          sendMessageToNLPQueue(sqs, msg_page_id, tries, dedup_id)

        else:
          writeToLog(LOGFILE, "Warning: Unable to get raw_html for " + msg_page_url + ' : ' + str(http_status))
          writeToLog(LOGFILE, "Warning: Unable to get raw_html for " + \
                     msg_page_url + ' : ' + str(http_status))
          updatePageReachability(dbh, msg_page_id, 'offline')

        processed += 1
        sleeps = 0 # reset sleeps counter
        end = datetime.datetime.now()

        #===== Bail if we set a number to process and we've hit it
        if (args.num_to_process and (processed >= int(args.num_to_process))):
          writeToLog(LOGFILE, str(processed) + " processed.  Exiting...")
          elapsed = end - start
          elapsed_secs = int(elapsed.total_seconds())
          instances_per_min = processed/(elapsed_secs/60.0)
          report =  "Processing time: " + str(elapsed_secs) + "s.\n" + \
                    "Processing rate:  " + str(instances_per_min) + \
                    " instances per minute."
          writeToLog(LOGFILE, report)
          writeToLog(LOGFILE, report)
          loop=False

      except:
        #===== Log this failure
        raise

    else:
      print("No messages in queue.  Sleeping.")
      time.sleep(30)
      sleeps += 1

      #==== Bail if we've slept too long
      if (args.max_sleeps and sleeps >= int(args.max_sleeps)):
        print("Hit " + str(sleeps) + " consecutive sleeps.  Exiting.")
        writeToLog(LOGFILE, "Hit " + str(sleeps) + " consecutive sleeps.  Exiting.")
        end = datetime.datetime.now()
        elapsed = end - start
        elapsed_secs = int(elapsed.total_seconds())
        instances_per_min = processed/(elapsed_secs/60.0)
        report =  "Processing time: " + str(round(elapsed_secs, 2)) + "s.\n" + \
                  "Processing rate:  " + str(round(instances_per_min, 2)) + \
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
