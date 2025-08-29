#==============================================================================
"""
process_element_queue.py

Written by C. Thomas
Development began 2019-12-16

"""
#==============================================================================

import boto3
import os, io, sys
import time, re
import argparse, requests
import hashlib

from PIL import Image

sys.path.append(os.getcwd())
from proc_mongo import *
from sqs_utils import *
from s3_utils import *
from ad_utils import *


#==============================================================================
#    C O N F I G
#==============================================================================
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/757768816950/ELEMENT_RETRIEVAL.fifo'
S3_BUCKET = 'thousandx-image-data'
cwd = os.path.dirname(os.path.realpath(__file__))
LOGFILE = cwd + '/logs/process_element_queue.log'
REQUEST_TIMEOUT = 10
MAX_RETRIES = 1 
DEBUG = True


#==============================================================================
#    P A R S E R
#==============================================================================
parser = argparse.ArgumentParser(description='Do HTML harvesting for pages in the HTML processing queue.')
parser.add_argument('--element_id', help='An ElementID to manually process.')
parser.add_argument('--num_to_process', help='Number of messages to process.')
parser.add_argument('--max_sleeps', help='Bail after this many consecutive sleeps.')
parser.add_argument('--force', help='Update the page even if the entry already exists.')


#==============================================================================
#    G L O B A L S
#==============================================================================


#=================================================================
#    G E T  I M A G E  F I L E  E X T E N S I O N
#=================================================================
def getImageFileExtension(img_mime, img_format):
  """
  """

  img_type = ''
  suffix = ''

  try:

    if (img_mime):
      img_type, suffix = img_mime.split('/')
    elif (img_format):
      suffix = img_format
      img_type = 'image'
      if (suffix == 'swf'):
        image_type = 'video'

    #===== Massage
    if (suffix == 'jpeg'):
      suffix = 'jpg'
    if (img_type != 'image' and img_type != 'video'):
      img_type = 'unsupported'

  except Exception as ex:
    writeToLog(LOGFILE, "Error: Unable to get image type and suffix. " + str(ex))
    img_type = ''
    suffix = ''
    pass

  return(img_type, suffix)


#==============================================================================
#    G E T  I M A G E  D I M E N S I O N S
#==============================================================================
def getImageDimensions(img):
  """
  getImageDimensions()
  """

  img_width = 0
  img_height = 0

  try:
    img_width, img_height = img.size

  except Exception as ex:
    writeToLog(LOGFILE, "Error: Unable to get image dimensions. " + str(ex))
    img_width = 0
    img_height = 0
    pass

  return(img_width, img_height)


#==============================================================================
#    G E T  I M A G E  S P E C S
#==============================================================================
def getImageSpecs(img):
  """
  getImageSpecs()
  """
  img_type = ''
  suffix = ''
  img_mime = ''
  img_format = ''

  try:
    if (img.format):
      img_mime = Image.MIME[img.format]
      img_format = img.format.lower()
      writeToLog(LOGFILE, "Mime: " + img_mime)
      writeToLog(LOGFILE, "Format: " + img_format)
      img_type, suffix = getImageFileExtension(img_mime, img_format)

  except Exception as ex:
    img_type=''
    suffix = ''
    img_mime = ''
    img_format = ''
    raise

  return(img_type, suffix, img_mime, img_format)


#==============================================================================
#    G E T  I M A G E  F R O M  U R L
#==============================================================================
def getImageFromURL(img_url):
  """
  getImageFromURL()
  """

  img = ''
  raw_data = ''

  try:

    #===== Get the raw data
    response = requests.get(img_url, stream=True, timeout=REQUEST_TIMEOUT)
    file_obj = response.raw
    raw_data = file_obj.read() # <class 'bytes'>
    
    #===== Get an actual image object
    img = Image.open(io.BytesIO(raw_data))
    
  except Exception as e:
    writeToLog(LOGFILE, "Error: Unable to get image file and raw data. " + str(e))
    img = ''
    raw_data = ''
    pass 

  return(img, raw_data)


#==============================================================================
#    G E T  I M A G E  F R O M  W E B P  U R L
#==============================================================================
def getImageFromWebpURL(img_url):
  """
  getImageFromWebpURL()
  """

  img = ''
  raw_data = ''

  try:

    #===== Get the raw data
    response = requests.get(img_url, stream=True)
    response.raw.decode_content = True
    img = Image.open(response.raw).convert('RGB')
    raw_data = img.tobytes()
    
  except Exception as e:
    writeToLog(LOGFILE, "Error: Unable to get image file and raw data from WebP URL. " + str(e))
    img = ''
    raw_data = ''
    pass 

  return(img, raw_data)


#==============================================================================
#    G E T  I M A G E  C H E C K S U M
#==============================================================================
def getImageChecksum(img):
  """
  getImageChecksum()
  """

  checksum = ''

  try:

    checksum = hashlib.md5(img.tobytes()).hexdigest()

  except Exception as e:
    writeToLog(LOGFILE, "Error: Unable to get image checksum! " + str(e)) 
    checksum = ''
    pass

  return(checksum)


#==============================================================================
#    S A V E  I M A G E  T O  S 3
#==============================================================================
def saveImageToS3(s3_obj, img_filename, img_data):
  """
  """

  saved=False

  try:
    #===== Don't do anything if we've already got this element
    file_key = img_filename[0:4];
    outfile = file_key + "/" + img_filename;
    if (objectExists(s3_obj, S3_BUCKET, outfile)):
      writeToLog(LOGFILE, "Element: " + outfile + " already in S3.  Skipping...")
    else:
      #===== Create the parent directory if it doesn't exist
      parent_dir = file_key
      if (not objectExists(s3_obj, S3_BUCKET, parent_dir)):
        createDir(s3_obj, S3_BUCKET, parent_dir)

      #===== Write the file
      image_file = s3_obj.put_object(Body=img_data, \
                                     Bucket=S3_BUCKET, \
                                     Key=outfile)
      saved=True

  except:
    raise

  return(outfile, saved)


#==============================================================================
#    M A I N 
#==============================================================================
def main(args):
  """
  main()

  """

  loop = True
  sleeps = 0
  processed = 0

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
    if (args.element_id == None):
      message, receipt_handle = getMessageFromQueue(sqs, QUEUE_URL)
    else:
      message = True
      receipt_handle = True

    #===== Process this message
    if (message and receipt_handle):
 
      is_webp = False
      processed += 1

      try:

        #===== Reset our sleeps counter
        sleeps = 0

        if (args.element_id):
          element_id = args.element_id
          element_url = getElementURL(dbh, element_id)
          added_dt = getElementAddedDt(dbh, element_id)
          if (not added_dt):
            added_dt = datetime.datetime.now()
          tries = int(0)
        else:
          #===== Just delete it.  Worry about it later.
          deleteMessage(sqs, QUEUE_URL, receipt_handle)

          #===== Get info we need
          element_id = message['MessageAttributes']['element_id']['StringValue']
          element_url = message['MessageAttributes']['url']['StringValue']
          tries = int(message['MessageAttributes']['tries']['StringValue'])
          dt_str = message['MessageAttributes']['harvest_dt']['StringValue']
          if (dt_str):
            added_dt = datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S') 
          else:
            added_dt = datetime.datetime.now()

        md5_hash = getMD5HashFromString(element_url)

        #===== Here we go
        writeToLog(LOGFILE, "Processing " + element_id + "...")

        #===== Get file extension
        extension = getFileExtension(element_url).lower()

        #===== Verify ElementID
        if (md5_hash != element_id):
          writeToLog(LOGFILE, "ElementID " + element_id, + " does not match MD5 hash.")

        #===== Get the element
        if (extension == 'webp'):
          is_webp = True
          element_img, raw_img = getImageFromWebpURL(element_url)
        else:
          element_img, raw_img = getImageFromURL(element_url)
         
        if (element_img and raw_img):

          time.sleep(0.5) # play nice

          #===== Get image specs
          img_width, img_height = getImageDimensions(element_img)
          img_type, suffix, img_mime, img_format = getImageSpecs(element_img)
          if (is_webp):
            img_type = 'image'
            suffix = 'webp'
            img_mime = 'image/webp'
            img_format = 'webp'
          if (img_type == '' or suffix == ''):
            continue

          checksum = getImageChecksum(element_img)

          #===== Save the image to S3
          img_filename = element_id + '.' + suffix
          outfile, saved = saveImageToS3(s3_obj, img_filename, raw_img)         
          if (saved):
            msg = "Saved " + outfile + " to S3."
            writeToLog(LOGFILE, msg)
 
          #===== Save the image data as an Element record
          element = {}
          element['element_id'] = element_id
          element['url'] = element_url
          element['checksum'] = checksum 
          element['width'] = img_width
          element['height'] = img_height
          element['status'] = 'live' 
          element['added_dt'] = added_dt
          element['type'] = img_type 
          element['local_url'] = outfile
          element['mime_type'] = img_mime
          element['format'] = img_format
          error = updateElementInfo(dbh, element)

        else:
          err_msg = "Error: Unable to get image data for " + element_id
          writeToLog(LOGFILE, err_msg)

        #===== Set tentative end time
        end = datetime.datetime.now()

        #===== Bail if we set a number to process and we've hit it
        if (args.num_to_process and (processed >= int(args.num_to_process))):
          print(str(processed) + " processed.  Exiting...")
          writeToLog(LOGFILE, str(processed) + " processed.  Exiting...")
          loop=False

      except Exception as e:
        writeToLog(LOGFILE, str(e))
        raise

    else:
      print("No messages in queue.  Sleeping.")
      time.sleep(15)
      sleeps += 1

      #===== Bail if we've slept too long
      if (args.max_sleeps and sleeps >= int(args.max_sleeps)):
        writeToLog(LOGFILE, "Hit " + str(sleeps) + " consecutive sleeps.  Exiting.")
        end = datetime.datetime.now()
        elapsed = end - start
        elapsed_secs = int(elapsed.total_seconds())
        elements_per_min = processed/(elapsed_secs/60.0)
        report = "Processing time: " + str(round(elapsed_secs, 2)) + "s." + \
                 "Processing rate:  " + str(round(elements_per_min, 2)) + \
                 " elements per minute."
        print(report)
        writeToLog(LOGFILE, report)
        loop = False

  return


if __name__ == '__main__':
  args = parser.parse_args()
  print(vars(args))
  main(args)
  exit
