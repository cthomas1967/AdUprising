#==============================================================================
"""
process_nlp_queue.py

Written by C. Thomas
Development began 2019-12-22

"""
#==============================================================================
import boto3
import pprint
import argparse, time, datetime
import requests
import os, io, sys, re
import pytz
import warnings, operator
import RAKE

from urllib.parse import urlparse
from pymongo import MongoClient
from bs4 import BeautifulSoup
from bs4 import Tag
from collections import defaultdict
from langdetect import detect

sys.path.append(os.getcwd())
from proc_mongo import *
from sqs_utils import *
from s3_utils import *
from ad_utils import *


#==============================================================================
#    C O N F I G
#==============================================================================
NLP_QUEUE = 'https://sqs.us-east-1.amazonaws.com/757768816950/NLP_PROCESSING.fifo'
cwd = os.path.dirname(os.path.realpath(__file__))
LOGFILE = cwd + '/logs/process_nlp_queue.log'
COMMON_WORDS = cwd + '/common_words.txt'
ZIP_FILE = cwd + '/zip_codes.csv'
S3_BUCKET = 'thousandx-html-data'
MAX_KEYWORDS = 128
DEBUG = True


#==============================================================================
#    G L O B A L S
#==============================================================================
c_words = {}
zip_data = defaultdict(dict)


#==============================================================================
#    P A R S E R
#==============================================================================
parser = argparse.ArgumentParser(description='Apply NLP processing to pages in the NLP processing queue.')
parser.add_argument('--page_id', help='A PageID to manually process.')
parser.add_argument('--num_to_process', help='Number of messages to process.')
parser.add_argument('--max_sleeps', help='Bail out if we sleep more times than this.')
parser.add_argument('--cur_version', help='The most recent version of the parser.')
parser.add_argument('--force', help='Update the page even if the entry already exists.')


#==============================================================================
#    R E A D  Z I P  D A T A
#==============================================================================
def readZipData():

  """
  readZipData()
  """

  global zip_data

  with open(ZIP_FILE) as infile:
    for line in infile:
      fields = line.split(',') 
      area_code = fields[4].replace('"', '')
      zip_code = fields[0].replace('"', '')
      for ac in area_code.split('/'):
        try:
          ac = int(ac)
          zip_code = int(zip_code)
          zip_data[ac][zip_code] = 1
        except:
          pass

  return(zip_data)

#==============================================================================
#    R E A D  C O M M O N  K E Y W O R D S
#==============================================================================
def readCommonKeywords():
  fh = open(COMMON_WORDS, 'r')
  c_words = fh.read()
  return c_words 


#==============================================================================
#    G E T  P A G E  I D
#==============================================================================
def getPageID(fn):

  page_id = ''
  r = re.compile(r'([\da-f]+)\.html$')
  pids = r.findall(fn)
  for p in pids:
    page_id = p

  return(page_id)


#==============================================================================
#    C O N T A I N S  P H O N E  N U M B E R
#==============================================================================
def containsPhoneNumber(kw):

  phones = [] 
  r = re.compile(r'(\d{3}[-\.\s\)]\d{3}[-\.\s]\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]\d{4}|\(\d{3}\)\s?\d{3}[-\.\s]\d{4})')
  phones = r.findall(kw)
  dd_phones = []

  #===== Massage and remove duplicates
  for i in phones:
    i = i.replace('(', '')
    i = i.replace(') ', ')')
    i = i.replace(')', '-')
    i = i.replace('.', '-')
    i = re.sub(r"\s+", '-', i)
    i = re.sub(r"-+", '-', i)

    if i not in dd_phones:
      dd_phones.append(i)

  return(dd_phones)


#==============================================================================
#    C O N T A I N S  A D D R E S S E S
#==============================================================================
def containsAddresses(kw):

  addresses = [] 
  dd_addresses= []

  r = re.compile(r', ([A-Z]{2} [0-9]{5}|, [A-Z]{2} [0-9]{5}-[0-9]{4})')
  addresses = r.findall(kw)

  #===== Remove duplicates
  for i in addresses:
    if i not in dd_addresses:
      dd_addresses.append(i)

  return(dd_addresses)


#==============================================================================
#    P R O C E S S  K E Y W O R D
#==============================================================================
def processKeyword(kw):

  p_kw = kw

  #===== Process the keyword
  p_kw = p_kw.lower()
  p_kw = p_kw.rstrip().rstrip('.').rstrip(';').rstrip(')').rstrip('(')
  p_kw = p_kw.rstrip(':').rstrip('?').rstrip("'").rstrip('!')
  p_kw = p_kw.rstrip('>').rstrip('—')
  p_kw = p_kw.lstrip().lstrip("'")
  p_kw = p_kw.replace('"', '')
  p_kw = p_kw.replace('“', '')
  p_kw = p_kw.replace('”', '')
  p_kw = p_kw.replace('‘', "'")
  p_kw = p_kw.replace('\'s', "s")
  p_kw = p_kw.rstrip().lstrip()

  #===== This kills a lot of special chars.  Too strong.
  #p_kw = re.sub('[^a-zA-Z0-9_\s\-&\)\)]+', '', p_kw)

  return(p_kw)


#==============================================================================
#    I S  K E Y W O R D
#==============================================================================
def isKeyword(kw):

  global c_words

  is_keyword = True

  if (len(kw) <= 2): # no words shorter than three letters
    is_keyword = False
  elif (kw in c_words): # no common words
    is_keyword = False
  elif (re.match(r"^\d+$", kw) and len(kw) != 4): # no non-year numbers
    is_keyword = False
  elif (re.match(r"^[\s\d]+$", kw)): # only spaces and numbers
    is_keyword = False
  elif (re.match(r"^sha\-[\w\d]+$", kw)): # Hashes
    is_keyword = False
  elif (not re.match(r"[\w\d]", kw)): # must contain a letter or digit
    is_keyword = False

  return(is_keyword)


#==============================================================================
#    A D D  K E Y W O R D S
#==============================================================================
def addKeywords(text, key_weights, weight):

  #===== Split by commas, then by whitespace
  phrases = text.split(',')
  for phrase in phrases:
    words = phrase.split()
    for word in words:

      word = processKeyword(word)

      #===== Add it to the dictionary
      if (isKeyword(word) and (word in key_weights)):
        key_weights[word] += int(weight)
      elif(isKeyword(word)):
        key_weights[word] = int(weight)

  return


#==============================================================================
#    G E T  K E Y W O R D  W E I G H T S
#
#    Arg1: The dictionary to hold results.
#    Arg2: The text to be parsed.
#    Arg3: The max size of the key phrases to parse.
#    Arg4: The weight multiplier.
#==============================================================================
def getKeywordWeights(key_weights, text, max_phrase_size, weight):

  text = text.replace('‘', "'")
  Rake = RAKE.Rake(RAKE.SmartStopList())
  min_chars = 3
  min_frequency = 1 

  #===== Do the parsing
  keywords = Rake.run(text, min_chars, max_phrase_size, min_frequency)

  #===== Save the results
  for k in keywords:
    kwd = k[0]
    #wgt = k[1]
    kwd = processKeyword(kwd)

    # Give multi-word keywords extra weight

    #===== Process the keyword if it's a valid keyword
    if (kwd in key_weights and isKeyword(kwd)):
      if (len(kwd.split()) > 1):
        key_weights[kwd] += (weight + 2)
      else:
        key_weights[kwd] += weight
    elif (isKeyword(kwd)):
      if (len(kwd.split()) > 1):
        key_weights[kwd] = (weight + 10)
      else:
        key_weights[kwd] = weight

    #===== Parse apart phrases and add them separately
    if (len(kwd.split()) > 1):
      addKeywords(kwd, key_weights, weight)

  return


#==============================================================================
#    P R O C E S S  T I T L E
#==============================================================================
def processTitle(tree, key_weights, weight):
  """
  processTitle()
  """
  title_str = ''

  try:
    title = tree.find('title')
    if (title and title.string):
      addKeywords(title.string, key_weights, weight)
      title_str = title.string

  except Exception as ex:
    writeToLog(LOGFILE, "ERROR: " + str(ex))
    title_str = ''
    pass

  return(title_str)


#==============================================================================
#    P R O C E S S  A N C H O R S
#==============================================================================
def processAnchors(tree, key_weights, weight):

  anchors = []
  for anchor in tree.find_all('a', href=True):

    #===== Process them for keywords
    if (anchor.string):
      addKeywords(anchor.string, key_weights, weight)

    #===== Now store the anchors themselves
    href = anchor['href']
    if (href.startswith('http')):
      anchors.append(href)

  return(anchors)


#==============================================================================
#    P R O C E S S  L I S T S
#==============================================================================
def processLists(tree, key_weights, weight):
 
  for ul in tree.findAll('ul'):
    for li in ul.findAll('li'):
       if (li.string):
         addKeywords(li.string, key_weights, weight)

  return


#==============================================================================
#    G E T  R A W  T E X T
#==============================================================================
def getRawText(tree):
  body = tree.body
  if body is None:
      return None

  #===== Get rid of stuff we don't care about
  for tag in body.select('script'):
      tag.decompose()
  for tag in body.select('style'):
      tag.decompose()

  #===== Process what's left
  text = body.get_text(separator=' ')
  text = text.replace('\r', ' ').replace('\n', ' ')
  text = re.sub("\s\s+", " ", text)
  
  return text


#==============================================================================
#    P R O C E S S  M E T A  T A G S
#==============================================================================
def processMetaTags(tree, key_weights, weight):

  page_description = ''

  meta = tree.find_all('meta')

  for tag in meta:
    if 'name' in tag.attrs.keys():
      if tag.attrs['name'].strip().lower() == 'keywords' and 'content' in tag.attrs.keys():
        text = tag.attrs['content']
        if (text):
          meta_keywords = text.split(',')
          for word in meta_keywords:
              word = processKeyword(word)
    
              #===== Add it to the dictionary
              if (isKeyword(word) and (word in key_weights)):
                key_weights[word] += int(weight)
              elif(isKeyword(word)):
                key_weights[word] = int(weight)
      elif tag.attrs['name'].strip().lower() == 'description':
        if 'content' in tag.attrs.keys():
          page_description = tag.attrs['content']
          addKeywords(page_description, key_weights, weight)

      elif (tag.attrs['name'].strip().lower() == "twitter:title" or
            tag.attrs['name'].strip().lower() == "twitter:description"):

        #===== Process text for this tag
        if 'content' in tag.attrs.keys():
          text = tag.attrs['content']
          if (text):
            addKeywords(text, key_weights, weight)
      
  return(page_description)


#==============================================================================
#    G E T  P A G E  L A N G U A G E
#==============================================================================
def getPageLanguage(raw_text):

  lang = ''

  try:
    lang = detect(raw_text)

  except Exception as e:
    writeToLog(LOGFILE, "ERROR: getPageLanguage() " + str(e))
    lang = ''
    pass

  return(lang)


#==============================================================================
#    P R O C E S S  H E A D E R S
#==============================================================================
def processHeaders(tree, key_weights, weight):
  headers = []
  h1_fields = tree.find_all('h1')
  if (len(h1_fields)):
    for h in h1_fields:
      headers.append(h.string)
  h2_fields = tree.find_all('h2')
  if (len(h2_fields)):
    for h in h2_fields:
      headers.append(h.string)
  h3_fields = tree.find_all('h3')
  if (len(h3_fields)):
    for h in h3_fields:
      headers.append(h.string)
  h4_fields = tree.find_all('h4')
  if (len(h4_fields)):
    for h in h4_fields:
      headers.append(h.string)

  #===== Process each header
  for h in headers:
    if (h):
      addKeywords(h, key_weights, weight)

  return


#==============================================================================
#    R E A D  F I L E  F R O M  S 3
#==============================================================================
def readFileFromS3(s3_obj, filename):
  """
  """

  html = ''
  try:
    data = s3_obj.get_object(Bucket=S3_BUCKET, Key=filename)
    html = data['Body'].read()
  except:
    pass

  return(html)


#==============================================================================
#    P A R S E  H T M L
#==============================================================================
def parseHTML(dbh, s3_obj, filepath, dto):
  """
  parseHTML()
  """

  global zip_data

  anchors = []
  phones = []
  addresses = []
  keywords = []
  weights = []
  key_weights = {}
  tree = {}
  keyword_dict= {}
  geo_dict= {}
  anchor_dict= {}
  lang = ''
  num_zips = 0

  if (DEBUG):
    writeToLog(LOGFILE, "Reading file " + filepath + " from S3...")

  #===== Get PageID from filepath
  page_id = getPageID(filepath)

  #===== Open the file
  html = readFileFromS3(s3_obj, filepath)

  #===== Parse the raw HTML
  if (html):
    try:
      tree = BeautifulSoup(html, 'html.parser', from_encoding="iso-8859-1")
      #tree = BeautifulSoup(html, 'html.parser', from_encoding="utf-8")
    except:
      writeToLog(LOGFILE, "Couldn't parse HTML for " + page_id)
      tree = ''
      pass
  else:
    writeToLog(LOGFILE, "Warning: No html read for " + page_id)

  if (tree):

    #===== Title
    title_str = processTitle(tree, key_weights, 5)

    #===== Anchors
    anchors = processAnchors(tree, key_weights, 3)

    #===== Headers (e.g. h1, h2, h3, h4)
    processHeaders(tree, key_weights, 5)

    #===== Lists
    processLists(tree, key_weights, 2)

    #===== Process meta tags
    page_description = processMetaTags(tree, key_weights, 3)
    if (page_description):
      lang = getPageLanguage(page_description)
      if (lang):
        writeToLog(LOGFILE, "lang: " + lang)
        updatePageLanguage(dbh, page_id, lang)
      else:
        updatePageLanguage(dbh, page_id, '')


    #===== Get raw text
    raw_text = getRawText(tree)
    #writeToLog(LOGFILE, raw_text)
   
    #===== Parse raw text
    if (raw_text):
      phones = containsPhoneNumber(raw_text)
      addresses = containsAddresses(raw_text)
      getKeywordWeights(key_weights, raw_text, 2, 1)
   
      #===== If we didn't get the language above, try here
      if (lang == ''):
        lang = getPageLanguage(raw_text)
        if (lang):
          writeToLog(LOGFILE, "lang: " + lang)
          updatePageLanguage(dbh, page_id, lang)
        else:
          updatePageLanguage(dbh, page_id, '')

    #===== keyword_dict
    if (len(key_weights)):
      keyword_dict['page_id'] =  page_id
      keyword_dict['title'] = title_str 
      keyword_dict['page_description'] =  page_description
      keyword_dict['version'] =  '2.9'
      keyword_dict['last_harvest'] = dto
      keyword_dict['lang'] = lang
      sorted_d = sorted(key_weights.items(), key=operator.itemgetter(1), reverse=True)
      kw_added = 0
      for k in sorted_d:
        keyword = k[0]
        weight = k[1]
        keywords.append(keyword)
        weights.append(weight)
        #writeToLog(LOGFILE, keyword + " " +str(weight))
        kw_added += 1
        if (kw_added > MAX_KEYWORDS):
          break

      keyword_dict['keywords'] = keywords
      keyword_dict['weights'] = weights

    #===== anchor_dict
    if (anchors):
      anchor_dict['last_updated'] = dto
      anchor_dict['page_id'] = page_id 
      anchor_dict['type'] = 'anchors' 
      anchor_dict['anchors'] = anchors

    #===== geo_dict
    geo_zips = []

    #===== Add zip codes gleaned from area codes
    for phone in phones:
      area_code = int(phone[0:3])
      for zip in zip_data[area_code]:
        geo_zips.append(zip)
        num_zips += 1

    #===== Add zip codes gleaned from addresses
    for address in addresses:
      city, add_zip = address.split()
      geo_zips.append(int(add_zip))
      num_zips += 1
    
    if (len(geo_zips)): 
      geo_dict['last_updated'] = dto
      geo_dict['page_id'] = page_id
      geo_dict['type'] = 'geo_info' 
      geo_dict['zip_codes'] = geo_zips
      writeToLog(LOGFILE, "ZIPS: " + str(num_zips))


  return(keyword_dict, geo_dict, anchor_dict)


#==============================================================================
#    M A I N
#==============================================================================

def main(args):
 
  global c_words
  global zip_data

  loop = True
  start = 0
  end = 0
  sleeps = 0
  processed = 0

  #===== Get a DB handle
  dbh = getDBHandle()

  #===== Create pretty printer
  pp_obj = pprint.PrettyPrinter(indent=2)

  #===== Create a datetime object
  dto = datetime.datetime.now()
  timezone = pytz.timezone("America/Chicago")
  dt_local = timezone.localize(dto)

  #===== Read in common keywords
  writeToLog(LOGFILE, "Reading common words...")
  c_words = readCommonKeywords()

  #===== Read in the ZIP file
  writeToLog(LOGFILE, "Reading in zip data...")
  zip_data = readZipData()

  #===== Create SQS client
  writeToLog(LOGFILE, "Creating SQS client...")
  sqs = getSQSClient()

  queue_url = 'https://sqs.us-east-1.amazonaws.com/757768816950/NLP_PROCESSING.fifo'

  #===== Create s3 client
  writeToLog(LOGFILE, "Creating S3 client...")
  s3_obj = getS3Client()

  #===== Begin loop
  writeToLog(LOGFILE, "Beginning processing loop...")
  start = datetime.datetime.now()
  while(loop):
 
    #===== Attempt to get a message
    message, receipt_handle = getMessageFromQueue(sqs, NLP_QUEUE)
    #message = True
    #receipt_handle = True

    #===== Proceed if we have what we need
    if (message and receipt_handle):

      #===== Reset our sleeps counter
      sleeps = 0

      #===== Just delete it and worry about this later
      deleteMessage(sqs, NLP_QUEUE, receipt_handle)

      #===== Get the info we need
      if (args.page_id):
        page_id = args.page_id
        tries = 0
        args.num_to_process = 1
      else:
        page_id = message['MessageAttributes']['page_id']['StringValue']
        tries = int(message['MessageAttributes']['tries']['StringValue']) 

      #===== Read in and parse one file 
      try:

        #===== Get info we need
        prefix = page_id[0:4]
        filepath = prefix + '/' + page_id + '.html'
        page_url = getPageURL(dbh, page_id)
        parse_status = getPageParseStatus(dbh, page_id)
        version = getPageKeywordVersion(dbh, page_id)

        #===== If the page couldn't be parsed, skip it unless we're in force mode
        if (parse_status == 'failed' and not args.force):
          writeToLog(LOGFILE, "Info: PageID " + page_id + " parse status was 'failed'.  Skipping.")
          continue

        #===== If the page is already at the correct version, skip it
        if (args.cur_version and version == args.cur_version):
          writeToLog(LOGFILE, "Info: PageID " + page_id + " already at version " + str(version))
          continue

        #===== If the file doesn't exist, add a message to the HTML queue
        #      and then skip it
        if (not objectExists(s3_obj, S3_BUCKET, filepath) or
            getObjectSize(s3_obj, S3_BUCKET, filepath) == 0):

          #===== Check the reachability
          reachability = getPageReachability(dbh, page_id)
          if (reachability and reachability == 'offline'):
            writeToLog(LOGFILE, "Info: PageID " + page_id + " is offline.  Skipping.")
            updatePageParseStatus(dbh, page_id, 'failed')
            continue
          else:
            writeToLog(LOGFILE, "Info: PageID " + page_id + " not in S3.  Queuing...")
            tries=str(0)
            dedup_id = page_id
            sendMessageToHTMLQueue(sqs, page_id, page_url, tries, dedup_id) 
            continue

        #===== Parse the page
        keyword_dict, geo_dict, anchor_dict = parseHTML(dbh, s3_obj, filepath, dto)

        #===== Update the PageKeyword2 collection
        if (keyword_dict):
          writeToLog(LOGFILE, "Updating page keyword info...")
          error = updatePageKeywordInfo(dbh, keyword_dict) 
          updatePageReachability(dbh, page_id, 'online')
          updatePageParseStatus(dbh, page_id, 'success')
          updatePageLastParse(dbh, page_id)
        else:
          writeToLog(LOGFILE, "Warning: Got no keywords from " + page_url)
          writeToLog(LOGFILE, "Warning: Got no keywords from " + page_id)
          updatePageReachability(dbh, page_id, 'offline')
          updatePageParseStatus(dbh, page_id, 'failed')

        #===== Update PageInfo.geo collection
        if (geo_dict):
          writeToLog(LOGFILE, "Updating page geo info...")
          error = updatePageInfo(dbh, geo_dict)

        #===== Update PageInfo.anchors collection
        if (anchor_dict):
          writeToLog(LOGFILE, "Updating page anchor info...")
          error = updatePageInfo(dbh, anchor_dict)

        processed += 1
        end = datetime.datetime.now()

        #===== Bail if we set a number to process and we've hit it
        if (args.num_to_process and (processed >= int(args.num_to_process))):
          writeToLog(LOGFILE, str(processed) + " processed.  Exiting...")
          end = datetime.datetime.now()
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
      writeToLog(LOGFILE, "No messages in queue.  Sleeping...")
      time.sleep(15) # rest
      sleeps += 1
      if (args.max_sleeps and sleeps >= int(args.max_sleeps)):
        writeToLog(LOGFILE, "Hit " + str(sleeps) + " consecutive sleeps.  Exiting.")
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
