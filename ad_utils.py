import datetime
import hashlib
import random, string
import os, re

from urllib.parse import urlparse
from urllib.parse import unquote



#==============================================================================
#    G E T  F I L E  E X T E N S I O N
#==============================================================================
def getFileExtension(filename):
  """
  getFileExtension()
  """

  extension = ""

  try:
    filename, extension = os.path.splitext(filename)
  except Exception as ex:
    extension = "" 
    pass

  return(extension)


#==============================================================================
#    G E T  M D 5  H A S H  F R O M  S T R I N G
#==============================================================================
def getMD5HashFromString(mystr):
  """
  """

  md5_hash = '';
  try:
    result = hashlib.md5(mystr.encode()) 
    md5_hash = result.hexdigest()
  except:
    raise

  return(md5_hash)


#==============================================================================
#    W R I T E  T O  L O G
#==============================================================================
def writeToLog(logfile, msg):
  """
  writeToLog()
  """

  try:

    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if (msg and logfile):
      with open(logfile, 'a') as logfile:
        logfile.write(ts + " " + msg + "\n")
  except Exception as e:
    print("Error writing to " + logfile + ": " + str(e))
    pass

  return


#==============================================================================
#    R A N D O M  S T R I N G
#==============================================================================
def randomString(str_len=8):
  """
  """

  letters = string.ascii_lowercase

  return ''.join(random.choice(letters) for i in range(str_len))


#==============================================================================
#    G E T  P A G E  F R O M  U R L
#==============================================================================
def getPageFromURL(url):
  """
  getPageFromURL()
  """

  page = ''
  params = ''

  try:
    if (url.find('?') != -1): 
      page, params = url.split('?')
    else:
      page = url
      params = ''

  except Exception as e:
    print("Error: " + str(e))
    page = ''
   
  #print("URL: " + url)
  #print("Page: " + page)

  return(page)


#==============================================================================
#    G E T  H O S T  F R O M  U R L
#==============================================================================
def getHostFromURL(url):
  """
  """

  host = ''

  try:
    parsed_uri = urlparse(url)  
    host = parsed_uri.scheme + '://' + parsed_uri.hostname
    #print("URL: " + url)
    #print("Host: " + host)

  except Exception as e:
    print("Exception: " + str(e)) 
    host = ''

  return(host)


#==============================================================================
#    F I X  G O O G L E  U R L
#==============================================================================
def fixGoogleURL(orig_url):
  """
  fixGoogleURL()
  """

  fixed_url = ''
  temp_url = ''

  try:

    #===== Try to get URL from Google Ad Services and Doubleclick URLs
    r = re.compile(r'&adurl=(.+)$')
    decode_url = unquote(orig_url)
    decode_url = unquote(decode_url)
    #print(decode_url)
    hits = r.findall(decode_url)
    for h in hits:
      temp_url = h 

    if (temp_url):
      fixed_url = temp_url
    else:
      fixed_url = orig_url

  except Exception as ex:
    print("Error: Unable to fix google url " + orig_url)
    fixed_url = orig_url
    pass

  return(fixed_url)


#============================================================================== 
#    S T R I P  U R L
#============================================================================== 
def stripURL(url):
  """
  stripURL()
  """

  strip_url = url

  try:

    strip_url = strip_url.replace("http://","")
    strip_url = strip_url.replace("https://","")
    strip_url = strip_url.replace("www.","")
    strip_url = strip_url.replace(":443","")

  except Exception as ex:
    raise

  return(strip_url)
