import datetime
import os
from pymongo import MongoClient

#DEBUG = True
DEBUG = False

cwd = os.path.dirname(os.path.realpath(__file__))
LOGFILE = cwd + '/logs/proc_mongo.log'

#==============================================================================
#    G E T  D B  H A N D L E
#==============================================================================
def getDBHandle():
  db_host = '52.91.254.92'
  db_user = 'au_portal'
  db_pass = '5y2tweMnPk6TcUy'
  db_name = 'au_mongo'
  db_client = MongoClient('mongodb://%s:%s@%s/%s' % (db_user, db_pass, db_host, db_name))

  return(db_client)


#==============================================================================
#    G E T  D A T E T I M E  O B J
#==============================================================================
def getDatetimeObj(date_str):
  """
  """

  date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
  return(date_obj)


#==============================================================================
#    G E T  C O L L E C T I O N  S I Z E
#==============================================================================
def getCollectionSize(dbh, collection_name, field, begin_dt='', end_dt=''):
  """
  getCollectionSize()
  """
  query = {}
  size = 0

  writeToLog("getCollectionSize(" + collection_name + ',' + field + ',' + \
             begin_dt + ',' + end_dt)
  try:

    if (not field):
      field = 'added_dt'

    #===== Set up the query
    if (begin_dt and end_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query[field] = {'$gte': begin_obj, '$lte': end_obj}
    elif (begin_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      query[field] = {'$gte': begin_obj}
    elif (end_dt):
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query[field] = {'$lte': end_obj}

    db = dbh['au_mongo']

    size = db[collection_name].count(query)

  except:
    raise

  return(size)


#==============================================================================
#    E L E M E N T  E X I S T S
#==============================================================================
def elementExists(dbh, element_id):
  """
  elementExists()
  """

  writeToLog("elementExists(" + element_id + ")")

  exists = False
  query = {}

  try:
    #===== Get our db
    db = dbh['au_mongo']

    query['element_id'] = element_id

    entry = db.Element.find_one(query)
    if (entry and 'element_id' in entry):
      exists = True

  except:
    raise

  return(exists)


#==============================================================================
#    F U L L  U R L  E X I S T S
#==============================================================================
def fullURLExists(dbh, full_url_id):
  """
  fullURLExists()
  """

  writeToLog("fullURLExists(" + full_url_id + ")")

  exists = False
  query = {}

  try:
    #===== Get our db
    db = dbh['au_mongo']

    query['id'] = full_url_id

    entry = db.FullURL.find_one(query)
    if (entry and 'id' in entry):
      exists = True

  except:
    raise

  return(exists)


#==============================================================================
#    P A G E  E X I S T S
#==============================================================================
def pageExists(dbh, page_id):
  """
  """

  writeToLog("pageExists(" + page_id + ")")

  exists = False

  try:
    #===== Get our db
    db = dbh['au_mongo']

    query = {}
    query['id'] = page_id

    entry = db.Page.find_one(query)
    if (entry and 'id' in entry):
      exists = True

  except:
    raise

  return(exists)


#==============================================================================
#    H O S T  E X I S T S
#==============================================================================
def hostExists(dbh, host_id):
  """
  hostExists()
  """

  writeToLog("hostExists(" + host_id + ")")

  exists = False

  try:
    #===== Get our db
    db = dbh['au_mongo']

    query = {}
    query['id'] = host_id

    entry = db.Host.find_one(query)
    if (entry and 'id' in entry):
      exists = True

  except:
    raise

  return(exists)

#==============================================================================
#    I N S T A N C E  F U L L  U R L  E X I S T S
#==============================================================================
def instanceFullURLExists(dbh, full_url_id):
  """
  instanceFullURLExists()
  Gets records that match the search criteria provided.

  dbh: A database handle.
  full_url_id: A FullURLID.
  """
  
  writeToLog("instanceFullURLExists(" + full_url_id + ")")

  inst_info = {}
  query = {}
  inst_id = ''

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    #      We are using $or, but $in might be faster.
    query['$or'] = [{'linked_id':full_url_id}, {'publisher_id':full_url_id}]

    cursor = db.Instance.find(query)

    for entry in cursor:
      if ('id' in entry):
        inst_id = entry['id']

  except:
    inst_id = ''
    raise

  return(inst_id)


#==============================================================================
#    D E L E T E   P A G E 
#==============================================================================
def deletePage(dbh, page_id):
  """
  deletePage()
  """

  writeToLog("deletePage(" + page_id + ")")

  query = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']
    query['id'] = page_id
    result = db.Page.delete_one(query)

  except Exception as e:
    print("Error: unable to remove Page " + page_id + " " + str(e))
    pass  

  return(result)


#==============================================================================
#    D E L E T E   P A G E   K E W O R D S
#==============================================================================
def deletePageKeywords(dbh, page_id):
  """
  deletePageKeywords()
  """

  writeTolog("deletePageKeywords(" + page_id + ")")

  query = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']
    query['page_id'] = page_id
    result = db.PageKeyword2.delete_one(query)

  except Exception as e:
    print("Error: unable to remove keywords for PageID " + page_id + " " + str(e))
    pass  

  return(result)


#==============================================================================
#    D E L E T E  I N S T A N C E
#==============================================================================
def deleteInstance(dbh, inst_id):
  """
  deleteInstance()
  """

  writeToLog("deleteInstance(" + inst_id + ")")

  query = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']
    query['id'] = inst_id
    result = db.Instance.delete_one(query)

  except Exception as e:
    print("Error: unable to remove InstanceID " + inst_id + " " + str(e))
    pass  

  return(result)


#==============================================================================
#    G E T  P A G E  I N F O
#==============================================================================
def getPageInfo(dbh, begin_dt=None, end_dt=None):
  """
  getPageInfo()
  Gets page records that match the search criteria provided.

  dbh: A database handle.
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """

  writeToLog("getPageInfo(" + begin_dt + "," + end_dt + ")")

  page_info = {}
  query = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    if (begin_dt and end_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['added_dt'] = {'$gte': begin_obj, '$lte': end_obj}
    elif (begin_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      query['added_dt'] = {'$gte': begin_obj}
    elif (end_dt):
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['added_dt'] = {'$lte': end_obj}

    cursor = db.Page.find(query)
    for entry in cursor:
      page_id = entry['id']
      if (page_id):
        page_info[page_id] = entry

  except:
    raise

  return(page_info)


#==============================================================================
#    G E T  I N S T A N C E  I N F O
#==============================================================================
def getInstanceInfo(dbh, begin_dt=None, end_dt=None):
  """
  getInstanceInfo()
  Gets records that match the search criteria provided.

  dbh: A database handle.
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """

  writeTolog("getInstanceInfo(" + begin_dt + ',' + end_dt + ")")

  inst_info = {}
  query = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    if (begin_dt and end_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['dt'] = {'$gte': begin_obj, '$lte': end_obj}
    elif (begin_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      query['dt'] = {'$gte': begin_obj}
    elif (end_dt):
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['dt'] = {'$lte': end_obj}

    cursor = db.Instance.find(query)
    for entry in cursor:
      inst_id = entry['id']
      if (inst_id):
        inst_info[inst_id] = entry

  except:
    raise

  return(inst_info)


#==============================================================================
#    P A G E  I N S T A N C E  E X I S T S
#==============================================================================
def pageInstanceExists(dbh, page_id):
  """
  pageInstanceExists()
  Gets records that match the search criteria provided.

  dbh: A database handle.
  page_id: A PageID.
  """

  writeToLog("pageInstanceExists(" + page_id + ")")

  inst_info = {}
  query = {}
  inst_id = ''

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    #      We are using $or, but $in might be faster.
    query['$or'] = [{'link_page_id':page_id}, {'publ_page_id':page_id}]

    cursor = db.Instance.find(query)

    for entry in cursor:
      if ('id' in entry):
        inst_id = entry['id']

  except:
    inst_id = ''
    raise

  return(inst_id)


#==============================================================================
#    G E T  F U L L  U R L  I N F O
#==============================================================================
def getFullURLInfo(dbh, begin_dt=None, end_dt=None):
  """
  getFullURLInfo()
  Gets records that match the search criteria provided.

  dbh: A database handle.
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """

  writeToLog("getFullURLInfo(" + begin_dt + ',' + end_dt + ")")

  full_url_info = {}
  query = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    if (begin_dt and end_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['added_dt'] = {'$gte': begin_obj, '$lte': end_obj}
    elif (begin_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      query['added_dt'] = {'$gte': begin_obj}
    elif (end_dt):
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['added_dt'] = {'$lte': end_obj}

    cursor = db.FullURL.find(query)
    for entry in cursor:
      full_url_id = entry['id']
      if (full_url_id):
        full_url_info[full_url_id] = entry

  except:
    raise

  return(full_url_info)


#==============================================================================
#    G E T  P A G E  I D S
#==============================================================================
def getPageIDs(dbh, begin_dt=None, end_dt=None, version=None):
  """
  getPageIDs()
  Gets PageIDs that match the search criteria provided.

  dbh: A database handle.
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  version: A version string (optional)
  """

  writeToLog("getPageIDs(" + begin_dt + ',' + end_dt + ',' + version + ")")

  page_ids = {}
  query = {}

  db = dbh['au_mongo']

  #===== Set up the query
  if (begin_dt and end_dt):
    begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
    end_obj = getDatetimeObj(end_dt + " 23:59:59")
    query['added_dt'] = {'$gte': begin_obj, '$lte': end_obj}
  elif (begin_dt):
    begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
    query['added_dt'] = {'$gte': begin_obj}
  elif (end_dt):
    end_obj = getDatetimeObj(end_dt + " 23:59:59")
    query['added_dt'] = {'$lte': end_obj}
  if (version):
    query['version'] = version

  cursor = db.Page.find(query)
  for entry in cursor:
    page_id = entry['id']
    if (page_id):
      page_ids[page_id] = 1

  return(page_ids)


#==============================================================================
#    G E T  P A G E  U R L
#==============================================================================
def getPageURL(dbh, page_id):
  """
  getPageURL()
  Gets the URL for the PageID that matches the search criteria provided.

  dbh: A database handle.
  page_id: A PageID.
  returns: The page URL.
  """

  writeToLog("getPageURL(" + page_id + ")")

  query = {}
  page_url = ''

  db = dbh['au_mongo']

  #===== Set up the query
  query['id'] = page_id 

  #===== Perform the search
  entry = db.Page.find_one(query)
  if (entry):
    page_url = entry['url']

  return(page_url)


#==============================================================================
#    G E T  P A G E  R E A C H A B I L I T Y
#==============================================================================
def getPageReachability(dbh, page_id):
  """
  getPageReachabilty()

  dbh: A database handle.
  page_id: A PageID.
  returns: The page reachability field.
  """

  writeToLog("getPageReachability(" + page_id + ")")

  query = {}
  reachability = ''

  db = dbh['au_mongo']

  #===== Set up the query
  query['id'] = page_id 

  #===== Perform the search
  entry = db.Page.find_one(query)
  if (entry and 'reachability' in entry):
    reachability = entry['reachability']

  return(reachability)


#==============================================================================
#    G E T  P A G E  P A R S E  S T A T U S
#==============================================================================
def getPageParseStatus(dbh, page_id):
  """
  getPageParseStatus()

  dbh: A database handle.
  page_id: A PageID.
  returns: The page parse_status field.
  """

  writeToLog("getParseStatus(" + page_id + ")")

  query = {}
  parse_status = ''

  db = dbh['au_mongo']

  #===== Set up the query
  query['id'] = page_id 

  #===== Perform the search
  entry = db.Page.find_one(query)
  if (entry and 'parse_status' in entry):
    parse_status = entry['parse_status']

  return(parse_status)


#==============================================================================
#    U P D A T E  P A G E  I N F O
#==============================================================================
def updatePageInfo(dbh, data_dict):
  """ 
  Updates a PageInfo record with the contents of a dictionary.

  dbh: A database handle.
  page_id: A PageID.
  data_type: The type of PageInfo record (e.g. 'anchors').
  data_dict: A python dictionary holding the data to be inserted
  returns: Nothing if successful, otherwise an error message.
  """

  writeToLog("updatePageInfo()")

  page_id = data_dict['page_id']
  data_type = data_dict['type']
  error = ''

  #===== Set up collection
  db = dbh['au_mongo']
  col = db['PageInfo']

  #===== Insert/Update the record
  try:
    result = col.replace_one({'page_id': page_id, 'type': data_type}, \
                             data_dict, \
                             upsert=True)
  except:
    raise

  return(error)


#==============================================================================
#    U P D A T E  P A G E  R E A C H A B I L I T Y
#==============================================================================
def updatePageReachability(dbh, page_id, status):
  """ 
  Updates a Page.reachability status.

  dbh: A database handle.
  page_id: A PageID.
  status: "offline" or "online"
  returns: Nothing if successful, otherwise an error message.
  """

  writeToLog("updatePageReachability(" + page_id + ", " + status + ")")

  error = ''

  #===== Set up db object
  db = dbh['au_mongo']

  #===== Insert/Update the record
  try:
    result = db.Page.update({'id': page_id}, \
                            {'$set': {'reachability': status}})
  except:
    raise

  return(error)


#==============================================================================
#    U P D A T E  P A G E  P A R S E  S T A T U S
#==============================================================================
def updatePageParseStatus(dbh, page_id, status):
  """ 
  Updates a Page.parse_status status.

  dbh: A database handle.
  page_id: A PageID.
  status: "success" or "failed"
  returns: Nothing if successful, otherwise an error message.
  """

  writeToLog("updatePageParseStatus(" + page_id + ", " + status + ")")

  error = ''

  #===== Set up db object
  db = dbh['au_mongo']

  #===== Insert/Update the record
  try:
    result = db.Page.update({'id': page_id}, \
                            {'$set': {'parse_status': status}})
  except:
    raise

  return(error)


#==============================================================================
#    U P D A T E  P A G E  L A S T  P A R S E
#==============================================================================
def updatePageLastParse(dbh, page_id, date_str=''):
  """ 
  updatePageLastParse()

  Updates a Page.last_harvest date.

  dbh: A database handle.
  page_id: A PageID.
  date_str: A date string.
  returns: Nothing if successful, otherwise an error message.
  """

  error = ''

  writeToLog("updatePageLastParse(" + page_id + ", " + date_str + ")")

  #===== Get a date object
  if (date_str):
    date_obj = getDatetimeObj(date_str)
  else:
    date_obj = datetime.datetime.now()

  #===== Set up db object
  db = dbh['au_mongo']

  #===== Insert/Update the record
  try:
    result = db.Page.update({'id': page_id}, \
                            {'$set': {'last_parse_dt': date_obj}})
  except:
    raise                        

  return(error)


#==============================================================================
#    U P D A T E  P A G E  L A N G U A G E
#==============================================================================
def updatePageLanguage(dbh, page_id, lang=''):
  """ 
  Updates a Page.last_harvest date.

  dbh: A database handle.
  page_id: A PageID.
  lang: A language value (e.g. 'en-US')
  returns: Nothing if successful, otherwise an error message.
  """

  error = ''

  writeToLog("updatePageLanguage(" + page_id + ", " + lang + ")")

  #===== Set up db object
  db = dbh['au_mongo']

  #===== Insert/Update the record
  try:
    result = db.Page.update({'id': page_id}, \
                            {'$set': {'lang': lang}})
  except:
    raise                        

  return(error)


#==============================================================================
#    U P D A T E  P A G E  K E Y W O R D   I N F O
#==============================================================================
def updatePageKeywordInfo(dbh, data_dict):
  """ 
  Updates a PageKeyword record with the contents of a dictionary.

  dbh: A database handle.
  page_id: A PageID
  data_dict: A python dictionary holding the data to be inserted
  returns: Nothing if successful, otherwise an error message.
  """

  writeToLog("updatePageKeywordInfo()")

  page_id = data_dict['page_id']

  error = ''

  #===== Set up collection
  db = dbh['au_mongo']
  col = db['PageKeyword2']

  #===== Insert/Update the record
  try:
    result = col.replace_one({'page_id': page_id}, \
                             data_dict, \
                             upsert=True)
  except:
    raise

  return(error)

#==============================================================================
#    U P D A T E  S Y S T E M  I N F O
#==============================================================================
def updateSystemInfo(dbh, data_dict):
  """ 
  Updates a SystemInfo record with the contents of a dictionary.

  dbh: A database handle.
  data_dict: A python dictionary holding the data to be inserted
  returns: Nothing if successful, otherwise an error message.
  """

  data_date = data_dict['data_date']
  data_type = data_dict['type']

  error = ''

  try:

    #===== Set up collection
    db = dbh['au_mongo']
    col = db['SystemInfo']

    #===== Insert/Update the record
    result = col.replace_one({'data_date': data_date, 'type': data_type}, \
                             data_dict, \
                             upsert=True)
  except:
    raise

  return(error)


#==============================================================================
#    G E T  P A G E  K E Y W O R D  I D S
#==============================================================================
def getPageKeywordIDs(dbh, begin_dt=None, end_dt=None):
  """
  getPageKeywordIDs()
  Gets PageIDs that match the search criteria provided.

  dbh: A database handle.
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """

  page_ids = {}
  query = {}

  db = dbh['au_mongo']

  #===== Set up the query
  if (begin_dt and end_dt):
    begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
    end_obj = getDatetimeObj(end_dt + " 23:59:59")
    query['last_harvest'] = {'$gte': begin_obj, '$lte': end_obj}
  elif (begin_dt):
    begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
    query['last_harvest'] = {'$gte': begin_obj}
  elif (end_dt):
    end_obj = getDatetimeObj(end_dt + " 23:59:59")
    query['last_harvest'] = {'$lte': end_obj}

  cursor = db.PageKeyword2.find(query)
  for entry in cursor:
    page_id = entry['page_id']
    if (page_id):
      page_ids[page_id] = 1

  return(page_ids)


#==============================================================================
#    G E T  P A G E  K E Y W O R D  I N F O
#==============================================================================
def getPageKeywordInfo(dbh, begin_dt=None, end_dt=None, page_id=None):
  """
  getPageKeywordInfo()
  Gets PageKeyword2 entries that match the search criteria provided.

  dbh: A database handle.
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """

  page_keywords = {}
  query = {}

  db = dbh['au_mongo']

  #===== Set up the query
  if (begin_dt and end_dt):
    begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
    end_obj = getDatetimeObj(end_dt + " 23:59:59")
    query['added_dt'] = {'$gte': begin_obj, '$lte': end_obj}
  elif (begin_dt):
    begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
    query['added_dt'] = {'$gte': begin_obj}
  elif (end_dt):
    end_obj = getDatetimeObj(end_dt + " 23:59:59")
    query['added_dt'] = {'$lte': end_obj}
  if (page_id):
    query['page_id'] = page_id

  cursor = db.PageKeyword2.find(query)
  for entry in cursor:
    page_id = entry['page_id']
    if (page_id):
      page_keywords[page_id] = entry

  return(page_keywords)


#==============================================================================
#   G E T  P A G E  K E Y W O R D  I N F O  B Y  V E R S I O N
#==============================================================================
def getPageKeywordInfoByVersion(dbh, version='', limit=''):
  """
  getPageKeywordInfoByVersion()
  Gets PageKeyword2 entries that match the search criteria provided.

  dbh: A database handle.
  version: An version number (e.g. '1.0', optional).
  """

  page_keywords = {}
  query = {}

  try:
    db = dbh['au_mongo']

    #===== Set up the query
    if (version):
      query['version'] = version

    if (limit):
      cursor = db.PageKeyword2.find(query).limit(limit)
    else:
      cursor = db.PageKeyword2.find(query)

    for entry in cursor:
      page_id = entry['page_id']
      if (page_id):
        page_keywords[page_id] = entry

  except Exception as ex:
    print("Error: getPageKeywordInfoByVersion() : " + str(ex))

  return(page_keywords)


#==============================================================================
#   G E T  P A G E  K E Y W O R D  V E R S I O N
#==============================================================================
def getPageKeywordVersion(dbh, page_id):
  """
  getPageKeywordVersion()
  Gets PageKeyword2 parser version for the PageID given.

  dbh: A database handle.
  version: A PageID
  """

  query = {}
  version = ''

  try:
    db = dbh['au_mongo']

    #===== Set up the query
    query['page_id'] = page_id 

    cursor = db.PageKeyword2.find(query)

    for entry in cursor:
      if ('version' in entry):
        version = entry['version']

  except Exception as ex:
    print("Error: getPageKeywordVersion() : " + str(ex))
    version = ''
    pass

  return(version)


#==============================================================================
#    G E T  P A G E  I D S  B Y  Z I P  C O D E
#==============================================================================
def getPageIDsByZipCode(dbh, zip_code):
  """
  getPageIDsByZipCode()
  Gets PageIDs that match the search criteria provided.

  dbh: A database handle.
  zip_code: A zip code
  """

  page_ids = {}
  query = {}

  db = dbh['au_mongo']

  #===== Set up the query
  query['type'] = 'geo_info' 
  query['zip_codes'] = int(zip_code)

  cursor = db.PageInfo.find(query)
  for entry in cursor:
    page_id = entry['page_id']
    if (page_id):
      page_ids[page_id] = 1

  return(page_ids)


#==============================================================================
#    G E T  P A G E  I D S  B Y  Z I P  C O D E S
#==============================================================================
def getPageIDsByZipCodes(dbh, zip_codes):
  """
  getPageIDsByZipCodes()
  Gets PageIDs that match the search criteria provided.

  dbh: A database handle.
  zip_codes: An array of zip codes.
  """

  page_ids = {}
  query = {}

  db = dbh['au_mongo']

  #===== Set up the query
  query['type'] = 'geo_info' 
  query['zip_codes'] = {'$in' : zip_codes}

  cursor = db.PageInfo.find(query)
  for entry in cursor:
    page_id = entry['page_id']
    if (page_id):
      page_ids[page_id] = 1

  return(page_ids)


#==============================================================================
#    I N S E R T  F U L L  U R L  I N F O
#==============================================================================
def insertFullURLInfo(dbh, data_dict):
  """
  insertFullURLInfo()
  """
  result = ''

  try:

    #===== Set up db object
    db = dbh['au_mongo']

    #===== Insert
    result = db.FullURL.insert_one(data_dict)

  except: 
    raise

  return(result)


#==============================================================================
#    I N S E R T  P A G E  I N F O
#==============================================================================
def insertPageInfo(dbh, page_id, page_url, added_dt):
  """
  """
  insert = {}
  result = ''

  try:

    #===== Set up db object
    db = dbh['au_mongo']

    date_obj = getDatetimeObj(added_dt)

    #===== Add info
    insert['id'] = page_id
    insert['url'] = page_url
    insert['added_dt'] = date_obj

    #===== Insert
    result = db.Page.insert_one(insert)

  except: 
    raise

  return(result)


#==============================================================================
#    G E T  E L E M E N T  I N F O
#==============================================================================
def getElementInfo(dbh, element_id='', begin_dt='', end_dt=''):
  """
  getElementInfo()
  Gets Element records that match the search criteria provided.

  dbh: A database handle.
  element_id: An ElementID (optional).
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """

  writeToLog("getElementInfo(" + element_id + "," + begin_dt + "," + end_dt + ")")

  element_info = {}
  query = {}

  try:
    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    if (begin_dt and end_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['added_dt'] = {'$gte': begin_obj, '$lte': end_obj}
    elif (begin_dt):
      begin_obj = getDatetimeObj(begin_dt + " 00:00:00")
      query['added_dt'] = {'$gte': begin_obj}
    elif (end_dt):
      end_obj = getDatetimeObj(end_dt + " 23:59:59")
      query['added_dt'] = {'$lte': end_obj}
    if (element_id):
      query['element_id'] = element_id

    cursor = db.Element.find(query)
    for entry in cursor:
      element_id = entry['element_id']
      if (element_id):
        element_info[element_id] = entry

  except:
    raise

  return(element_info)


#==============================================================================
#    G E T  E L E M E N T  U R L
#==============================================================================
def getElementURL(dbh, element_id):
  """
  getElementURL()
  Gets the url field for an Element.

  dbh: A database handle.
  element_id: An ElementID.
  returns: The url field or nothing.
  """

  writeToLog("getElementURL(" + element_id + ")")

  query = {}
  url = ''

  try:
    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    query['element_id'] = element_id

    #===== Do the query
    cursor = db.Element.find(query)
    for entry in cursor:
      url = entry['url']

  except:
    url = ''
    raise

  return(url)


#==============================================================================
#    G E T  E L E M E N T  A D D E D  D T
#==============================================================================
def getElementAddedDt(dbh, element_id):
  """
  getElementAddedDt()
  Gets the added_dt field for an Element.

  dbh: A database handle.
  element_id: An ElementID.
  returns: The added_dt field or nothing.
  """

  writeToLog("getElementAddedDt(" + element_id + ")")

  query = {}
  added_dt = ''

  try:
    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    query['element_id'] = element_id

    #===== Do the query
    cursor = db.Element.find(query)
    for entry in cursor:
      added_dt = entry['added_dt']

  except:
    raise

  return(added_dt)


#==============================================================================
#    U P D A T E  E L E M E N T  I N F O
#==============================================================================
def updateElementInfo(dbh, data_dict):
  """ 
  Updates an Element record with the contents of a dictionary.

  dbh: A database handle.
  data_dict: A python dictionary holding the data to be inserted
  returns: Nothing if successful, otherwise an error message.
  """

  writeToLog("updateElementInfo()")

  element_id = data_dict['element_id']
  error = ''

  #===== Set up collection
  db = dbh['au_mongo']
  col = db['Element']

  #===== Insert/Update the record
  try:
    result = col.replace_one({'element_id': element_id}, \
                             data_dict, \
                             upsert=True)
  except:
    raise

  return(error)


#==============================================================================
#    G E T  H A R V E S T  I N S T A N C E  I N F O
#==============================================================================
def getHarvestInstanceInfo(dbh, instance_id='', begin_dt='', end_dt=''):
  """
  getHarvestInstanceInfo()
  Gets HarvestInstance records that match the search criteria provided.

  dbh: A database handle.
  instance_id: A HarvestInstanceID (optional)
  begin_dt: A start datetime (optional).
  end_dt: An end datetime (optional).
  """

  writeToLog("getHarvestInstanceInfo(" + instance_id + "," + begin_dt + "," + end_dt + ")")

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
    if (instance_id):
      query['id'] = instance_id

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
#    U P D A T E  H A R V E S T  P R O C  D A T E  T I M E
#==============================================================================
def updateHarvestProcDateTime(dbh, instance_id, process_dt):
  """
  """
  writeToLog("updateHarvestProcDateTime(" + instance_id + ", " + process_dt + ")")

  error = ''

  #===== Set up db object
  db = dbh['au_mongo']
  date_obj = getDatetimeObj(process_dt)

  #===== Insert/Update the record
  try:
    result = db.HarvestInstance.update({'id': instance_id}, \
                            {'$set': {'process_dt': date_obj}})
  except:
    raise

  return(error)


  return


#==============================================================================
#    I N S E R T  H O S T  I N F O
#==============================================================================
def insertHostInfo(dbh, host_id, host_url, added_dt):
  """
  """
  insert = {}
  result = ''

  try:

    #===== Set up db object
    db = dbh['au_mongo']

    date_obj = getDatetimeObj(added_dt)

    #===== Add info
    insert['id'] = host_id
    insert['url'] = host_url
    insert['added_dt'] = date_obj

    #===== Insert
    result = db.Host.insert_one(insert)

  except: 
    raise

  return(result)


#==============================================================================
#    G E T  F U L L  U R L  P A G E  I D
#==============================================================================
def getFullURLPageID(dbh, full_id):
  """
  getFullURLPageID()
  """

  writeToLog("getFullURLPageID(" + full_id + ")")

  query = {}
  page_id = ''

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    query['id'] = full_id

    #===== Do the query
    entry = db.FullURL.find_one(query)

    #===== Store the results
    if (entry and 'page_id' in entry):
      page_id = entry['page_id']

  except:
    raise

  return(page_id)


#==============================================================================
#    G E T  F U L L  U R L  H O S T  I D
#==============================================================================
def getFullURLHostID(dbh, full_id):
  """
  getFullURLHostID()
  """

  writeToLog("getFullURLHostID(" + full_id + ")")

  query = {}
  host_id = ''

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    query['id'] = full_id

    #===== Do the query
    entry = db.FullURL.find_one(query)

    #===== Store the results
    if (entry and 'host_id' in entry):
      host_id = entry['host_id']

  except:
    raise

  return(host_id)


#==============================================================================
#    U P D A T E  I N S T A N C E  I N F O
#==============================================================================
def updateInstanceInfo(dbh, data_dict):
  """ 
  Updates an Instance record with the contents of a dictionary.

  dbh: A database handle.
  data_dict: A python dictionary holding the data to be inserted
  returns: Nothing if successful, otherwise an error message.
  """

  writeToLog("updateInstanceInfo()")

  instance_id = data_dict['id']
  error = ''

  #===== Set up collection
  db = dbh['au_mongo']
  col = db['Instance']

  #===== Insert/Update the record
  try:
    result = col.replace_one({'id': instance_id}, \
                             data_dict, \
                             upsert=True)
  except:
    raise

  return(error)


#==============================================================================
#    I N S T A N C E  P A G E  E X I S T S
#==============================================================================
def instancePageExists(dbh, page_id):
  """
  instancePageExists()
  """
  exists = False
  query = {}
  query2 = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Do query for the linked page
    query['link_page_id'] = page_id
    entry = db.Instance.find_one(query)
    if (entry and 'link_page_id' in entry):
      exists = True

    #===== Do the query for the publisher page
    query2['publ_page_id'] = page_id
    entry = db.Instance.find_one(query2)
    if (entry and 'publ_page_id' in entry):
      exists = True

  except Exception as ex:
    print("Error: " + str(ex))
    exists = False
    
  return(exists) 


#==============================================================================
#    P A G E  K E Y W O R D  P A G E  E X I S T S
#==============================================================================
def pageKeywordPageExists(dbh, page_id):
  """
  """
  exists = False
  query = {}

  try:

    #===== Get our db
    db = dbh['au_mongo']

    query['page_id'] = page_id

    entry = db.PageKeyword2.find_one(query)
    if (entry and 'page_id' in entry):
      exists = True

  except Exception as ex:
    print("Error: " + str(ex))
    exists = False
    
  return(exists) 


#==============================================================================
#    G E T  M I N  M A X  C O L L E C T I O N  F I E L D
#==============================================================================
def getMinMaxCollectionField(dbh, coll_name, field, direction):
  """
  getMinMaxCollectionField()
  """
  result = ''

  if (True):
    print("getMinMaxCollectionField(" + coll_name + ',' + field + ',' + str(direction) + ')')

  try:

    db = dbh['au_mongo']
    cursor = db[coll_name].find().sort(field, direction).limit(1)

    for entry in cursor:
      if (field in entry):
        result = entry[field]

  except Exception as ex:
    print("Error: getMinMaxCollectionField() " + str(ex))
    result = ''
    pass

  return(result)


#==============================================================================
#   I S  L I N K E D  P A G E
#==============================================================================
def isLinkedPage(dbh, page_id):
  """
  isLinkedPage()
  """

  writeToLog("isLinkedPage(" + page_id + ')')

  query = {}
  is_linked = False

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    query['link_page_id'] = page_id

    cursor = db.Instance.find(query)

    for entry in cursor:
      if ('id' in entry):
        is_linked = True

  except Exception as ex:
    raise

  return(is_linked)


#==============================================================================
#   I S  P U B L I S H E R  P A G E
#==============================================================================
def isPublisherPage(dbh, page_id):
  """
  isPublisherPage()
  """

  writeToLog("isPublisherPage(" + page_id + ')')

  query = {}
  is_publ = False

  try:

    #===== Get our db
    db = dbh['au_mongo']

    #===== Set up the query
    query['publ_page_id'] = page_id

    cursor = db.Instance.find(query)

    for entry in cursor:
      if ('id' in entry):
        is_publ = True

  except Exception as ex:
    raise

  return(is_publ)


#==============================================================================
#    W R I T E  T O  L O G
#==============================================================================
def writeToLog(msg):
  """
  """

  try:

    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if (DEBUG and msg):
      with open(LOGFILE, 'a') as logfile:
        logfile.write(ts + " " + msg + "\n")

  except Exception as e:
    print("Error writing to " + LOGFILE + ": " + str(e))
    pass
 
  return 

