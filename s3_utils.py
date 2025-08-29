import boto3


#==============================================================================
#    G E T  S 3  C L I E N T
#==============================================================================
def getS3Client():

  """
  """

  s3_obj = boto3.client('s3')
  return(s3_obj)


#==============================================================================
#    O B J E C T  E X I S T S
#==============================================================================
def objectExists(s3_obj, bucket, filename):
  """
  """
  exists = False

  try:
    results = s3_obj.list_objects(Bucket=bucket, Prefix=filename)
    if ('Contents' in results):
      exists = True

  except:
    raise

  return(exists)


#==============================================================================
#    G E T  O B J E C T  S I Z E
#==============================================================================
def getObjectSize(s3_obj, bucket, filename):
  """
  getObjectSize()
  """
  size = 0

  try:
    response = s3_obj.head_object(Bucket=bucket, Key=filename)
    size = response['ContentLength']

  except Exception as ex:
    print("Error: " + str(ex) + " " + filename)
    size=0
    pass 

  return(size)


#==============================================================================
#    C R E A T E  D I R
#==============================================================================
def createDir(s3_obj, bucket, dirname):
  """
  """

  try:
    if (not objectExists(s3_obj, bucket, dirname)):
      s3_obj.put_object(Bucket=bucket, Key=(dirname+'/'))  
  except:
    raise

  return


#==============================================================================
#    R E M O V E  O B J E C T  F R O M  S 3
#==============================================================================
def removeObjectFromS3(s3_obj, bucket, filename):
  """
  removeObjectFromS3()

  s3_obj: An s3 client object
  """

  try:
    response = s3_obj.delete_object(Bucket=bucket, Key=filename) 

  except Exception as ex:
    print("Error: Unable to remove object " + filename + " : " + str(ex))
    pass

  return(response)
