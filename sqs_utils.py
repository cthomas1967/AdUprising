import boto3


#=============================================================================
#    C O N F I G
#=============================================================================
NLP_QUEUE = 'https://sqs.us-east-1.amazonaws.com/757768816950/NLP_PROCESSING.fifo'
HTML_QUEUE = 'https://sqs.us-east-1.amazonaws.com/757768816950/HTML_RETRIEVAL.fifo'
HARVEST_QUEUE = 'https://sqs.us-east-1.amazonaws.com/757768816950/HARVEST_INSTANCE_PROCESSING.fifo'
ELEMENT_QUEUE = 'https://sqs.us-east-1.amazonaws.com/757768816950/ELEMENT_RETRIEVAL.fifo'
VIDEO_QUEUE = 'https://sqs.us-east-1.amazonaws.com/757768816950/VIDEO_PROCESSING.fifo'
DEALER_QUEUE = 'https://sqs.us-east-1.amazonaws.com/757768816950/DEALER_PAGE_PROCESSING.fifo'
VISIBILITY_TIMEOUT=60
#DEBUG = True
DEBUG = False


#==============================================================================
#    G E T  S Q S  C L I E N T
#==============================================================================
def getSQSClient():

  """
  """

  # Create SQS client
  sqs = boto3.client('sqs', \
                     region_name='us-east-1' \
                    )

  return(sqs)


#==============================================================================
#    G E T  M E S S A G E  F R O M  Q U E U E
#==============================================================================
def getMessageFromQueue(sqs, queue_url):

  message = ''
  receipt_handle = ''

  # Receive message from SQS queue
  response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=[
      'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=VISIBILITY_TIMEOUT,
    WaitTimeSeconds=5
  )

  if 'Messages' in response:
    message = response['Messages'][0]
  if (message):
    receipt_handle = message['ReceiptHandle']

  return(message, receipt_handle)


#==============================================================================
#    P R E P A R E  E L E M E N T  M E S S A G E  A T T R I B U T E S
#==============================================================================
def prepareElementMessageAttributes(element_id, url, harvest_dt, tries):

  """
  """

  attrs =  {
      'element_id': {
        'DataType': 'String',
        'StringValue': element_id
      },
      'harvest_dt': {
        'DataType': 'String',
        'StringValue': harvest_dt 
      },
      'tries': {
        'DataType': 'Number',
        'StringValue': tries
      },
      'url': {
        'DataType': 'String',
        'StringValue': url
      }
    }

  return(attrs)


#==============================================================================
#    P R E P A R E  V I D E O  M E S S A G E  A T T R I B U T E S
#==============================================================================
def prepareVideoMessageAttributes(harvest_id, tries):

  """
  """

  attrs =  {
      'harvest_id': {
        'DataType': 'String',
        'StringValue': harvest_id
      },
      'tries': {
        'DataType': 'Number',
        'StringValue': tries
      }
    }

  return(attrs)



#==============================================================================
#    P R E P A R E  H T M L  M E S S A G E  A T T R I B U T E S
#==============================================================================
def prepareHTMLMessageAttributes(page_id, page_url, tries):

  """
  """

  attrs =  {
      'page_id': {
        'DataType': 'String',
        'StringValue': page_id
      },
      'tries': {
        'DataType': 'Number',
        'StringValue': tries
      },
      'page_url': {
        'DataType': 'String',
        'StringValue': page_url
      }
    }

  return(attrs)


#==============================================================================
#    P R E P A R E  N L P  M E S S A G E  A T T R I B U T E S
#==============================================================================
def prepareNLPMessageAttributes(page_id, tries):

  """
  prepareNLPMessageAttributes()
  """

  attrs =  {
      'page_id': {
        'DataType': 'String',
        'StringValue': page_id
      },
      'tries': {
        'DataType': 'Number',
        'StringValue': tries
      }
    }

  return(attrs)


#==============================================================================
#    P R E P A R E  H A R V E S T  M E S S A G E  A T T R I B U T E S
#==============================================================================
def prepareHarvestMessageAttributes(instance_id, tries):

  """
  prepareHarvestMessageAttributes()
  """

  attrs =  {
      'id': {
        'DataType': 'String',
        'StringValue': instance_id
      },
      'tries': {
        'DataType': 'Number',
        'StringValue': tries
      }
    }

  return(attrs)


#==============================================================================
#    P R E P A R E  D E A L E R  M E S S A G E  A T T R I B U T E S
#==============================================================================
def prepareDealerMessageAttributes(dealer, invent_page):

  """
  prepareDealerMessageAttributes()
  """

  attrs =  {
    'dealer': {
      'DataType': 'String',
      'StringValue': dealer
    },
    'inventory_page': {
      'DataType': 'String',
      'StringValue': invent_page
    }
  }

  return(attrs)


#==============================================================================
#    S E N D  M E S S A G E  T O  E L E M E N T  Q U E U E
#==============================================================================
def sendMessageToElementQueue(sqs, element_id, url, harvest_dt, tries, dedup_id):
  """
  sendMessageToElementQueue()
  """

  try:
    attrs = prepareElementMessageAttributes(element_id, url, harvest_dt, tries)
    msg_body = "ElementID: " + element_id
    response = sendMessage(sqs, ELEMENT_QUEUE, attrs, msg_body, dedup_id)
  except:
    raise

  return(response)


#==============================================================================
#    S E N D  M E S S A G E  T O  H T M L  Q U E U E
#==============================================================================
def sendMessageToHTMLQueue(sqs, page_id, page_url, tries, dedup_id):
  """
  sendMessageToHTMLQueue()
  """

  try:
    attrs = prepareHTMLMessageAttributes(page_id, page_url, tries)
    msg_body = "PageID: " + page_id
    response = sendMessage(sqs, HTML_QUEUE, attrs, msg_body, dedup_id)
  except:
    raise

  return(response)


#==============================================================================
#    S E N D  M E S S A G E  T O  N L P  Q U E U E
#==============================================================================
def sendMessageToNLPQueue(sqs, page_id, tries, dedup_id):
  """
  sendMessageToNLPQueue()
  """

  try:
    attrs = prepareNLPMessageAttributes(page_id, tries)
    msg_body = "PageID: " + page_id
    response = sendMessage(sqs, NLP_QUEUE, attrs, msg_body, dedup_id)
  except:
    raise

  return(response)


#==============================================================================
#    S E N D  M E S S A G E  T O  H A R V E S T  Q U E U E
#==============================================================================
def sendMessageToHarvestQueue(sqs, instance_id, tries, dedup_id):
  """
  sendMessageToHarvestQueue()
  """

  try:
    attrs = prepareHarvestMessageAttributes(instance_id, tries)
    msg_body = "HarvestInstanceID: " + instance_id
    response = sendMessage(sqs, HARVEST_QUEUE, attrs, msg_body, dedup_id)
  except:
    raise

  return(response)


#==============================================================================
#    S E N D  M E S S A G E  T O  V I D E O  Q U E U E
#==============================================================================
def sendMessageToVideoQueue(sqs, harvest_id, tries, dedup_id):
  """
  sendMessageToVideoQueue()
  """

  try:
    attrs = prepareVideoMessageAttributes(harvest_id, tries)
    msg_body = "HarvestInstanceID: " + harvest_id
    response = sendMessage(sqs, VIDEO_QUEUE, attrs, msg_body, dedup_id)
  except:
    raise

  return(response)


#==============================================================================
#    S E N D  M E S S A G E  T O  D E A L E R   Q U E U E
#==============================================================================
def sendMessageToDealerQueue(sqs, dealer, invent_page, dedup_id):
  """
  sendMessageToDealerQueue()
  """

  try:
    attrs = prepareDealerMessageAttributes(dealer, invent_page)
    msg_body = "Dealer: " + dealer + "\n"
    msg_body = "Inventory Page: " + invent_page + "\n"
    response = sendMessage(sqs, DEALER_QUEUE, attrs, msg_body, dedup_id)
  except:
    raise

  return(response)


#==============================================================================
#    S E N D  M E S S A G E
#==============================================================================
def sendMessage(sqs, queue_url, attrs, msg_body, dedup_id):

  """
  sendMessage()
  """

  result = ''

  try:

    # Send message to SQS queue
    response = sqs.send_message(
      QueueUrl = queue_url,
      DelaySeconds = 0,
      MessageAttributes = attrs,
      MessageBody=msg_body,
      MessageGroupId='586474de88e03',
      MessageDeduplicationId=dedup_id
    )

    if (response and 'MessageId' in response):
      result = response['MessageId']   
  except Exception as e:
    result = str(e)
    pass


  return(result)


#==============================================================================
#    D E L E T E  M E S S A G E
#==============================================================================
def deleteMessage(sqs, queue_url, receipt_handle):
 
  """
  """

  try:
    # Delete received message from queue
    sqs.delete_message(
      QueueUrl=queue_url,
      ReceiptHandle=receipt_handle
    )

  except:
    raise

  return
