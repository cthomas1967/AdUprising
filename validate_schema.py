#==============================================================================
"""
validate_schema.py
"""
#==============================================================================

import re
from datetime import datetime as dt
from schema import *


#==============================================================================
#    H A R V E S T  S C H E M A
#==============================================================================
"""
"""
harvest_schema = Schema({
    'id': And(Use(str), lambda x: re.findall(r"([a-f\d]{32})", x)),
    'crawl_id': And(Use(int)),
    #'harvest_dt': And(lambda x: type(x) == 'datetime.datetime'),
    'profile_name': And(Use(str)),
    'device_info': {
        'name': And(Use(str)),
        'userAgent': And(Use(str)),
        'viewport': {
          'width' : And(Use(int), lambda n: n >= 0),
          'height' : And(Use(int), lambda n: n >= 0),
          'deviceScaleFactor' : And(Use(int)),
          'isMobile' : And(Use(bool)),
          'hasTouch' : And(Use(bool)),
          'isLandscape' : And(Use(bool))
      }
    },
    'device_type': Or(Use(str)),
    'environment': And(Use(str), lambda x: x in ('desktop', 'mobile')),
    'publisher_url': Or(Use(str)),
    'element_url': Or(Use(str)),
    'linked_url': Or(Use(str)),
    'creative_type': And(Use(str), lambda x: x in ('video', 'display')),
    'creative_width': And(Use(int), lambda n: n >= 0),
    'creative_height': And(Use(int), lambda n: n >= 0),
    'srcs': Or(Use(str)),
    #'process_dt': Or(Use(datetime.datetime), None, only_one=True),
    'version': And(Use(float)),
}, ignore_extra_keys=True)


#==============================================================================
#    V A L I D A T E  S C H E M A
#==============================================================================
def validateSchema(schema, test_dict):
  """
  """
  valid = False
  error = ''

  try:
    schema.validate(test_dict)
    valid = True
  except SchemaError as ex:
    valid = False
    error = str(ex)
    pass
 
  return(valid, error)
