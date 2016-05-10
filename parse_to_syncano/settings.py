# -*- coding: utf-8 -*-

import os

PARSE_APPLICATION_ID = os.getenv('PARSE_APPLICATION_ID', '')
PARSE_MASTER_KEY = os.getenv('PARSE_MASTER_KEY', '')

SYNCANO_INSTANCE_NAME = os.getenv('SYNCANO_INSTANCE_NAME', '')
SYNCANO_ADMIN_API_KEY = os.getenv('SYNCANO_ADMIN_API_KEY', '')
SYNCANO_APIROOT = os.getenv('SYNCANO_APIROOT', '')

PARSE_PAGINATION_LIMIT = 1000  # the biggest value parse allows

try:
    from local_settings import *  # noqa
except:
    pass
