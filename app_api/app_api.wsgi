#! /usr/bin/python3.6

import logging
import sys
logging.basicConfig(stream=sys.stderr)
sys.path.insert(0, '/home/hussu/project/bachat_backend/app-api')
from app_api.app_api import app as application
application.secret_key = 'this is a secret'
