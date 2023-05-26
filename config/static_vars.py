# -*- coding: utf-8 -*-

import os


# deployment
ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
IS_DEBUG = os.getenv("IS_DEBUG") != "0"
DB_NAME = "offline_stock"
MONGO_URI = os.getenv("MONGO_URI")

# project exclusive
DAY_ZERO = "2019-01-01"
SELECTED_CODES = ["sh.000001", "sh.000003", "sh.000905"]

# web service
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0"

# logging
LOGGING_FMT = "%(asctime)s %(levelname)s %(funcName)s in %(filename)s: %(message)s"
LOGGING_DATE_FMT = "%Y-%m-%d %a %H:%M:%S"
LOGGING_NAME = "jibberjabber"
STREAMING_FMT = "%(levelname)s %(funcName)s %(message)s"
