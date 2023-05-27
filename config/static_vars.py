# -*- coding: utf-8 -*-

import os


# deployment
ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DEBUG = os.getenv("DEBUG") != "0"
DB_NAME = "offline_stock"
MONGO_URI = os.getenv("AZURE_MONGO_URI")

# project exclusive
DAY_ZERO = "2019-01-01"
SELECTED_CODES = ["sh.000001", "sh.000003", "sh.000905"]

# web service
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0"


