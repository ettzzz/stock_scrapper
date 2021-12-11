# -*- coding: utf-8 -*-

import os
import platform

# deployment
OS = platform.system()
OS_VER = platform.version()
ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MTB_IP = "http://www.ettzzz.ga"
if "Debian" in OS_VER:
    DEBUG = False
else:
    DEBUG = True

# project exclusive
DAY_ZERO = "2019-01-01"

STOCK_HISTORY_PATH = os.path.join(ROOT, "trade_history.db")

# SELECTED_CODES = [
#     'sh.000001', 'sh.000003', 'sz.399908', 'sz.399909',
#     'sz.399910', 'sz.399911', 'sz.399912', 'sz.399913',
#     'sz.399914', 'sz.399915', 'sz.399916', 'sz.399917'
# ] # tobe deprecated soon
SELECTED_CODES = ["sh.000001", "sh.000003", "sh.000905"]

# web service
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0"
BOT_DISPATCH_ADDRESS = "{}/api_v1/send_message".format(MTB_IP)

# logging
LOGGING_FMT = "%(asctime)s %(levelname)s %(funcName)s in %(filename)s: %(message)s"
LOGGING_DATE_FMT = "%Y-%m-%d %a %H:%M:%S"
LOGGING_NAME = "jibberjabber"
STREAMING_FMT = "%(levelname)s %(funcName)s %(message)s"
