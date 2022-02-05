# -*- coding: utf-8 -*-

import os
import platform

# deployment
OS = platform.system()
OS_VER = platform.version()
ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MTB_IP = "http://www.ettzzz.ga"
if "Debian" in OS_VER:
    ICU_IP = "localhost"
    DEBUG = False
else:
    ICU_IP = "82.157.178.246"
    DEBUG = True


# project exclusive
DAY_ZERO = "2021-01-01"

STOCK_HISTORY_PATH = os.path.join(ROOT, "trade_history.db")

SELECTED_CODES = ["sh.000001", "sh.000003", "sh.000905"]

# web service
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0"
BOT_DISPATCH_ADDRESS = "{}/api_v1/send_message".format(MTB_IP)
NEWS_HOST = "http://{}:7706/api_v1".format(ICU_IP)

# logging
LOGGING_FMT = "%(asctime)s %(levelname)s %(funcName)s in %(filename)s: %(message)s"
LOGGING_DATE_FMT = "%Y-%m-%d %a %H:%M:%S"
LOGGING_NAME = "jibberjabber"
STREAMING_FMT = "%(levelname)s %(funcName)s %(message)s"
