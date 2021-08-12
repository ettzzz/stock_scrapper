# -*- coding: utf-8 -*-

import os

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
STOCK_HISTORY_PATH = os.path.join(ROOT, 'trade_history.db')
DAY_ZERO = '2019-01-01'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0'
MTB_IP = 'https://api.ettzzz.nl'
BOT_DISPATCH_ADDRESS = '{}/api_v1/send_message'.format(MTB_IP)
