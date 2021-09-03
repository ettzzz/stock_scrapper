# -*- coding: utf-8 -*-

import os
import platform

OS = platform.system()
ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

if OS == 'Linux':
    DEBUG = False
else:
    DEBUG = True

IS_STOCK_WHOLE = True
if IS_STOCK_WHOLE:
    STOCK_HISTORY_PATH = os.path.join(ROOT, 'trade_history_whole.db')
else:
    STOCK_HISTORY_PATH = os.path.join(ROOT, 'trade_history_zz500.db')
DAY_ZERO = '2019-01-01'
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0'
MTB_IP = 'http://www.ettzzz.ga'
BOT_DISPATCH_ADDRESS = '{}/api_v1/send_message'.format(MTB_IP)

SELECTED_CODES = [
    'sh.000001', 'sh.000003', 'sz.399908', 'sz.399909',
    'sz.399910', 'sz.399911', 'sz.399912', 'sz.399913',
    'sz.399914', 'sz.399915', 'sz.399916', 'sz.399917'
]

selected_features = list(map(lambda x: x.replace('.', '_'), SELECTED_CODES))
