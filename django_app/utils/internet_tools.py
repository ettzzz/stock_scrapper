# -*- coding: utf-8 -*-

import requests

from config.static_vars import BOT_DISPATCH_ADDRESS

def call_bot_dispatch(to, link, text):
    r = requests.post(
        BOT_DISPATCH_ADDRESS,
        data = {
            'to': to,
            'link': link,
            'text': text
            }
        )
    return r.json()
