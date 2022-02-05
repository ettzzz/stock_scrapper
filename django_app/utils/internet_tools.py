# -*- coding: utf-8 -*-

import requests

from config.static_vars import BOT_DISPATCH_ADDRESS, NEWS_HOST


def call_bot_dispatch(to, link, text):
    r = requests.post(BOT_DISPATCH_ADDRESS, data={"to": to, "link": link, "text": text})
    return r.json()


def get_train_news_weight(start_date, end_date):
    # NEWS_HOST = "http://127.0.0.1:7705/api_v1"
    r = requests.post(
        "{}/historical_weight".format(NEWS_HOST),
        json={
            "start_date": start_date,
            "end_date": end_date,
        },  # 学习了 data是给form data的
    )
    return r.json()
