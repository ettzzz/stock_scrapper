#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:40:49 2022

@author: eee
"""


import requests
import baostock as bs
from bs4 import BeautifulSoup

from config.static_vars import UA, DAY_ZERO


def call_bs_func(bs_func, *args, **kwargs):
    raw = bs_func(*args, **kwargs)
    if raw.error_msg == "网络接收错误。":
        bs.login()
        raw = bs_func(*args, **kwargs)
    return raw


def jsonify_bs_response(baostock_raw):
    data_list = list()
    _data_fields = baostock_raw.fields
    while (baostock_raw.error_code == "0") & baostock_raw.next():
        # data_list.append(baostock_raw.get_row_data())
        d = dict(zip(baostock_raw.fields, baostock_raw.get_row_data()))
        data_list.append(d)

    return data_list, _data_fields


class stockScrapper:
    def __init__(self):
        bs.login()

    def get_open_days(self, start_date, end_date):
        raw = call_bs_func(bs.query_trade_dates, start_date, end_date)
        data, fields = jsonify_bs_response(raw)
        open_days = [d["calendar_date"] for d in data if d["is_trading_day"] == "1"]
        return open_days

    def if_date_open(self, date):
        open_days = self.get_open_days(date, date)
        return len(open_days) > 0

    def scrape_k_data(self, config):
        """
        config = {
            'code': 'sh.600006',
            'fields': "self.stock_fields['minute']",
            'start_date': '2019-05-13',
            'end_date': '2019-05-31',
            'frequency': 'd',
            'adjustflag': '1'
            }
        """
        raw = call_bs_func(bs.query_history_k_data_plus, **config)
        data, fields = jsonify_bs_response(raw)
        return data, fields

    def scrape_whole_pool_data(self, update_date=DAY_ZERO):
        config = {"date": update_date}
        raw = call_bs_func(bs.query_stock_industry, **config)
        # raw_knock = call_bs_func(
        #     bs.security.sectorinfo.query_terminated_stocks, **config
        # ) ## codes are knocked out
        raw_zz500 = call_bs_func(bs.query_zz500_stocks, **config)
        raw_hs300 = call_bs_func(bs.query_hs300_stocks, **config)
        raw_sz50 = call_bs_func(bs.query_sz50_stocks, **config)

        data, fields = jsonify_bs_response(raw)
        # data_knock, fields_knock = jsonify_bs_response(raw_knock)
        data_zz500, fields_zz500 = jsonify_bs_response(raw_zz500)
        data_hs300, fields_hs300 = jsonify_bs_response(raw_hs300)
        data_sz50, fields_sz50 = jsonify_bs_response(raw_sz50)

        extra_attr = {
            "is_zz500": {i["code"]: 1 for i in data_zz500},
            "is_hs300": {i["code"]: 1 for i in data_hs300},
            "is_sz50": {i["code"]: 1 for i in data_sz50},
        }
        for idx, d in enumerate(data):
            code = d["code"]
            appendix = {"is_zz500": 0, "is_hs300": 0, "is_sz50": 0}
            for key, mapping in extra_attr.items():
                if mapping.get(code) == 1:
                    appendix.update({key: 1})

            data[idx].update(appendix)

        return data, fields

    def scrape_feature_list(self):
        """
        Could be an absolutely fragile function, no exception handling at all.
        """
        r = requests.get(
            url="http://baostock.com/baostock/index.php/公式与数据格式说明",
            headers={"User-Agent": UA},
        )
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.select("table.wikitable")

        code_pool = []
        features = []
        for t in tables[:3]:  # only first 4 tables are useful
            bullets = t.select("tr")
            for b in bullets[1:]:  # first one row is table title
                content = [i.get_text().strip() for i in b.select("td")]
                if content[0] in code_pool:
                    continue
                else:
                    code_pool.append(content[0])
                    features.append(content)  # kind of stupid way but it works

        return features
