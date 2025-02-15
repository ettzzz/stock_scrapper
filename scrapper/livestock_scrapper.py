#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:59:58 2022

@author: eee
"""

import random

import requests

from utils.datetime_tools import get_now

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0"

class liveStockScraper:
    def _sh_formatter(self, before, cat="date"):
        if cat == "date":
            before = str(before)
            after = "-".join([before[:4], before[4:6], before[6:]])
        elif cat == "time":
            before = str(before)
            after = ":".join([before[:2], before[2:4]])
        else:
            after = before
        return after

    def _sz_formatter(self, before, cat="time"):
        if cat == "time":
            after = before.split(" ")[-1][:5]
        elif cat == "date":
            after = before.split(" ")[0]
        else:
            after = before
        return after

    def sh_live_k_data(self, code, data_type="min"):
        """
        http://www.sse.com.cn/market/price/trends/index.shtml?code=SH000004
        """
        headers = {
            "User-Agent": UA,
            "Host": "yunhq.sse.com.cn:32041",
            "Referer": "http://www.sse.com.cn/",
        }

        if data_type == "min":
            base_url = "http://yunhq.sse.com.cn:32041//v1/sh1/line/{}".format(
                code.split(".")[-1]
            )
            params = {
                "begin": -31,
                "end": -1,
                "select": "time,price,volume",
                "_": round(get_now() * 1000),
            }
        else:  # data_type == 'day':
            base_url = "http://yunhq.sse.com.cn:32041//v1/sh1/dayk/{}".format(
                code.split(".")[-1]
            )
            params = {
                "begin": -3,
                "end": -1,
                "select": "date,open,high,low,close,volume",
                "_": round(get_now() * 1000),
            }

        r = requests.get(base_url, params=params, headers=headers)

        # date, _time, _open, high, low, _close = each_min

        result = []
        if r.status_code == 200:
            response = r.json()
            live_data = [
                self._sh_formatter(response["date"], cat="date"),
                self._sh_formatter(response["time"], cat="time"),
                response["line"][0][1],
                response["highest"],
                response["lowest"],
                response["line"][-1][1],
            ]
        else:
            live_data = []

        result.append(live_data)
        return result

    def sz_live_k_data(self, code):
        """
        http://www.szse.cn/market/trend/index.html?code=399623
        """
        headers = {
            "User-Agent": UA,
            "Host": "www.szse.cn",
            "Referer": "http://www.szse.cn/market/trend/index.html?code={}".format(
                code.split(".")[-1]
            ),
        }
        base_url = "http://www.szse.cn/api/market/ssjjhq/getTimeData"
        params = {"random": random.random(), "marketId": 1, "code": code.split(".")[-1]}
        r = requests.get(base_url, params=params, headers=headers)

        # response['data']['picupdata'][0] structure:
        # time, close, avg, pctChg, 涨跌幅？,share, volume
        # date, _time, _open, high, low, _close = each_min
        result = []
        if r.status_code == 200:
            response = r.json()
            live_data = [
                self._sz_formatter(response["data"]["marketTime"], cat="date"),
                self._sz_formatter(response["data"]["marketTime"], cat="time"),
                float(response["data"]["picupdata"][0][1]),  # response['data']['open'],
                float(response["data"]["high"]),
                float(response["data"]["low"]),
                float(response["data"]["picupdata"][-1][1]),
            ]
        else:
            live_data = []

        result.append(live_data)
        return result
