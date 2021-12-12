# -*- coding: utf-8 -*-
import random
import traceback

from bs4 import BeautifulSoup
import requests
import baostock as bs

from utils.datetime_tools import get_now
from config.static_vars import UA, DAY_ZERO


class stockScraper:
    def _relogin(self):
        bs.login()

    def call_baostock(self, baostock_raw):
        data_list = []
        while (baostock_raw.error_code == "0") & baostock_raw.next():
            data_list.append(baostock_raw.get_row_data())
        if len(data_list) == 0:
            return [], []
        else:
            return data_list, baostock_raw.fields

    def get_open_days(self, start_date, end_date):
        raw = bs.query_trade_dates(start_date, end_date)
        if raw.error_msg == "网络接收错误。":
            self._relogin()
            raw = bs.query_trade_dates(start_date, end_date)
        data, fields = self.call_baostock(raw)
        open_days = [d[0] for d in data if d[1] == "1"]
        return open_days

    def scrape_k_data(self, config):
        """
        config = {
            'code': 'sh.600000',
            'fields': self.stock_fields['minute'],
            'start_date': '2019-05-13',
            'end_date': '2019-05-31',
            'frequency': '30',
            'adjustflag': '1'
            }
        """
        raw = bs.query_history_k_data_plus(**config)
        if raw.error_msg == "网络接收错误。":
            self._relogin()
            raw = bs.query_history_k_data_plus(**config)

        data, fields = self.call_baostock(raw)
        return data, fields

    def scrape_whole_pool_data(self, update_date=DAY_ZERO):
        config = {"date": update_date}

        raw = bs.query_stock_industry(**config)
        raw_knock = bs.security.sectorinfo.query_terminated_stocks(**config)
        raw_zz500 = bs.query_zz500_stocks(**config)
        raw_hs300 = bs.query_hs300_stocks(**config)
        raw_sz50 = bs.query_sz50_stocks(**config)
        if any(r.error_msg == "网络接收错误。" for r in [raw, raw_zz500, raw_hs300, raw_sz50]):
            self._relogin()
            raw = bs.query_stock_industry(**config)
            raw_knock = bs.security.sectorinfo.query_terminated_stocks(**config)
            raw_zz500 = bs.query_zz500_stocks(**config)
            raw_hs300 = bs.query_hs300_stocks(**config)
            raw_sz50 = bs.query_sz50_stocks(**config)

        data, fields = self.call_baostock(raw)
        data_knock, fields_knock = self.call_baostock(raw_knock)
        data_zz500, fields_zz500 = self.call_baostock(raw_zz500)
        data_hs300, fields_hs300 = self.call_baostock(raw_hs300)
        data_sz50, fields_sz50 = self.call_baostock(raw_sz50)

        extra_fields = ["is_zz500", "is_hs300", "is_sz50"]
        fields += extra_fields

        store = [
            [i[1] for i in data_zz500],
            [i[1] for i in data_hs300],
            [i[1] for i in data_sz50],
        ]
        knock = [k[1] for k in data_knock]

        for idx, d in enumerate(data):
            code = d[1]
            is_data = ["0", "0", "0"]
            if code not in knock:
                for i in range(len(is_data)):
                    if code in store[i]:
                        is_data[i] = "1"
            else:
                pass
            data[idx] += is_data

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

    def scrape_feature_data(self, feature_codes, start_date, end_date):
        feature_codes_str = [c[0] for c in feature_codes]

        dates, _ = self.scrape_k_data(
            {
                "code": "sh.000001",
                "fields": "date",
                "start_date": start_date,
                "end_date": end_date,
                "frequency": "d",
                "adjustflag": "1",
            }
        )  # get valid dates first

        store = {date[0]: [] for date in dates}
        for idx, code in enumerate(feature_codes):
            code = code[0]
            config = {
                "code": code,
                "fields": "date,pctChg",
                "start_date": start_date,
                "end_date": end_date,
                "frequency": "d",
                "adjustflag": "1",
            }
            fetched, _ = self.scrape_k_data(config)
            temp_dict = dict(fetched)
            for date in store:
                if date in temp_dict:
                    store[date].append(temp_dict[date])
                else:
                    store[date].append("0")

        stacks = []
        for date, indices in store.items():
            stacks.append([date, ",".join(feature_codes_str), ",".join(indices)])

        return stacks


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
