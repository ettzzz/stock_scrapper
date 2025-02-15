
from datetime import datetime, timedelta

import requests
import baostock as bs
import baostock.common.contants as cons
import pandas as pd
from bs4 import BeautifulSoup


class ConnectionManager:
    def __init__(self, user_id='anonymous', password='123456'):
        self.user_id = user_id
        self.password = password
        self.is_connected = False

    def ensure_connected(self):
        if not self.is_connected:
            print("Reconnecting...")
            result = bs.login(self.user_id, self.password)
            if result.error_code == cons.BSERR_SUCCESS:
                self.is_connected = True
            else:
                raise Exception("Failed to login: " + result.error_msg)

    def close_connection(self):
        if self.is_connected:
            result = bs.logout(self.user_id)
            if result.error_code == cons.BSERR_SUCCESS:
                self.is_connected = False
            else:
                raise Exception("Failed to logout: " + result.error_msg)

    def __enter__(self):
        self.ensure_connected()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def is_connection_lost(self):
        try:
            # 尝试发送一个简单的查询来检测连接
            rs = bs.query_trade_dates(start_date="2024-01-01", end_date="2024-01-07")
            if rs.error_code != cons.BSERR_SUCCESS:
                return True
            return False
        except Exception as e:
            print("Connection lost:", e)
            return True


class stockScrapper:
    def __init__(self):
        self.connect_manager = ConnectionManager()
        self.connect_manager.ensure_connected()
        self.fields_mapping = {
            "d": "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST",
            "w": "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg",
            "m": "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg",
        }


    def date_yielder(self, start_date, end_date, interval_days):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    
        while start < end:
            next_start = start + timedelta(days=interval_days)
            yield [start.strftime("%Y-%m-%d"), min(next_start, end).strftime("%Y-%m-%d")]
            start = next_start


    def get_all_codes_df(self):
        if self.connect_manager.is_connection_lost():
            self.connect_manager.ensure_connected()
        rs = bs.query_stock_basic()
        df = rs.get_data()
        return df


    def get_k_data_df(self, code, frequency, start_date, end_date):
        all_dfs = []

        dates = self.date_yielder(start_date, end_date, interval_days=30)
        for sd, ed in dates:
            if self.connect_manager.is_connection_lost():
                self.connect_manager.ensure_connected()
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=self.fields_mapping[frequency], 
                start_date=sd, 
                end_date=ed, 
                frequency=frequency
                )
            all_dfs.append(rs.get_data())

        df = pd.concat(all_dfs)

        return df


if __name__ == "__main__":
    t = stockScrapper()
    # all_codes = t.get_all_codes()

    code = 'sh.600006'
    start_date = '2023-10-13'
    end_date = '2024-10-23'
    frequency = 'd'
    
    df = t.get_k_data_df(code, frequency, start_date, end_date)

    print("helloworld")



