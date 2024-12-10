
from datetime import datetime, timedelta

import requests
import baostock as bs
import pandas as pd
from bs4 import BeautifulSoup

# from config.static_vars import UA, DAY_ZERO


class stockScrapper:
    def __init__(self):
        self.fields_mapping = {
            "d": "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST",
            "w": "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg",
            "m": "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg",
        }

    def relogin(self):
        self.lg = bs.login()


    def date_yielder(self, start_date, end_date, interval_days):
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    
        current_date = start
        while current_date <= end:
            # Prepare a list to hold dates for the current interval
            date_list = []
            # Collect dates for the current interval
            for _ in range(interval_days):
                if current_date <= end:
                    date_list.append(current_date.strftime("%Y-%m-%d"))
                    current_date += timedelta(days=1)
                else:
                    break
            
            # Yield the collected date list
            yield date_list


    def get_all_codes_df(self):
        lg = bs.login()
        rs = bs.query_stock_basic()
        df = rs.get_data() # pd.dataframe
        bs.logout()
        return df


    def get_k_data_df(self, code, frequency, start_date, end_date):
        lg = bs.login()
        all_dfs = []

        dates = self.date_yielder(start_date, end_date, interval_days=30)
        for date_list in dates:
            sd, ed = date_list[0], date_list[-1]
            rs = bs.query_history_k_data_plus(
                code=code,
                fields=self.fields_mapping[frequency], 
                start_date=sd, 
                end_date=ed, 
                frequency=frequency
                )

            all_dfs.append(rs.get_data())

        df = pd.concat(all_dfs)

        bs.logout()
        return df

    


if __name__ == "__main__":
    t = stockScrapper()
    # all_codes = t.get_all_codes()

    code = 'sh.600006'
    start_date = '2024-10-13'
    end_date = '2024-10-23'
    frequency = 'd'
    df = t.get_k_data_df(code, frequency, start_date, end_date)

    print("helloworld")



