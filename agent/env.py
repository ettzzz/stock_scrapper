
import random

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from database.stock_operator_v2 import StockDatabase
from scrapper.baostock_scrapper_v2 import stockScrapper

class StockDataloader:
    def __init__(self):
        temporal = 'd'
        self.db = StockDatabase()
        self.scrapper = stockScrapper()
        all_codes = self.db.conn.execute(f"SELECT DISTINCT(code) FROM {temporal}_record;").fetchall()
        self.all_codes = [a[0] for a in all_codes]
        # all_codes_df = self.scrapper.get_all_codes_df()
        # self.all_codes_df = all_codes_df[all_codes_df["code"].isin(self.all_codes)]
        self.features = ["pctChg", "turn", "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "K", "D", "J"]
        # self.features = ["pctChg", "turn"] ## only for week month
        
    def append_kdj(self, df, temporal):
        K, D, J = [], [], []
        for idx, row in df.iterrows():
            if row["high"] - row["low"] == 0:
                rsv = 0
            else:
                rsv = ((row["close"] - row["low"]) / (row["high"] - row["low"])) * 100
            if idx == 0:
                k = 50
                d = 50
            else:
                k = round(0.666*K[-1]+0.333*rsv, 2)
                d = round(0.666*D[-1]+0.333*k, 2)
            j = round(3*k - 2*d, 2)

            K.append(k)
            D.append(d)
            J.append(j)
        df["K"] = K
        df["D"] = D
        df["J"] = J

        return df

    def get_raw_data(self, code, start_date="", end_date=""):
        # if code not in self.all_codes_df["code"].unique():
        #     return None
        # code_df = self.all_codes_df[self.all_codes_df["code"] == code]
        # row = code_df.iloc[0]
        # if len(start_date) >= 0 and start_date < row["ipoDate"]:
        #     start_date = row["ipoDate"]

        # if len(end_date) > 0 and row["outDate"] is not None: # TODO: outdate
        #     end_date = row["outDate"]
        temporal_dfs = []
        # for temporal in ['d', 'w', 'm']: ## m: last day of the month, w: last day of the month
        for temporal in ['d']:
            df = self.db.fetch_record(temporal, code, start_date, end_date)
            if len(df) == 0:
                return None
            df = self.append_kdj(df, temporal)
            df["temporal"] = temporal
            temporal_dfs.append(df)

        temporal_df = pd.concat(temporal_dfs)
        return temporal_df


    def get_reward(self, reward_df, style="expected"):
        net_value = 1.0
        max_value = []
        for pctChg in reward_df["pctChg"]:
            net_value *= (1+pctChg)
            max_value.append(net_value)         
        
        if style == "expected":
            return round(net_value - 1, 4)
        else:
            return round(max(max_value) - 1, 4)

    def get_status(self, status_df):
        return np.array(status_df[self.features])


    def yield_batch_data(self, step_size, reward_size, batch_size=4, start_date="", end_date=""):
        status, reward, status_, trace_meta = [], [], [], []
        warmup_steps = 22
        # for idx, row in self.all_codes_df.iterrows():
        #     code = row["code"]
        for code in self.all_codes:
            code_df =self.get_raw_data(code, start_date=start_date, end_date=end_date)
            if code_df is None or step_size + reward_size >= len(code_df):
                continue

            print(code, "code_df len is", len(code_df))
            code_df[self.features] = code_df[self.features].replace('', np.nan)
            code_df[self.features] = code_df[self.features].ffill()
            code_df[self.features] = code_df[self.features].fillna(0)
            for k in ["pctChg", "turn", "pcfNcfTTM", "peTTM", "K", "D", "J"]:
                if k in code_df.columns:
                    code_df[k] = round(code_df[k]/100, 4)
        
            for i in range(warmup_steps, len(code_df)-step_size-reward_size-1): ## -1 for status_
                s = self.get_status(code_df[i: i+step_size])
                s_ = self.get_status(code_df[i+1: i+step_size+1])
                r = self.get_reward(code_df[i+step_size: i+step_size+reward_size])
                m = code_df.iloc[i: i+step_size][["code", "date", "pctChg"]].values.tolist()
                if random.random() > 0.5:
                    status.append(s)
                    status_.append(s_)
                    reward.append(r)
                    trace_meta.append(m)
                if len(reward) >= batch_size:
                    S = np.array(status)
                    S_ = np.array(status_)
                    R = np.array(reward)
                    yield S, S_, R, trace_meta
                    status.clear()
                    status_.clear()
                    reward.clear()
                    trace_meta.clear()


if __name__ == "__main__":
    t = StockDataloader()
    step_size = 63
    reward_size = 10
    start_date="" 
    end_date=""
    gen = t.yield_batch_data(step_size, reward_size)
    for S, S_, R, trace_meta in gen:
        print(S.shape)
        


