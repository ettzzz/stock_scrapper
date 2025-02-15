import datetime

import pandas as pd

from scrapper.baostock_scrapper_v2 import stockScrapper
from database.stock_operator_v2 import StockDatabase
from utils import gabber


class IndicatorCalculator:
    def __init__(self, df):
        self.df = df
        self.backtrace_steps = 26 ## max steps is 26 in MACD

    def calculate_kdj(self):
        low_list = self.df["low"].rolling(window=9, min_periods=1).min()
        high_list = self.df["high"].rolling(window=9, min_periods=1).max()
        rsv = (self.df["close"] - low_list) / (high_list - low_list) * 100
        self.df["K"] = rsv.ewm(com=2).mean()
        self.df["D"] = self.df["K"].ewm(com=2).mean()
        self.df["J"] = 3 * self.df["K"] - 2 * self.df["D"]
        return self.df

    def calculate_ma(self, windows=[5, 20]):
        for window in windows:
            self.df[f"MA{window}"] = self.df["close"].rolling(window=window, min_periods=1).mean()
        return self.df
    
    def calculate_vma(self, windows=[5, 20]):
        for window in windows:
            self.df[f"V_MA{window}"] = self.df["volume"].rolling(window=window, min_periods=1).mean()
        return self.df
    
    def calculate_is_close_up(self):
        self.df["is_close_up"] = (self.df["open"] > self.df["close"]).astype(int)

    def calculate_cross(self):
        self.df["is_gold_cross"] = 0
        self.df["is_dead_cross"] = 0
        kdj_position = self.df["K"] > self.df["D"]
        self.df.loc[kdj_position[(kdj_position == True) & (kdj_position.shift() == False)].index, "is_gold_cross"] = 1
        self.df.loc[kdj_position[(kdj_position == False) & (kdj_position.shift() == True)].index, "is_dead_cross"] = 1
        return self.df

    def _calculate_macd(self, short_window=12, long_window=26, signal_window=9):
        self.df["EMA_short"] = self.df["close"].ewm(span=short_window, adjust=False).mean()
        self.df["EMA_long"] = self.df["close"].ewm(span=long_window, adjust=False).mean()
        self.df["MACD"] = self.df["EMA_short"] - self.df["EMA_long"]
        self.df["MACD_signal"] = self.df["MACD"].ewm(span=signal_window, adjust=False).mean()
        self.df["MACD_hist"] = self.df["MACD"] - self.df["MACD_signal"]
        return self.df
    
    def calculate_macd(self, short_window=12, long_window=26, signal_window=9):
        self.df["EMA_short"] = self.df["close"].ewm(span=short_window, adjust=False, min_periods=0).mean()
        self.df["EMA_long"] = self.df["close"].ewm(span=long_window, adjust=False, min_periods=0).mean()
        self.df["DIF"] = self.df["EMA_short"] - self.df["EMA_long"]
        self.df["DEA"] = self.df["DIF"].ewm(span=signal_window, adjust=False, min_periods=0).mean()
        self.df["MACD"] = 2 * (self.df["DIF"] - self.df["DEA"])
        return self.df

    def calculate_ema(self, span=12):
        self.df[f"EMA_{span}"] = self.df["close"].ewm(span=span, adjust=False, min_periods=0).mean()
        return self.df

    def calculate_vwap(self):
        self.df["Cumulative_Volume"] = self.df["volume"].cumsum()
        self.df["Cumulative_Volume_Price"] = (self.df["volume"] * (self.df["high"] + self.df["low"] + self.df["close"]) / 3).cumsum()
        self.df["VWAP"] = self.df["Cumulative_Volume_Price"] / self.df["Cumulative_Volume"]
        return self.df


def get_indicators(df, df_ind_tail=None):
    df["volume"] = df["volume"].replace("", 0)
    if "tradestatus" in df.columns:
        df = df[(df["tradestatus"] == "1") & (df["volume"] > 0)]
    else:
        df = df[df["volume"] > 0]
        
    columns = ["low", "high", "close"]
    df = df.dropna(subset=columns)
    for key in columns:
        key_data = df[key].astype(float)
        del df[key]
        df.insert(0, key, key_data)
    
    if df_ind_tail is not None:
        df = pd.concat([df_ind_tail, df], ignore_index=True)
    
    indicators = IndicatorCalculator(df)
    indicators.calculate_is_close_up()
    indicators.calculate_kdj()
    indicators.calculate_ma()
    indicators.calculate_vma()
    indicators.calculate_cross()
    indicators.calculate_macd()
    indicators.calculate_ema(span=9)
    indicators.calculate_ema(span=20)
    indicators.calculate_vwap()
    
    return indicators.df


def main(max_backsteps=26):
    scrapper = stockScrapper()
    db = StockDatabase(db_name='stock_data.db')
    db_ind = StockDatabase(db_name='stock_indicator.db')
    target_cols = ['date', 'code', 'close', 'volume', 'amount', 'turn', 'pctChg', 'is_close_up', 'K', 'D', 'J', 'MA5', 'MA20', 'is_gold_cross', 'is_dead_cross', 'EMA_short', 'EMA_long', 'MACD', "DIF", "DEA", 'EMA_9', 'EMA_20', 'VWAP']

    all_codes_df = scrapper.get_all_codes_df()
    all_codes_df = all_codes_df[
        (all_codes_df["type"] == "1")
    ].reset_index(drop=True)

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    temporals = ["d", "w", "m"]

    for frequency in temporals:
        table_name = f"{frequency}_record"
        table_exist = db_ind.if_table_exist(table_name)
        for idx, row in all_codes_df.iterrows():
            code = row["code"]
            try:
                ind_latest_date = db_ind.get_latest_date(frequency, code)
                if ind_latest_date ==  '2025-02-07 00:00:00':
                    continue
                
                if row["outDate"] != "":
                    continue
                end_date = today
                
                
                if ind_latest_date is None: # not exist
                    df = db.fetch_record(table_name=table_name, code=code, start_date=row["ipoDate"], end_date=end_date)
                    df_ind = get_indicators(df)
                    start_date = row["ipoDate"]
                else: # already exist, need update
                    df = db.fetch_record(table_name=table_name, code=code, start_date=row["ipoDate"], end_date=end_date)
                    df_ind = get_indicators(df)
                    df_ind = df_ind[df_ind["date"] > ind_latest_date]
                    start_date = ind_latest_date.split(" ")[0]
                    
                df_ind = df_ind[target_cols]
                if not table_exist:
                    db_ind.create_tables(df_ind, table_name)
                db_ind.insert_record(df_ind, table_name)
                gabber.info(f"update_indicator for {code}-{frequency}, {idx+1}/{len(all_codes_df)}, start_date: {start_date}, end_date: {end_date}")
            except Exception as e:
                gabber.error(f"error {code}-{frequency} {e}")
                
        

if __name__ == "__main__":
    import time
    t = time.time()
    main()
    print(time.time() - t)