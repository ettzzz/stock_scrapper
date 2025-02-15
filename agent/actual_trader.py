
import random

import pandas as pd

from database.stock_operator_v2 import StockDatabase

import mplfinance as mpf
import matplotlib.pyplot as plt

class ruleBasedTrader:
    def __init__(self):
        self.db = StockDatabase()
        self.all_d_codes = self.db.conn.execute("SELECT DISTINCT(code) FROM d_record;").fetchall()
        self.all_w_codes = self.db.conn.execute("SELECT DISTINCT(code) FROM w_record;").fetchall()
        self.all_m_codes = self.db.conn.execute("SELECT DISTINCT(code) FROM m_record;").fetchall()

    def _random_pick_code(self):
        code = random.choice(self.all_d_codes)
        if code in self.all_m_codes and code in self.all_w_codes:
            return code[0]
        else:
            return self._random_pick_code()

    def append_kdj(self, df):
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


    def output_action_sequence(self, code, start_date, end_date):
        temporal = "d" ### TODO: just daily index only
        df = self.db.fetch_record(temporal, code, start_date, end_date)
        df = self.append_kdj(df)

        dates, actions, prices = [], [], []

        for idx, row in df.iterrows():
            if row["J"] < 5:
                gap_days = 0
                for i in range(5):
                    if idx - i >= 0 and df.iloc[idx-i]["pctChg"] < -2:
                        gap_days += 1
                print("gap_day", gap_days)
                dates.append(df.iloc[idx+gap_days]["date"])
                actions.append("B")
                prices.append(df.iloc[idx+gap_days]["close"])

        res = {
            "date": dates,
            "action": actions,
            "price": prices
        }
        return df, pd.DataFrame(res)

    def _plot(self, df, seq):
        stock_data = df[["date", "open", "close", "high", "low"]].copy()
        stock_data['date'] = pd.to_datetime(stock_data['date'])
        stock_data.set_index('date', inplace=True)
        indicator_data = df[["date", "J"]].copy()
        indicator_data['date'] = pd.to_datetime(indicator_data['date'])
        indicator_data.set_index('date', inplace=True)
        actions_data = seq
        actions_data['date'] = pd.to_datetime(actions_data['date'])
        # 画图
        fig, ax = plt.subplots(figsize=(24, 8))

        # 绘制股票价格的K线图
        mpf.plot(stock_data, type='candle', ax=ax, style='yahoo', show_nontrading=True)

        # 绘制自定义指标
        ax2 = ax.twinx()
        ax2.plot(indicator_data.index, indicator_data['J'], color='cyan', label='J')
        ax2.set_ylabel('J')

        # 绘制买卖操作
        for idx, row in actions_data.iterrows():
            color = 'blue' if row['action'] == 'B' else 'yellow'
            ax.scatter(row['date'], row['price'], color=color, s=100, zorder=5, label=row['action'])

        # 添加图例
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys())

        plt.title('Stock Price with Custom Indicator and Actions')
        plt.tight_layout()
        plt.savefig("./fig.png")



if __name__ == "__main__":
    t = ruleBasedTrader()
    code = t._random_pick_code()
    start_date = '2020-05-15'
    end_date = '2024-10-31'
    print(code)
    df, seq = t.output_action_sequence(code, start_date, end_date)
    print(seq)
    t._plot(df, seq)
    print("hi")