import pandas as pd
import numpy as np
import mplfinance as mpf
import matplotlib.pyplot as plt

# 假设的股票数据
stock_data = pd.DataFrame({
    'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
    'open': [100, 102, 101, 104, 103, 105, 107, 106, 108, 110],
    'close': [102, 101, 104, 103, 105, 107, 106, 108, 110, 111],
    'high': [103, 103, 105, 106, 106, 108, 108, 110, 111, 112],
    'low': [99, 100, 100, 102, 102, 104, 105, 105, 107, 109]
})
stock_data.set_index('date', inplace=True)

# 自定义指标数据
indicator_data = pd.DataFrame({
    'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
    'J_index': [50, 55, 53, 58, 57, 60, 59, 62, 61, 65]
})
indicator_data.set_index('date', inplace=True)

# 操作记录数据
actions_data = pd.DataFrame({
    'date': ['2023-01-03', '2023-01-06', '2023-01-09'],
    'action': ['B', 'S', 'B'],
    'price': [102, 105.6, 105]
})
actions_data['date'] = pd.to_datetime(actions_data['date'])

# 画图
fig, ax = plt.subplots(figsize=(12, 8))

# 绘制股票价格的K线图
mpf.plot(stock_data, type='candle', ax=ax, style='yahoo', show_nontrading=True)

# 绘制自定义指标
ax2 = ax.twinx()
ax2.plot(indicator_data.index, indicator_data['J_index'], color='blue', label='J_index')
ax2.set_ylabel('J_index')

# 绘制买卖操作
for idx, row in actions_data.iterrows():
    color = 'green' if row['action'] == 'B' else 'red'
    ax.scatter(row['date'], row['price'], color=color, s=100, zorder=5, label=row['action'])

# 添加图例
handles, labels = ax.get_legend_handles_labels()
by_label = dict(zip(labels, handles))
ax.legend(by_label.values(), by_label.keys())

plt.title('Stock Price with Custom Indicator and Actions')
plt.savefig("./fig.png")
