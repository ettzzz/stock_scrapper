# -*- coding: utf-8 -*-

import os
import time
import random

from scraper._sina import sinaScrapper
from scraper._yuncaijing import yuncaijingScrapper
from scraper._tonghuashun import tonghuashunScrapper
from scraper._eastmoney import eastmoneyScrapper

from utils.datetime_tools import date_range
from config.static_vars import ROOT


class newsScraper():
    def __init__(self):
        pass


ss = sinaScrapper()
ys = yuncaijingScrapper()

sql_file_path = './trade_news.db'
conn = sqlite3.connect(sql_file_path)


years = ['2017', '2018', '2019', '2020']

for t in years:
    conn.execute("DROP TABLE news_{};".format(t))
    conn.commit()


for t in years:
    conn.execute("CREATE TABLE news_{} (\
                        uid INTEGER PRIMARY KEY AUTOINCREMENT,\
                        fid TEXT NOT NULL,\
                        source TEXT,\
                        content TEXT NOT NULL,\
                        timestamp TEXT,\
                        tag TEXT,\
                        code TEXT,\
                        industry TEXT,\
                        info TEXT,\
                        comment TEXT);".format(t))
    conn.commit()


init_params = ss.get_params(_type=0)
sina_max_id = ss.get_news(init_params)['max_id']
sina_key = True
while sina_key:

    batch = {year: [] for year in years}
    sina_params = ss.get_params(_id=sina_max_id)
    sina_news = ss.get_news(sina_params, standard=True)

    if not sina_news:
        sina_max_id = sina_params['id'] - 1
        time.sleep(random.uniform(2, 4))
        continue

    for i in sina_news['list']:
        target_table = str(time.localtime(int(i['timestamp'])).tm_year)  # 2016
        if target_table not in years:
            sina_key = False
            break
        batch[target_table].append(tuple(i.values()))

    struct_time = time.localtime(int(i['timestamp']))
    time_str = time.strftime('%Y-%m-%d', struct_time)
    print('sina', time_str)
    for b in batch:
        if batch[b]:
            conn.executemany("INSERT INTO news_{} ({}) VALUES ({});".format(
                b,
                ','.join(list(i.keys())),
                ','.join(['?']*len(i))),
                batch[b])
    conn.commit()

    sina_max_id = sina_news['min_id'] - 1
    time.sleep(random.uniform(2, 4))


dates = date_range('2017-01-01', '2019-10-26')[::-1]

for date in dates:
    print('yuncaijing', date)
    year = date[:4]
    page = 1
    ycj_key = True
    append_news = []
    skip = 0
    while ycj_key:
        ycj_params = ys.get_params(page, date)
        ycj_news = ys.get_news(ycj_params, False)
        time.sleep(random.uniform(2, 4))

        if not ycj_news:
            print('yuncaijing skipped one page')
            page += 1
            skip += 1
            continue

        if skip >= 100:
            break

        for i in ycj_news:
            struct_time = time.localtime(int(i['timestamp']))
            time_str = time.strftime('%Y-%m-%d', struct_time)
            if time_str != date:
                ycj_key = False
                break
            append_news.append(tuple(i.values()))
        page += 1

    conn.executemany("INSERT INTO news_{} ({}) VALUES ({});".format(
        year,
        ','.join(list(i.keys())),
        ','.join(['?']*len(i))),
        append_news)
    conn.commit()
