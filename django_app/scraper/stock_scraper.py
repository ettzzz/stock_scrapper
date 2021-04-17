# -*- coding: utf-8 -*-
import random
import traceback

from bs4 import BeautifulSoup
import requests
import baostock as bs

from utils.datetime_tools import get_now
from config.static_vars import UA


class stockScraper():
    def __init__(self):
        bs.login()


    def call_baostock(self, baostock_raw):
        data_list = []
        while (baostock_raw.error_code == '0') & baostock_raw.next():
            data_list.append(baostock_raw.get_row_data())
        if len(data_list) == 0:
            return [], []
        else:
            return data_list, baostock_raw.fields


    def scrape_k_data(self, config):
        '''
        config = {
            'code': 'sh.600000',
            'fields': self.stock_fields['minute'],
            'start_date': '2019-05-13',
            'end_date': '2019-05-31',
            'frequency': '30',
            'adjustflag': '1'
            }
        '''
        raw = bs.query_history_k_data_plus(**config)
        if raw.error_msg == '网络接收错误。':
            bs.login()
            raw = bs.query_history_k_data_plus(**config)
        data, fields = self.call_baostock(raw)
        return data, fields


    def scrape_pool_data(self, update_date = '2019-01-01'):
        config = {
            'date': update_date
            }
        raw = bs.query_zz500_stocks(**config)
        # bs.query_stock_industry()
        if raw.error_msg == '网络接收错误。':
            bs.login()
            raw = bs.query_zz500_stocks(**config)
        data, fields = self.call_baostock(raw)
        return data, fields


    def scrape_feature_list(self):
        '''
        Could be an absolutely fragile function, no exception handling at all.
        '''
        stock_list_url = 'http://baostock.com/baostock/index.php/公式与数据格式说明'

        r = requests.get(url=stock_list_url, headers={'User-Agent': UA})
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.select('table.wikitable')

        code_pool = []
        features = []
        for t in tables[:3]: # only first 4 tables are useful
            bullets = t.select('tr')
            for b in bullets[1:]: # first one row is table title
                content = [i.get_text().strip() for i in b.select('td')]
                if content[0] in code_pool:
                    continue
                else:
                    code_pool.append(content[0])
                    features.append(content) # kind of stupid way but it works

        return features

        # cfields = ['code', 'code_name', 'code_fullname', 'update_from', 'orgnization', 'description']
        # # TODO; this cfields could be a pit
        # fetched, fields = self.scrape_pool_data({'date': update_date})

        # return fetched, fields, features, cfields


    def scrape_feature_data(self, feature_codes, start_date, end_date):
        dates, _ = self.scrape_k_data({
            'code': 'sh.000001',
            'fields': 'date',
            'start_date': start_date,
            'end_date': end_date,
            'frequency': 'd',
            'adjustflag': '1'
            }) # get valid dates first

        store = {date[0]: [] for date in dates}
        for idx, code in enumerate(feature_codes):
            # if idx % 100 == 0:
            #     print('scraping feature {}/{}.'.format(idx + 1, len(feature_codes)))
            code = code[0]
            config = {
                'code': code,
                'fields': 'date,pctChg',
                'start_date': start_date,
                'end_date': end_date,
                'frequency': 'd',
                'adjustflag': '1'
            }
            fetched, _ = self.scrape_k_data(config)
            temp_dict = {k: v for (k, v) in fetched}
            for date in store:
                if date in temp_dict:
                    store[date].append(temp_dict[date])
                else:
                    store[date].append(0)

        stacks = []
        for date, indices in store.items():
            stacks.append([date] + indices)

        return stacks


class liveStockScraper():
    def sh_live_k_data(self, code, data_type = 'min'):
        '''
        http://www.sse.com.cn/market/price/trends/index.shtml?code=SH000004
        '''
        headers = {
            'User-Agent': UA,
            'Host': 'yunhq.sse.com.cn:32041',
            'Referer': 'http://www.sse.com.cn/'
            }
        
        if data_type == 'min':
            base_url = 'http://yunhq.sse.com.cn:32041//v1/sh1/line/{}'.format(code.split('.')[-1])
            params = {
                'begin': -40,
                'end': -1,
                'select': 'time,price,volume',
                '_': round(get_now()*1000)
                }
        else: # data_type == 'day':
            base_url = 'http://yunhq.sse.com.cn:32041//v1/sh1/dayk/{}'.format(code.split('.')[-1])
            params = {
                'begin': -3,
                'end': -1,
                'select': 'date,open,high,low,close,volume',
                '_': round(get_now()*1000)
                }
            
        r = requests.get(
            base_url,
            params = params,
            headers = headers
            )
        
        # date, _time, _open, high, low, _close = each_min
        if r.status_code == 200:
            response = r.json()
            highest = max([l[1] for l in response['line'][-30:]])
            lowest = min([l[1] for l in response['line'][-30:]])
            live_data = [
                response['date'],
                response['time'],
                response['line'][-30][1],
                highest, #response['highest'],
                lowest, #response['lowest'],
                response['line'][-1][1],
                ]
        else:
            live_data = []
        return live_data
    
    
    def sz_live_k_data(self, code):
        '''
        http://www.szse.cn/market/trend/index.html?code=399623
        '''
        headers = {
            'User-Agent': UA,
            'Host': 'www.szse.cn',
            'Referer': 'http://www.szse.cn/market/trend/index.html?code={}'.format(code.split('.')[-1])
            }
        base_url = 'http://www.szse.cn/api/market/ssjjhq/getTimeData'
        params = {
            'random': random.random(),
            'marketId': 1,
            'code': code.split('.')[-1]
            }
        r = requests.get(
            base_url,
            params = params,
            headers = headers
            )
        
        #response['data']['picupdata'][0] structure:
        #time, close, avg, pctChg, 涨跌幅？,share, volume
        # date, _time, _open, high, low, _close = each_min
        if r.status_code == 200:
            response = r.json()
            highest = max([float(l[1]) for l in response['data']['picupdata'][-30:]])
            lowest = min([float(l[1]) for l in response['data']['picupdata'][-30:]])
            live_data = [
                response['data']['marketTime'].split(' ')[0],
                response['data']['marketTime'],
                float(response['data']['picupdata'][-30][1]), #response['data']['open'],
                highest, #response['data']['high'],
                lowest, #response['data']['low'],
                float(response['data']['picupdata'][-1][1]),
                ]
        else:
            live_data = []
        
        return live_data


'''
TODOs:
    2. live k data class怎么安排成能用的feature？效率如何？
        改日期、时间 符合feature的样子, str-float
    3. 找一天的数据验证一下和baostock的计算结果是否一致
    4.把2017-2018的数据也搞进来

'''


