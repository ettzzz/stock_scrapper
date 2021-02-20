# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import baostock as bs

        
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
        stock_list_url = 'http://baostock.com/baostock/index.php/%E5%85%AC%E5%BC%8F%E4%B8%8E%E6%95%B0%E6%8D%AE%E6%A0%BC%E5%BC%8F%E8%AF%B4%E6%98%8E'
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:85.0) Gecko/20100101 Firefox/85.0'
        
        r = requests.get(url=stock_list_url, headers={'User-Agent': ua})
        soup = BeautifulSoup(r.text, 'html.parser')
        tables = soup.select('table.wikitable')
        
        code_pool = []
        features = []
        for t in tables[:9]: # only first 10 tables are useful
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
            if idx % 100 == 0:
                print('scraping feature {}/{}.'.format(idx + 1, len(feature_codes)))
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
        
    

