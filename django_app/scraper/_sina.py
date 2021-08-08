# -*- coding: utf-8 -*-

import requests
import re

from utils.datetime_tools import timestamper

class sinaScrapper():
    def __init__(self):
        self.base_url = 'http://zhibo.sina.com.cn/api/zhibo/feed'
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:79.0) Gecko/20100101 Firefox/79.0',
            'Referer': 'http://finance.sina.com.cn/7x24/',
            'Host': 'zhibo.sina.com.cn'
            }
        self.timestamp_format = '%Y-%m-%d %H:%M:%S'
        self.callback = 'jQuery0'


    def _data_cleaner(self, news_dict):
        fid = news_dict['id']
        content = news_dict['rich_text'].strip()
        timestamp = str(timestamper(news_dict['create_time'], self.timestamp_format))
        tag = ','.join(list(map(lambda x: x['name'], news_dict['tag'])))
        
        try:
            ext = news_dict['ext'].replace('true', 'True').replace('false', 'False')
            ext = eval(ext)
            code = ','.join(list(map(lambda x: x['symbol'], ext['stocks'])))
        except:
            code = ''
        
        return {'fid':fid, 
                'source':'sina',
                'content':content, 
                'timestamp':timestamp, 
                'tag':tag, 
                'code':code, 
                'industry':'', 
                'info':'', 
                'comment':''
                }
    
    
    def get_params(self, _id = 114514, _type = 1, page = 1, page_size = 100):
        #_type = 0: # get latest 100 news;
        #_type = 1: # get specific news with designated ids
        params = {
            'callback': self.callback,
            'page': page,
            'page_size': page_size,
            'zhibo_id': 152,
            'tag_id': 0,
            'dire': 'f',
            'dpc': 1,
            'pagesize': 20,
            'id': _id,
            'type': _type
            
        }
        '''
        tag id mapping
        {'1': '宏观', '2': '行业', '3': '公司', '4': '数据', '5': '市场',
         '6': '观点', '7': '央行', '8': '其他', '9': '焦点', '10': 'A股',
         '102': '国际', '110': '疫情',
         }
        '''
        return params
    
    
    def get_news(self, params, standard = True):
        '''
        params = {
            'callback': callback,
            'page': 1,
            'page_size': 100,
            'zhibo_id': 152,
            'tag_id': 0,
            'dire': 'f',
            'dpc': 1,
            'pagesize': 20,
            'id': 203152,
            'type': 1, # 1 historical, 0 latest
        }
        return a dictionary contains page_info, max_id, min_id and news list
        '''
        try:
            r = requests.get(
                url = self.base_url, 
                params = params, 
                headers = self.headers
                )
            if r.status_code == 200:
                text = re.findall(self.callback + r'\((.*)\);}catch', r.text)[0]
                content = eval(text)['result']['data']['feed']
                if standard:
                    cleaned_list = [self._data_cleaner(i) for i in content['list']]
                    content['list'] = cleaned_list
                return content
            else:
                print('from sinaScrapper: Requesting failed! check url \n{}'.format(r.url))
                return {}
        except Exception as e:
            print('from sinaScrapper: Normalizing data error with folling exception:\
                  \n {}\
                  \n'.format(e))
            return {}
        
        
