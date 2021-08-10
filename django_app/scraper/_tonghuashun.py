# -*- coding: utf-8 -*-

import requests

class tonghuashunScrapper():
    def __init__(self):
        self.base_url = 'https://news.10jqka.com.cn/tapp/news/push/stock'
        self.headers = {
            'Host': 'news.10jqka.com.cn',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:79.0) Gecko/20100101 Firefox/79.0',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://news.10jqka.com.cn/realtimenews.html'
            }
        self.timestamp_format = int
    
    
    def _data_cleaner(self, news_dict):
        fid = news_dict['id']
        content = news_dict['title'].strip() + ',' + news_dict['digest'].strip()
        timestamp = news_dict['ctime']
        tag = ','.join(
            [j['name'] for j in news_dict['tags']] + 
            [i['name'] for i in news_dict['tagInfo']])
        code = ''
        
        return {'fid':fid, 
                'source':'tonghuashun',
                'content':content, 
                'timestamp':timestamp, 
                'tag':tag, 
                'code':code, 
                'industry':'', 
                'info':'', 
                'comment':''
                }
    
    
    def get_params(self):
        params = {
            'pagesize': 200,
            'page': 1
            }
        return params
    
    
    def get_news(self, params, standard = True):
        r = requests.get(
            url = self.base_url, 
            params = params, 
            headers = self.headers
            )
        if r.status_code == 200 and r.json()['code'] == '200': # seems to be very silly
            content = r.json()['data']
            
            if standard:
                cleaned_list = [self._data_cleaner(i) for i in content['list']]
                # content['list'] = cleaned_list
                content = cleaned_list
            return content
        else:
            print('from tonghuashunScrapper: Requesting failed! check url \n{}'.format(r.url))
            return {}










