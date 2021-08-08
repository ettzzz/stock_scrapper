# -*- coding: utf-8 -*-

import requests
import traceback

class yuncaijingScrapper():
    def __init__(self):
        self.base_url = 'https://www.yuncaijing.com/news/get_realtime_news/yapi/ajax.html'
        self.timestamp_format = int     
    
    def _get_code(self, news_dict):
        if not news_dict['thm_related'] and not news_dict['stktags']:
            return ''
        
        codes = []
        if len(news_dict['thm_related']) > 0:
            for thm in news_dict['thm_related']:
                if 'stock' in thm and type(thm['stock']) is list:
                    for thm_list in thm['stock']:
                        codes.append(thm_list['code'])
                        
        if len(news_dict['stktags']) > 0:
            for j in news_dict['stktags']:
                codes.append(j['code'])
        
        return ','.join(codes)
        
    
    
    def _data_cleaner(self, news_dict):
        fid = news_dict['id']
        content = news_dict['title'].strip() + ',' + news_dict['description'].strip()
        timestamp = news_dict['inputtime']
        tag = news_dict['thmtags'].replace(' ', ',')
        code = self._get_code(news_dict)
        
        return {'fid':fid, 
                'source':'yuncaijing',
                'content':content, 
                'timestamp':timestamp, 
                'tag':tag, 
                'code':code, 
                'industry':'', 
                'info':'', 
                'comment':''
                }
            
        
    def get_params(self, page, date):
        params = {
            'page': page,
            'date': date #2020-08-26
            }
        return params


    def get_news(self, params, standard = True):
        date = params['date']
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:79.0) Gecko/20100101 Firefox/79.0',
            'Referer': 'https://www.yuncaijing.com/insider/list_{}.html'.format(date),
            'Host': 'www.yuncaijing.com',
            'Origin': 'https://www.yuncaijing.com',
            'Cookie': 'ycj_wafsid=wafsid_fb0db6857bc227a8ca1aa0a1bbd8c7e5; ycj_uuid=96d6792f5fb5aa66a701bb929af84d63; ycj_from_url=aHR0cHM6Ly9kdWNrZHVja2dvLmNvbS8%3D; ycj_locate=aHR0cHM6Ly93d3cueXVuY2FpamluZy5jb20v; Qs_lvt_168612=1598163066; Qs_pv_168612=3418167207889237500; Hm_lvt_b68ec780c488edc31b70f5dadf4e94f8=1598163069; PHPSESSID=4r7bm8qmcc7nm7gf0vt4borpl4; YUNSESSID=nleq71t123s2p8m6hjmqmg5c01',
            
        }
        try:
            r = requests.post(
                url = self.base_url, 
                data = params, 
                headers = headers)
            if r.status_code == 200 and r.json()['error_code'] == '0':
                content = r.json()['data']
                if standard:
                    content = [self._data_cleaner(i) for i in content]
                return content
            else:
                print('from yuncaijingScrapper: Requesting failed! check url \n{}'.format(r.url))
                return []
        except:
            e = traceback.print_exc()
            print('from yuncaijingScrapper: Normalizing data error with following exception:\
                  \n {}\
                  \n'.format(e))
            return []





