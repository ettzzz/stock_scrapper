# -*- coding: utf-8 -*-

import traceback
from concurrent.futures import ThreadPoolExecutor

from rest_framework.views import APIView
from rest_framework.response import Response
from apscheduler.schedulers.background import BackgroundScheduler

# well there are some articles about django_apscheduler, maybe I will
# try them out maybe I will not

from scraper import stockScraper, liveStockScraper
from database import stockDatabaseOperator
from config.static_vars import STOCK_HISTORY_PATH, DAY_ZERO
from utils.datetime_tools import get_delta_date, get_today_date
from utils.internet_tools import call_bot_dispatch

her_operator = stockDatabaseOperator(STOCK_HISTORY_PATH)
her_scraper = stockScraper()
her_live_scraper = liveStockScraper()

if len(her_operator.get_feature_codes()) == 0:
    print('there is no db file in the project. creating new one..')
    her_scraper._relogin()
    today = get_today_date()
    zz500, zz_fields = her_scraper.scrape_pool_data(update_date=today)
    whole, whole_fields = her_scraper.scrape_whole_pool_data(update_date=today)
    global_features = her_scraper.scrape_feature_list()
    her_operator._update_stock_list(zz500, global_features, whole)
    print('creating finished!, please call /api_v1/update')

exe_boy = ThreadPoolExecutor(1)  # TODO: how this boy is played?
scheduler = BackgroundScheduler()
scheduler.start()


'''

爬取all_codes时候记得把没数据的踢出去，就57个的那个

1.得建立一个对code的dispatch，sqlite按10年数据算，一个表数据不超过500万条
min30一共1.25亿条
min5一共7.53亿条
day一共1600万条
2. min30按sh/sz先分两半， 然后按前4位数分，预期100个code组成一个表
每个表最多预计300万条，83个表， 100个表最多了
3.day先按sh/sz分两半，然后按前3位数分，一共12个表
每个表最多预计200万条数据
最多xxx条数据似乎并没有极限。。哎

4.wrapper baostock重新请求的
5.wrapper sqlite on&off
6.基本上就是把tablename做个mapping，query的时候得加code='xxx'

'''


class codeNameMapping(APIView):
    def post(self, request):
        codes_str = request.data['codes']
        name_mapping = her_operator.get_cn_name(codes_str)
        return Response(name_mapping)


class allTrainingCodesSender(APIView):
    def get(self, request):
        all_codes = her_operator.get_all_codes(is_train=True)
        return Response(all_codes)


class allCodesSender(APIView):
    def get(self, request):
        all_codes = her_operator.get_all_codes(is_train=False)
        return Response(all_codes)


class codeFeaturesSender(APIView):
    def post(self, request):
        code = request.data['code']
        start_date = request.data['start_date']
        end_date = request.data['end_date']

        features = her_operator.get_train_data(code, start_date, end_date)
        return Response(features)


class codeLiveFeaturesSender(APIView):
    def post(self, request):
        code_str = request.data['code_str']
        date_str = request.data['date_str']
        codes = code_str.split(',')
        dates = date_str.split(',')

        partial_features = her_operator.get_partial_live_data(codes, dates)
        '''
        # rebuild this block when needed
        for code in codes:
            if code.startswith('sh'):
                live_data = her_live_scraper.sh_live_k_data(code)
            else:
                live_data = her_live_scraper.sz_live_k_data(code)
        '''
        features = partial_features
        return Response(features)


class globalFeaturesUpdater(APIView):
    def global_update(self, min_start_date, day_start_date, feature_start_date):
        her_scraper._relogin()
        end_date = get_today_date()
        all_codes = her_operator.get_all_codes(is_train=True)
        min_start_date = get_delta_date(min_start_date, 1)
        day_start_date = get_delta_date(day_start_date, 1)
        feature_start_date = get_delta_date(feature_start_date, 1)

        for idx, code in enumerate(all_codes):
            code = code[0]
            try:
                config_min = her_operator.generate_scrape_config(
                    code, min_start_date, end_date, 'minute')
                fetched, fields = her_scraper.scrape_k_data(config_min)
                if len(fetched) == 0:
                    continue
                her_operator.insert_min30_data(code, fetched, fields)

                config_day = her_operator.generate_scrape_config(
                    code, day_start_date, end_date, 'day')
                fetched, fields = her_scraper.scrape_k_data(config_day)
                her_operator.insert_day_data(code, fetched, fields)
            except:
                print(code, '\n')
                traceback.print_exc()  # for now it's all about no data

        feature_codes = her_operator.get_feature_codes()
        stacked = her_scraper.scrape_feature_data(feature_codes, feature_start_date, end_date)
        her_operator.insert_feature_data(feature_codes, stacked)

        print('Update done!')

    def post(self, request):
        min_start_date = her_operator.get_latest_date(_type='min')
        day_start_date = her_operator.get_latest_date(_type='day')
        feature_start_date = her_operator.get_latest_date(_type='whatever')

        tomorrow = get_delta_date(get_today_date(), 1)
        scheduler.add_job(
            func=self.global_update,
            kwargs={
                'min_start_date': min_start_date,
                'day_start_date': day_start_date,
                'feature_start_date': feature_start_date
            },
            trigger='date',
            run_date='{} 04:01:00'.format(tomorrow)
        )
        return Response({
            'msg': 'Update started, min start date from {},'
            'day start date from {},'
            'feature start date from {}.'.format(min_start_date,
                                                 day_start_date, feature_start_date)
        })
