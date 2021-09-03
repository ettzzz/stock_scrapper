# -*- coding: utf-8 -*-

import os
import traceback
from concurrent.futures import ThreadPoolExecutor

from rest_framework.views import APIView
from rest_framework.response import Response
from apscheduler.schedulers.background import BackgroundScheduler

from scraper import stockScraper, liveStockScraper
from database import stockDatabaseOperator
from config.static_vars import STOCK_HISTORY_PATH
from utils.datetime_tools import get_delta_date, get_today_date
# from utils.internet_tools import call_bot_dispatch

IS_FIRST_RUN = not os.path.exists(STOCK_HISTORY_PATH)

her_operator = stockDatabaseOperator(STOCK_HISTORY_PATH)
her_scraper = stockScraper()
her_live_scraper = liveStockScraper()


def first_run_check():
    if IS_FIRST_RUN:
        print('it\'s the first run on this instance, initiating basic tables...')
        today = get_today_date()
        her_operator._init_basic_tables()
        her_scraper._relogin()
        zz500, zz_fields = her_scraper.scrape_pool_data(update_date=today)
        all4000, all_fields = her_scraper.scrape_whole_pool_data(update_date=today)
        global_features = her_scraper.scrape_feature_list()
        her_operator._update_stock_list(zz500, global_features, all4000)
        print('initiating basic tables finished!, please call /api_v1/update')
    else:
        print('wow, hello again!')

first_run_check()
exe_boy = ThreadPoolExecutor(1)  # TODO: how this boy is played?
scheduler = BackgroundScheduler()
scheduler.start()


class codeNameMapping(APIView):
    def post(self, request):
        codes_str = request.data['codes']
        codes = codes_str.split(',')
        name_mapping = her_operator.get_cn_name(codes)
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
        # rebuild this block when needed, now it's deprecated.
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
        all_codes = her_operator.get_all_codes()  # 500 or 4000 will be decided by static_vars

        end_date = get_today_date()
        min_start_date = get_delta_date(min_start_date, 1)
        day_start_date = get_delta_date(day_start_date, 1)
        feature_start_date = get_delta_date(feature_start_date, 1)

        for idx, code in enumerate(all_codes):
            if idx % 100 == 0:
                print('update process {}/{}'.format(idx + 1, len(all_codes)))
            code = code[0]
            conn = her_operator.on()
            try:
                config_min = her_operator.generate_scrape_config(
                    code, min_start_date, end_date, 'min30')
                fetched, fields = her_scraper.scrape_k_data(config_min)
                if len(fetched) == 0:
                    her_operator.off(conn)
                    continue  # so when there is no min30, day data will not be updated either
                    # TODO: think again whether the sequence could be switched
                her_operator.insert_min30_data(code, fetched, fields, conn)

                config_day = her_operator.generate_scrape_config(
                    code, day_start_date, end_date, 'day')
                fetched, fields = her_scraper.scrape_k_data(config_day)
                her_operator.insert_day_data(code, fetched, fields, conn)
            except:
                print(code, '\n')
                traceback.print_exc()  # for now it's all about no data
                
            her_operator.off(conn)

        feature_codes = her_operator.get_feature_codes()
        stacked = her_scraper.scrape_feature_data(feature_codes, feature_start_date, end_date)
        her_operator.insert_feature_data(feature_codes, stacked)

        print('Update done!')

    def post(self, request):
        min_start_date = her_operator.get_latest_date(_type='min30')
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
            'feature start date from {}.'.format(
                min_start_date,
                day_start_date,
                feature_start_date
                )
        })
