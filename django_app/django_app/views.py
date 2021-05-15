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


her_operator = stockDatabaseOperator(STOCK_HISTORY_PATH)
her_scraper = stockScraper()
her_live_scraper = liveStockScraper()

if len(her_operator.get_feature_codes()) == 0:
    print('there is no db file in the project. creating new one..')
    zz500, _fields = her_scraper.scrape_pool_data(update_date=get_today_date())
    global_features = her_scraper.scrape_feature_list()
    her_operator._update_stock_list(zz500, global_features)
    print('creating finished!, please call /api_v1/update')

exe_boy = ThreadPoolExecutor(1) # TODO: how this boy is played?
scheduler = BackgroundScheduler()
scheduler.start()


class codeNameMapping(APIView):
    def post(self, request):
        codes_str = request.data['codes']
        codes = codes_str.split(',')
        name_mapping = her_operator.get_cn_name(codes)
        return Response(name_mapping)


class allCodesSender(APIView):
    def get(self, request):
        all_codes = her_operator.get_all_codes()
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
        code = request.data['code']
        if code.startswith('sh'):
            live_data = her_live_scraper.sh_live_k_data(code)
        elif code.startswith('sz'):
            live_data = her_live_scraper.sz_live_k_data(code)
        else:
            return Response({'msg': 'ohhhhh'})
        print(live_data)
        features = her_operator.get_live_data(code, live_data)
        return Response(features)


class globalFeaturesUpdater(APIView):
    def global_update(self, start_date, end_date):
        feature_codes = her_operator.get_feature_codes()
        print('scraping global features...')
        stacked = her_scraper.scrape_feature_data(feature_codes, start_date, end_date)
        her_operator.insert_feature_data(feature_codes, stacked)

        all_codes = her_operator.get_all_codes()
        print('scraping single code...')
        for idx, code in enumerate(all_codes):
            code = code[0]
            try:
                config_min = her_operator.generate_scrape_config(
                    code, start_date, end_date, 'minute')
                fetched, fields = her_scraper.scrape_k_data(config_min)
                if len(fields) == 0:
                    continue
                her_operator.insert_min30_data(code, fetched, fields)

                config_day = her_operator.generate_scrape_config(
                    code, start_date, end_date, 'day')
                fetched, fields = her_scraper.scrape_k_data(config_day)
                her_operator.insert_day_data(code, fetched, fields)
            except:
                print(code, '\n')
                traceback.print_exc() # for now it's all about no data

        print('Update done!')


    def post(self, request):
        start_date = request.data['start_date']
        end_date = request.data['end_date']

        latest_date = her_operator.get_latest_date()

        if not start_date and not end_date:
            start_date = get_delta_date(latest_date, 1)
            today = get_today_date()
            yesterday = get_delta_date(today, -1)
            end_date = yesterday
            if start_date > end_date:# this patch is for multiple request within one day
                return Response({
                    'msg': 'end_date {} already in database!'.format(end_date)
                    })
        else:
            if end_date <= DAY_ZERO:
                return Response({
                    'msg': 'end_date {} cannot be later than {}!'.format(end_date, DAY_ZERO)
                    })
            if end_date <= latest_date:
                return Response({
                    'msg': 'end_date {} already in database!'.format(end_date)
                    })
            if start_date <= latest_date:
                start_date = get_delta_date(latest_date, 1)

        scheduler.add_job(
            func = self.global_update,
            kwargs = {'start_date': start_date, 'end_date': end_date},
            trigger = 'date', # will do it immidiately
        )
        return Response({
            'msg': 'Update started, start date from {}, end date until {}.'.format(start_date, end_date)
            })
