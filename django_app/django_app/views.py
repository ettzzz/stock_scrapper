# -*- coding: utf-8 -*-

from concurrent.futures import ThreadPoolExecutor

from rest_framework.views import APIView
from rest_framework.response import Response
from apscheduler.schedulers.background import BackgroundScheduler

'''
well there are some articles about django_apscheduler, maybe I will
try them out maybe I will not
'''

from scraper import stockScraper
from database import stockDatabaseOperator
from config.static_vars import STOCK_HISTORY_PATH

her_operator = stockDatabaseOperator(STOCK_HISTORY_PATH)
her_scraper = stockScraper()

exe_boy = ThreadPoolExecutor(1)
scheduler = BackgroundScheduler()
scheduler.start()

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


class globalFeaturesUpdater(APIView):
    def global_update(self, start_date, end_date):
        '''
        TODO: add this fancy date recognizer

        from utils.datetime_tools import get_yesterday_date
        if not start_date or not end_date:
            start_date = last day + 1
            end_date = yesterday
        '''
        feature_codes = her_operator.get_feature_codes()
        stacked = her_scraper.scrape_feature_data(feature_codes, start_date, end_date)
        her_operator.insert_feature_data(feature_codes, stacked)

        all_codes = her_operator.get_all_codes()
        for idx, code in enumerate(all_codes):
            code = code[0]
            if idx % 100 == 0: # well this could be only useful in debugging
                print('scraping code {}/{}.'.format(idx + 1, len(all_codes)))
            try:
                config_min = her_operator.generate_scrape_config(
                    code, start_date, end_date, 'minute')
                fetched, fields = her_scraper.scrape_k_data(config_min)
                her_operator.insert_min30_data(code, fetched, fields)

                config_day = her_operator.generate_scrape_config(
                    code, start_date, end_date, 'day')
                fetched, fields = her_scraper.scrape_k_data(config_day)
                her_operator.insert_day_data(code, fetched, fields)
            except:
                print(code)
                continue
        print('Update done!')

    def post(self, request):
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        scheduler.add_job(
            func = self.global_update,
            kwargs = {'start_date': start_date, 'end_date': end_date},
            trigger = 'date', # will do it immidiately
        )
        return Response({
            'msg': 'Update started, start date from {}, end date util {}.'.format(start_date, end_date)
            })
