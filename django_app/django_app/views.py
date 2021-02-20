# -*- coding: utf-8 -*-

from rest_framework.views import APIView
from rest_framework.response import Response

from scraper import stockScraper
from database import stockDatabaseOperator
from config.static_vars import STOCK_HISTORY_PATH

his_operator = stockDatabaseOperator(STOCK_HISTORY_PATH)
his_scraper = stockScraper()


class codeFeaturesSender(APIView):
    def post(self, request):
        code = request.data['code']
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        
        features = his_operator.get_train_data(code, start_date, end_date)
        return Response(features)


class globalFeaturesUpdater(APIView):
    def post(self, request):
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        
        feature_codes = his_operator.get_feature_codes()
        stacked = his_scraper.scrape_feature_data(feature_codes, start_date, end_date)
        his_operator.insert_feature_data(feature_codes, stacked)
        
        all_codes = his_operator.get_all_codes()
        for idx, code in enumerate(all_codes):
            code = code[0]
            if idx % 100 == 0:
                print('scraping code {}/{}.'.format(idx + 1, len(all_codes)))
            try:
                config_min = his_operator.generate_scrape_config(
                    code, start_date, end_date, 'minute')
                fetched, fields = his_scraper.scrape_k_data(config_min)
                his_operator.insert_min30_data(code, fetched, fields)
                
                config_day = his_operator.generate_scrape_config(
                    code, start_date, end_date, 'day')
                fetched, fields = his_scraper.scrape_k_data(config_day)
                his_operator.insert_day_data(code, fetched, fields)
            except:
                print(code)
                continue
            
        return Response({
            'msg': 'Update Done'
            })
        # TODO: this func should be a async one


