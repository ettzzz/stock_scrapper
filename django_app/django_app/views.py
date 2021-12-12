# -*- coding: utf-8 -*-

import os
import traceback

# from concurrent.futures import ThreadPoolExecutor

from rest_framework.views import APIView
from rest_framework.response import Response

from scraper import stockScraper, liveStockScraper
from database import stockDatabaseOperator
from config.static_vars import STOCK_HISTORY_PATH, DEBUG
from utils.datetime_tools import get_delta_date, get_today_date
from schedule_maker.bg_schedule import scheduler
from gibber import gabber

IS_FIRST_RUN = not os.path.exists(STOCK_HISTORY_PATH)
her_operator = stockDatabaseOperator(STOCK_HISTORY_PATH)
her_scraper = stockScraper()
her_live_scraper = liveStockScraper()


def first_run_check():
    today = get_today_date()
    if DEBUG:
        gabber.info("It's on DEBUG mode!")

    if IS_FIRST_RUN:
        gabber.info("it's the first run on this instance, initiating basic tables...")
        her_operator._init_basic_tables()

        her_scraper._relogin()
        all4000, all_fields = her_scraper.scrape_whole_pool_data(update_date=today)
        her_operator.update_stock_list(all4000)

        global_features = her_scraper.scrape_feature_list()
        her_operator.update_feature_list(global_features)

        gabber.info("initiating basic tables finished!, please call /api_v1/update")
    else:
        gabber.info("wow, hello again on {}".format(today))


first_run_check()


class codeNameMapping(APIView):
    def post(self, request):
        codes_str = request.data["codes"]
        codes = codes_str.split(",")
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
        code = request.data["code"]
        start_date = request.data["start_date"]
        end_date = request.data["end_date"]

        features = her_operator.get_train_data(code, start_date, end_date)
        return Response(features)


class codeLiveFeaturesSender(APIView):
    def post(self, request):
        code_str = request.data["code_str"]
        date_str = request.data["date_str"]
        codes = code_str.split(",")
        dates = date_str.split(",")

        partial_features = her_operator.get_partial_live_data(codes, dates)
        """
        # rebuild this block when needed, now it's deprecated.
        for code in codes:
            if code.startswith('sh'):
                live_data = her_live_scraper.sh_live_k_data(code)
            else:
                live_data = her_live_scraper.sz_live_k_data(code)
        """
        features = partial_features
        return Response(features)


class globalFeaturesUpdater(APIView):
    def global_update(self):
        her_scraper._relogin()
        end_date = get_delta_date(get_today_date(), -1)
        # it should be yesterday, coz this func is scheduled on next day 0400

        all4000, all_fields = her_scraper.scrape_whole_pool_data(update_date=end_date)
        if all4000[0][0] == end_date:  # only update when all4000 is updated
            her_operator.update_stock_list(all4000)
        all_codes = her_operator.get_all_codes()

        time_ticket = dict()
        for idx, code in enumerate(all_codes):
            gabber.debug(f"building time_ticket {idx}/{len(all_codes)}")
            code = code[0]
            time_ticket[code] = {
                "min30": {
                    "start_date": get_delta_date(
                        her_operator.get_latest_date(_type="min30", code=code), 1
                    ),  # latest_date + 1 incase of duplication
                    "end_date": end_date,
                },
                "day": {
                    "start_date": get_delta_date(
                        her_operator.get_latest_date(_type="day", code=code), 1
                    ),
                    "end_date": end_date,
                },
            }

        conn = her_operator.on()
        commit_count = 0
        empty = list()
        for code, meta in time_ticket.items():
            try:
                config_min = her_operator.generate_scrape_config(
                    code,
                    meta["min30"]["start_date"],
                    meta["min30"]["end_date"],
                    "min30",
                )
                fetched, fields = her_scraper.scrape_k_data(config_min)
                if len(fetched) == 0:
                    gabber.debug(f"{code} is empty on {end_date}!")
                    empty.append(code)
                    continue  # so when there is no min30, day data will not be updated neither
                her_operator.insert_min30_data(code, fetched, fields, conn)

                config_day = her_operator.generate_scrape_config(
                    code,
                    meta["day"]["start_date"],
                    meta["day"]["end_date"],
                    "day",
                )
                fetched, fields = her_scraper.scrape_k_data(config_day)
                her_operator.insert_day_data(code, fetched, fields, conn)
                commit_count += 2

                if commit_count > 200:
                    her_operator.off(conn)
                    conn = her_operator.on()
                    commit_count = 0
                    gabber.debug("commit hohey!")

            except:
                gabber.error(
                    "update error of {}: {}".format(code, traceback.format_exc())
                )

        if commit_count > 0:
            her_operator.off(conn)
            gabber.debug("final commit hohey!")

        feature_codes = her_operator.get_feature_codes()
        feature_start_date = get_delta_date(
            her_operator.get_latest_date(_type="whatever"), 1
        )
        stacked = her_scraper.scrape_feature_data(
            feature_codes, feature_start_date, end_date
        )  # now it's [date, code, feature]
        her_operator.insert_feature_data(feature_codes, stacked)

        gabber.info("Update done for {}, empty length {}".format(end_date, len(empty)))

    def post(self, request):
        tomorrow = get_delta_date(get_today_date(), 1)
        _run_date = "{} 04:01:00".format(tomorrow)
        scheduler.add_job(
            func=self.global_update,
            trigger="date",
            run_date=_run_date,
        )
        return Response(
            {"msg": "Update will be started tomorrow ({}).".format(_run_date)}
        )
