# -*- coding: utf-8 -*-

import os
import traceback
import datetime

# from concurrent.futures import ThreadPoolExecutor
import numpy as np
from dtw import dtw

from rest_framework.views import APIView
from rest_framework.response import Response

from scraper import stockScraper, liveStockScraper
from database import stockDatabaseOperator
from config.static_vars import STOCK_HISTORY_PATH, DEBUG
from utils.datetime_tools import get_delta_date, get_today_date, get_now
from utils.internet_tools import get_train_news_weight
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


class allCodesSender(APIView):
    def get(self, request):
        all_codes = her_operator.get_all_codes()
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


class poolFeaturePicker(APIView):
    def post(self, request):
        start_date = request.data["start_date"]
        end_date = request.data["end_date"]

        features = ["turn", "pctChg"]
        # by default there should be 4000+ all codes, but actually tiny amount
        # of them are missing, won't hurt anyways
        # and the dates ought to be open_days
        all_tables = her_operator.get_all_tables()
        day_tables = [t[0] for t in all_tables if t[0].startswith("day")]

        res = dict()
        for day_table in day_tables:
            records = her_operator.fetch_by_command(
                "SELECT {} FROM '{}' \
                WHERE tradestatus='1' AND date BETWEEN '{}' AND '{}' \
                ORDER BY code, date;".format(
                    ",".join(["code", "date"] + features),
                    day_table,
                    start_date,
                    end_date,
                ),
            )

            for code, date, turn, pct in records:
                # TODO: cannot split them properly yet
                if code not in res:
                    res[code] = {k: list() for k in features + ["seq"]}
                    ratio = 1
                if pct == "":
                    continue
                ratio *= 1 + pct / 100
                res[code]["turn"].append(turn)
                res[code]["pctChg"].append(pct)
                res[code]["seq"].append(round(ratio, 3))

        return Response(res)


class newsAffiliatedCodeSender(APIView):
    def get(self):
        table_name = "affiliated_codes"
        data = her_operator.fetch_by_command(
            f"SELECT code, industry FROM {table_name};"
        )

        return Response(data)


class newsAffiliatedCodeUpdater(APIView):
    def global_update(self):
        today = get_today_date()
        end_date = get_delta_date(today, -2)
        start_date = get_delta_date(today, -92)

        ### price
        table_name = her_operator.init_table_names["whole_field"]
        all_codes = her_operator.fetch_by_command(
            f"SELECT code,industry FROM '{table_name}';"
        )
        price_dict = dict()
        conn = her_operator.on()
        for code, ind in all_codes:
            day_table = her_operator._table_dispatch(code, "day")
            cols = ",".join(her_operator.day_train_cols)
            day_data = her_operator.fetch_by_command(
                f"SELECT {cols} FROM '{day_table}' \
                WHERE code='{code}' AND date BETWEEN '{start_date}' AND '{end_date}';",
                conn=conn,
            )
            price_dict[code] = list()
            for d in day_data:
                close, preclose = d[-2:]
                price_dict[code].append(preclose)
                price_dict[code].append(close)
        her_operator.off(conn)

        ### news
        news_record = get_train_news_weight(start_date, end_date)
        news_dict = {code: [] for code, ind in all_codes}
        for i in news_record["results"]:
            _time = i["time"]
            if _time.startswith("10:0") or _time.startswith("15"):
                for code, ind in all_codes:
                    if code in i["weights_dict"]:
                        news_dict[code].append(i["weights_dict"][code])
                    else:
                        news_dict[code].append(0)

        ### pick
        pick = list()
        for code, ind in all_codes:
            if code not in news_dict or code not in price_dict:
                continue
            y1 = price_dict[code]
            y2 = news_dict[code]
            if sum(y2) == 0:
                continue
            if len(y1) == len(y2):
                norm_y1 = (y1 - np.min(y1)) / (np.max(y1) - np.min(y1))
                y2 = (y2 - np.min(y2)) / (np.max(y2) - np.min(y2))
                alignment = dtw(y2, norm_y1, keep_internals=True, distance_only=True)
                if alignment.normalizedDistance <= 0.075:
                    pick.append([code, ind, today])
        gabber.info(f"news affiliated code update done, {len(pick)} in total.")

        ### update
        table_name = "affiliated_codes"
        conn = her_operator.on()
        conn.execute(
            her_operator.create_table_sql_command(
                table_name, her_operator.stock_fields["news_feature"]
            )
        )
        her_operator.off(conn)
        her_operator.delete_table(table_name)
        conn = her_operator.on()
        fields = list(her_operator.stock_fields["news_feature"].keys())
        insert_sql = her_operator.insert_batch_sql_command(table_name, fields)
        conn.executemany(insert_sql, pick)
        her_operator.off(conn)

    def post(self, request):
        is_plan = request.data["is_plan"]
        if is_plan in [1, "1"]:
            _run_date = datetime.datetime.fromtimestamp(round(get_now() + 3))
        else:
            tomorrow = get_delta_date(get_today_date(), 2)
            _run_date = "{} 03:01:00".format(
                tomorrow
            )  # this should be called on Friday and will run on Sunday

        scheduler.add_job(
            func=self.global_update,
            trigger="date",
            run_date=_run_date,
        )
        return Response({"msg": "Update will be started ({}).".format(_run_date)})


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

    def testprint(self):
        print("hey")

    def post(self, request):
        is_plan = request.data["is_plan"]
        if is_plan in [1, "1"]:
            _run_date = datetime.datetime.fromtimestamp(round(get_now() + 3))
        else:
            tomorrow = get_delta_date(get_today_date(), 1)
            _run_date = "{} 04:01:00".format(tomorrow)

        scheduler.add_job(
            func=self.global_update,
            # func=self.testprint,
            trigger="date",
            run_date=_run_date,
        )
        return Response({"msg": "Update will be started ({}).".format(_run_date)})
