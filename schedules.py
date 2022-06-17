#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:38:45 2022

@author: eee
"""

import traceback

from apscheduler.schedulers.background import BackgroundScheduler

from utils.gibber import gabber
from utils.datetime_tools import get_today_date, get_delta_date

her_operator = None
her_scraper = None

"""
# scrape and update

ask if it's opened yesterday
get end_date, start_date out of feature data 
get all_codes
update day data
replace feature_data
that's enough

"""


def ask_if_open(date):
    open_days = her_scraper.get_open_days(date, date)
    return len(open_days) > 0


def call_for_update():
    today = get_today_date()
    yesterday = get_delta_date(today, -1)

    ## ask if opened yesterday, if not, return
    if not ask_if_open(yesterday):
        return

    ## get end_date, start_date out of feature data
    end_date = yesterday
    end_date = "2022-06-15"  # wed
    start_date = "2022-06-10"  # friday

    dtype = "day"
    conn = her_operator.on()
    ## update day data
    # all_codes = her_operator.get_all_codes()
    all_codes = ["sh.600006"]
    for idx, code in enumerate(all_codes):
        table_name = her_operator.table_dispatch(code, dtype)
        start_date = her_operator.get_latest_date(table_name, match={"code": code})
        config = her_operator.build_scrape_config(
            code,
            start_date,
            end_date,
            dtype,
        )
        fetched, fields = her_scraper.scrape_k_data(config)
        her_operator.insert_data(table_name, fetched, conn)

    ## update feature_data
    # feature_codes = her_operator.get_feature_codes()
    feature_codes = ["sh.000001"]
    for idx, fcode in enumerate(feature_codes):
        table_name = her_operator.init_table_names["all_feature_data"]
        start_date = her_operator.get_latest_date(table_name, match={"code": fcode})
        config = her_operator.build_scrape_config(
            fcode,
            start_date,
            end_date,
            dtype,
        )
        config.update({"fields": "code,date,pctChg,tradestatus,turn"})
        fetched, fields = her_scraper.scrape_k_data(config)
        her_operator.insert_data(table_name, fetched, conn)

    ## replace feature_codes
    table_name = her_operator.init_table_names["all_feature_codes"]
    latest_feature_codes = her_scraper.scrape_feature_list()
    _fields = list(her_operator.stock_fields["all_feature_codes"].keys())
    fetched = [dict(zip(_fields, fc)) for fc in latest_feature_codes]
    # good for you, there are all strs, so no need type_convertor
    her_operator.replace_data(table_name, fetched, conn)

    ## replace all_codes
    table_name = her_operator.init_table_names["all_codes"]
    fetched, _ = her_scraper.scrape_whole_pool_data(update_date=end_date)
    new_update_date = fetched[0]["updateDate"]
    last_update_date = her_operator.get_latest_date(table_name, date_key="updateDate")
    if new_update_date != last_update_date:
        ## this operation is quite slow
        her_operator.replace_data(table_name, fetched, conn)

    her_operator.off()
    return


scheduler = BackgroundScheduler()

scheduler.add_job(func=call_for_update, trigger="cron", day_of_week="tue-sat", hour=4)

# scheduler.start()
