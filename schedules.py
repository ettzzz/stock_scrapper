#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:38:45 2022

@author: eee
"""

from database.stock_operator import stockDatabaseOperator
from scrapper.baostock_scrapper import stockScrapper
from utils.datetime_tools import get_today_date, get_delta_date

# from utils.gibber import gabber


def call_for_update():
    today = get_today_date()
    yesterday = get_delta_date(today, -1)
    her_scrapper = stockScrapper()
    ## ask if opened yesterday, if not, return
    if not her_scrapper.if_date_open(yesterday):
        return

    ## get end_date, start_date out of feature data
    dtype = "day"
    end_date = yesterday
    her_operator = stockDatabaseOperator()
    conn = her_operator.on()

    ## replace all_codes
    table_name = her_operator.init_table_names["all_codes"]
    fetched, _ = her_scrapper.scrape_whole_pool_data(update_date=end_date)
    new_update_date = fetched[0]["updateDate"]
    last_update_date = her_operator.get_latest_date(table_name, date_key="updateDate")
    if new_update_date != last_update_date:
        ## this operation is quite slow, avoid doing it everyday.
        her_operator.replace_data(table_name, fetched, conn)
    else:
        del fetched

    ## update day data
    all_codes = her_operator.get_all_codes()
    for idx, code in enumerate(all_codes):
        table_name = her_operator.table_dispatch(code, dtype)
        start_date = her_operator.get_latest_date(table_name, match={"code": code})
        config = her_operator.build_scrape_config(
            code,
            start_date,
            end_date,
            dtype,
        )
        fetched, fields = her_scrapper.scrape_k_data(config)
        her_operator.insert_data(table_name, fetched, conn)

    ## replace feature_codes
    table_name = her_operator.init_table_names["all_feature_codes"]
    latest_feature_codes = her_scrapper.scrape_feature_list()
    _fields = list(her_operator.stock_fields["all_feature_codes"].keys())
    fetched = [dict(zip(_fields, fc)) for fc in latest_feature_codes]
    # good for you, there are all strs, so no need type_convertor
    her_operator.replace_data(table_name, fetched, conn)

    ## update feature_data
    feature_codes = her_operator.get_all_feature_codes()
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
        fetched, fields = her_scrapper.scrape_k_data(config)
        if len(fetched) == 0:
            continue  ## actually this condition is redundant, her_operator.insert_data has secure
        if len(fetched) > 0 and fetched[-1]["date"] == start_date:
            continue  ## in case of no data updated.
        her_operator.insert_data(table_name, fetched, conn)

    her_operator.off()
    return
