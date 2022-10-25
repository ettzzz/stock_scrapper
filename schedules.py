#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:38:45 2022

@author: eee
"""

from database.stock_operator import stockDatabaseOperator
from scrapper.baostock_scrapper import stockScrapper
from utils.datetime_tools import get_today_date, get_delta_date
from utils.gibber import get_logger


def call_for_update(start_date=None):
    dtype = "day"
    her_scrapper = stockScrapper()
    her_operator = stockDatabaseOperator()
    today = get_today_date()
    yesterday = get_delta_date(today, -1)
    end_date = yesterday
    gabber = get_logger()
    conn = her_operator.on()

    if start_date is None:  ## general update
        if not her_scrapper.if_date_open(yesterday):
            return  ## if not a trading day, return
        table_name = her_operator.init_table_names["all_feature_data"]
        start_date = her_operator.get_latest_date(
            table_name, date_key="updateDate", conn=conn
        )

    table_name = her_operator.init_table_names["all_codes"]
    fetched, _ = her_scrapper.scrape_whole_pool_data(update_date=yesterday)
    new_update_date = fetched[0]["updateDate"]
    last_update_date = her_operator.get_latest_date(
        table_name, date_key="updateDate", conn=conn
    )
    if new_update_date != last_update_date:
        her_operator.replace_data(table_name, fetched, conn)
        ## this operation is quite slow, avoid doing it everyday.
    else:
        del fetched

    all_codes = her_operator.get_all_codes(conn)
    for idx, code in enumerate(all_codes):
        table_name = her_operator.table_dispatch(code, dtype)
        start_date = her_operator.get_latest_date(
            table_name, match={"code": code}, conn=conn
        )
        config = her_operator.build_scrape_config(
            code,
            start_date,
            end_date,
            dtype,
        )
        fetched, fields = her_scrapper.scrape_k_data(config)
        her_operator.insert_data(table_name, fetched, conn)
        gabber.info(f"scrapping code {code}")

    ## replace feature_codes
    table_name = her_operator.init_table_names["all_feature_codes"]
    latest_feature_codes = her_scrapper.scrape_feature_list()
    _fields = list(her_operator.stock_fields["all_feature_codes"].keys())
    fetched = [dict(zip(_fields, fc)) for fc in latest_feature_codes]
    # good for you, there are all strs, so no need type_convertor
    her_operator.replace_data(table_name, fetched, conn)

    ## update feature_data
    feature_codes = her_operator.get_all_feature_codes(conn)
    for idx, fcode in enumerate(feature_codes):
        table_name = her_operator.init_table_names["all_feature_data"]
        start_date = her_operator.get_latest_date(
            table_name, match={"code": fcode}, conn=conn
        )
        config = her_operator.build_scrape_config(
            fcode,
            start_date,
            end_date,
            dtype,
        )
        config.update({"fields": "code,date,pctChg,tradestatus,turn"})
        fetched, fields = her_scrapper.scrape_k_data(config)
        if len(fetched) > 0 and fetched[0]["date"] == start_date:
            continue  ## in case of no data updated.
        her_operator.insert_data(table_name, fetched, conn)
        gabber.info(f"scrapping feature code {fcode}")

    her_operator.off()
    return
