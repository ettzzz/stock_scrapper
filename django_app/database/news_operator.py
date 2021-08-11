#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 11 13:39:11 2021

@author: eee
"""

import os
from .base_operator import sqliteBaseOperator
from config.static_vars import DAY_ZERO

'''
1.按日期来，还是得存一下原语料数据的
2.如果有多个来源的话 应该放在同一个表里
3.来一个总表，分date，timestamp，weight，训练时候主要读取这个，
两个column，一个是code的顺序，一个是500个数


实时的情绪池：在redis里，5分钟新闻爬下来->先分析一波，更新到redis里
新闻存在今天的表里，交易时段内每隔30分钟总表就读取一个redis生成一条新纪录

'''


class newsDatabaseOperator(sqliteBaseOperator):
    def __init__(self, sql_dbfile_path):

        self.init_table_names = {
            'field': 'all_zz500_codes',
            'whole_field': 'all_codes',
            'feature': 'all_feature_codes',
            'global': 'all_feature_data'
        }
        self.stock_fields = {
            'field': {
                'updateDate': ['DATE', 'NOT NULL'],
                'code': ['TEXT', 'NOT NULL'],
                'code_name': ['TEXT']
            },
            'whole_field': {
                'updateDate': ['DATE', 'NOT NULL'],
                'code': ['TEXT', 'NOT NULL'],
                'code_name': ['TEXT'],
                'industry': ['TEXT'],
                'industryClassification': ['TEXT']
            },
            'feature': {
                'code': ['TEXT', 'NOT NULL'],
                'code_name': ['TEXT'],
                'code_fullname': ['TEXT'],
                'update_from': ['TEXT'],
                'orgnization': ['TEXT'],
                'description': ['TEXT']
            },
            'minute': {
                'date': ['DATE'],
                'time': ['TIME'],
                'volume': ['INTEGER'],
                'open': ['REAL'],
                'high': ['REAL'],
                'low': ['REAL'],
                'close': ['REAL']
            },
            'day': {
                'date': ['DATE'],
                'volume': ['INTEGER'],
                'isST': ['INTEGER'],
                'tradestatus': ['INTEGER'],
                # used an ugly patch in update function, or could use ['REAL', 'DEFAULT 0']
                'turn': ['REAL'],
                'pctChg': ['REAL'],
                'peTTM': ['REAL'],
                'psTTM': ['REAL'],
                'pcfNcfTTM': ['REAL'],
                'pbMRQ': ['REAL'],
                'open': ['REAL'],
                'high': ['REAL'],
                'low': ['REAL'],
                'close': ['REAL'],
                'preclose': ['REAL']
            },
        }

        if not os.path.exists(sql_dbfile_path):
            super().__init__(sql_dbfile_path)
            conn = self.on()
            for table in ['field', 'feature', 'whole_field']:
                conn.execute(
                    self.create_table_sql_command(
                        self.init_table_names[table],
                        self.stock_fields[table])
                )
            self.off(conn)
        else:
            super().__init__(sql_dbfile_path)

    def purge_tables_with_caution(self, table_names=[]):
        table_names = list(self.init_table_names.values()) if not table_names else table_names
        for t in table_names:
            self.delete_table(t)
