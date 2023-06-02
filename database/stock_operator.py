#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:40:14 2022

@author: eee
"""
import time

from database.base_mongo import BaseMongoOperator
from config.static_vars import DAY_ZERO, MONGO_URI, DB_NAME


class stockDatabaseOperator(BaseMongoOperator):
    def __init__(self, mongo_uri=MONGO_URI, db_name=DB_NAME, safety_first=True):
        super().__init__(mongo_uri, db_name)
        self.chunk_size = 400
        self.safety_first = safety_first
        self.init_table_names = {
            "all_codes": "all_codes",
            "all_feature_codes": "all_feature_codes",
            "all_feature_data": "all_feature_data",
        }
        self.stock_fields = {
            "all_feature_data": {
                "date": [],
                # empty list means no need to convert type
                "code": [],
                "pctChg": [float, 0.0],
                "turn": [int, 0],
                "tradestatus": [int, 0],
            },
            "all_codes": {
                "updateDate": [],
                "code": [],
                "code_name": [],
                "industry": [],
                "industryClassification": [],
                "is_zz500": [int, 0],
                "is_hs300": [int, 0],
                "is_sz50": [int, 0],
            },
            "all_feature_codes": {
                "code": [],
                "code_name": [],
                "code_fullname": [],
                "update_from": [],
                "orgnization": [],
                "description": [],
            },
            "minute": {
                "code": ["TEXT"],  # TO BE CREATED
                "date": ["DATE"],
                "time": ["TIME"],
                "volume": ["INTEGER"],
                "open": ["REAL"],
                "high": ["REAL"],
                "low": ["REAL"],
                "close": ["REAL"],
            },
            "day": {
                "code": [],
                "date": [],
                "volume": [int, 0],
                "isST": [int, 0],
                "tradestatus": [int, 0],
                "turn": [float, 0.0],
                # 'DEFAULT 0', 'NOT NULL' not working as '' is not NULL
                "pctChg": [float, 0.0],
                "peTTM": [float, 0.0],  # 滚动市盈率
                "psTTM": [float, 0.0],  # 滚动市销率 # will be deprecated soon
                "pcfNcfTTM": [float, 0.0],  # 滚动市现率 # will be deprecated soon
                "pbMRQ": [float, 0.0],  # 市净率 # will be deprecated soon
                "open": [float, 0.0],
                "high": [float, 0.0],
                "low": [float, 0.0],
                "close": [float, 0.0],
                "preclose": [float, 0.0],
            },
        }

    def _baostock_timestamper(self, single_fetched):
        mdts = single_fetched[2][:14]  # just for min_fetched, index=2 is timestamp
        # y = mdts[:4]
        # m = mdts[4:6]
        # d = mdts[6:8]
        H = mdts[8:10]
        M = mdts[10:12]
        S = mdts[12:]
        standard_timestamp = f"{H}:{M}:{S}"
        single_fetched[2] = standard_timestamp
        return single_fetched

    def _type_convertor(self, data, convert_mapping):
        """
        data = List[Dict]
        convert_mapping = Dict{key: [target_type, default_value]}
        """
        if not convert_mapping or not data:
            return data

        for idx, d in enumerate(data):
            for k, v in d.items():
                m = convert_mapping.get(k)
                if type(m) is list and len(m) == 2:
                    target_type, default = m
                else:
                    continue
                try:
                    new_v = target_type(v)
                except:
                    new_v = default
                data[idx].update({k: new_v})
        return data

    def table_dispatch(self, code, _type="min30"):
        """
        input: code, _type, output: table_name
        _type: 'min30', 'day'
        e.g. code = 'sh.600006', _type = min30, output = _min30_sh_6000

        """
        assert _type in ["min30", "day"]

        market, number = code.split(".")
        table_name = "_".join([_type, market, number[:3]])
        return table_name
    
    def chunk_yielder(self, data):
        chunk = list()
        for i, d in enumerate(data):
            if i % self.chunk_size == 0 and i > 0:
                yield chunk
                del chunk [:]
            chunk.append(d)
        yield chunk
        

    def _safe_insert(self, data, col, retry=0):
        if retry >= 3:
            return
        try:
            col.insert_many(data)
            time.sleep(1)
            return
        except:
            time.sleep(1)
            return self._safe_insert(data, col, retry+1)

    def insert_data(self, table_name, data_list, conn):
        if not data_list:
            return

        col = conn[table_name]
        if table_name.startswith("all"):
            _type = table_name
        else:
            _type = table_name.split("_")[0]
        data_list = self._type_convertor(data_list, self.stock_fields.get(_type))
        if self.safety_first == True:
            chunks = self.chunk_yielder(data_list)
            for data_chunk in chunks:
                self._safe_insert(data_chunk, col)
        else:
            col.insert_many(data_list)

    def replace_data(self, table_name, new_data_list, conn):
        self.purge_table_with_caution(table_name)
        self.insert_data(table_name, new_data_list, conn)

    def build_scrape_config(self, code, start_date, end_date, _type):
        config = {
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "adjustflag": "1",
            # 'fields': '', # to_be_added in this function,
            # 'frequency': '', # to_be_added in this function,
        }

        if _type.startswith("min"):
            config["fields"] = ",".join(list(self.stock_fields["minute"].keys()))
            config["frequency"] = _type[3:]  # '30' or '5'
        else:  # day as default
            config["fields"] = ",".join(list(self.stock_fields["day"].keys()))
            config["frequency"] = "d"
        return config

    def get_all_codes(self, conn=None):
        if conn is None:
            conn = self.on()
        table_name = self.init_table_names["all_codes"]
        col = conn[table_name]
        all_codes = [q["code"] for q in col.find({})]

        return all_codes

    def get_all_feature_codes(self, conn=None):
        if conn is None:
            conn = self.on()
        table_name = self.init_table_names["all_feature_codes"]
        col = conn[table_name]
        all_feature_codes = [q["code"] for q in col.find({})]

        return all_feature_codes

    def get_feature_data(
        self, code, start_date, end_date, query_key="pctChg", conn=None
    ):
        if conn is None:
            conn = self.on()
        table_name = self.init_table_names["all_feature_data"]
        col = conn[table_name]
        all_feature_data = {"code": code, query_key: list(), "date": list()}
        query_res = col.aggregate(
            [
                {
                    "$match": {
                        "date": {"$gte": start_date, "$lte": end_date},
                        "code": code,
                    }
                },
                {"$group": {"_id": "$date", query_key: {"$first": f"${query_key}"}}},
                {"$sort": {"_id": 1}},  # from start_date to end_date
            ]
        )

        for q in query_res:
            all_feature_data[query_key].append(q[query_key])
            all_feature_data["date"].append(q["_id"])

        return all_feature_data

    def get_latest_date(self, table_name, date_key="date", match={}, conn=None):
        if conn is None:
            conn = self.on()
        col = conn[table_name]
        query_res = col.aggregate([{"$sort": {"_id": -1}}, {"$match": match}, {"$limit": 1}])

        try:
            q = query_res.next()
            if q.get(date_key) is not None:
                return q[date_key]
            else:
                raise f"No date key named {date_key}!"
        except:  # raise StopIteration
            return DAY_ZERO

    def get_cn_name(self, codes, conn=None):
        if conn is None:
            conn = self.on()
        table_name = self.init_table_names["all_codes"]
        col = conn[table_name]
        query_res = col.aggregate(
            [
                {"$match": {"code": {"$in": codes}}},
                {"$group": {"_id": "$code", "code_name": {"$first": "$code_name"}}},
            ]
        )

        result = {qa["_id"]: qa["code_name"] for qa in query_res}
        return result
