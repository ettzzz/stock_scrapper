#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:40:14 2022

@author: eee
"""

from database.base_mongo import BaseMongoOperator
from config.static_vars import DAY_ZERO, MONGO_URI, DB_NAME
from utils.datetime_tools import get_delta_date


class stockDatabaseOperator(BaseMongoOperator):
    def __init__(self, mongo_uri=MONGO_URI, db_name=DB_NAME):
        super().__init__(mongo_uri, db_name)
        self.basic_tables = ["all_codes", "all_feature_codes"]

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
        # self.minute_train_cols = [
        #     "date",
        #     "time",
        #     "volume",
        #     "open",
        #     "high",
        #     "low",
        #     "close",
        # ]
        # # self.day_train_cols = ['date', 'turn', 'pctChg', 'peTTM', 'psTTM', 'pcfNcfTTM', \
        # #                        'pbMRQ', 'open', 'high', 'low', 'close', 'preclose']
        # self.day_train_cols = [
        #     "date",
        #     "turn",
        #     "pctChg",
        #     "peTTM",
        #     "open",
        #     "high",
        #     "low",
        #     "close",
        #     "preclose",
        # ]
        # self.feature_train_cols = ["date"] + selected_features
        # self.feature_train_cols = SELECTED_CODES

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

    # def _first_time_check(self):
    #     ## TODO: only update all_codes and all_feature_codes
    #     return

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

    # def init_basic_tables(self):

    #     conn = self.on()
    #     for table in self.basic_tables:

    #         conn.execute(
    #             self.create_table_sql_command(
    #                 self.init_table_names[table], self.stock_fields[table]
    #             )
    #         )
    #     self.off(conn)

    def insert_data(self, table_name, data_list, conn):
        col = conn[table_name]
        if table_name.startswith("all"):
            _type = table_name
        else:
            _type = table_name.split("_")[0]
        data_list = self._type_convertor(data_list, self.stock_fields.get(_type))
        col.insert_many(data_list)

    def replace_data(self, table_name, new_data_list, conn):
        self.purge_table_with_caution(table_name)
        self.insert_data(table_name, new_data_list, conn)

    # def update_feature_list(self, feature_list):
    # # def replace_all_feature_codes(self):
    #     self.purge_tables_with_caution(table_names=self.init_table_names["all_feature_codes"])

    #     conn = self.on()
    #     feature_list_fields = list(self.stock_fields["feature"].keys())
    #     conn.executemany(
    #         self.insert_batch_sql_command(
    #             self.init_table_names["all_feature_codes"], feature_list_fields
    #         ),
    #         feature_list,
    #     )
    #     self.off(conn)

    def build_scrape_config(self, code, start_date, end_date, _type):
        config = {
            "code": code,
            # 'fields': '', # to_be_added in this function,
            "start_date": start_date,
            "end_date": end_date,
            # 'frequency': '', # to_be_added in this function,
            "adjustflag": "1",
        }

        if _type.startswith("min"):
            config["fields"] = ",".join(list(self.stock_fields["minute"].keys()))
            config["frequency"] = _type[3:]  # '30' or '5'
        else:
            config["fields"] = ",".join(list(self.stock_fields["day"].keys()))
            config["frequency"] = "d"
        return config

    # def insert_min30_data(self, code, fetched, fields, conn):
    #     _type = "min30"
    #     fetched = list(map(self._baostock_timestamper, fetched))
    #     table_name = self.table_dispatch(code, _type)
    #     if not self.has_table(table_name, conn):
    #         conn.execute(
    #             self.create_table_sql_command(table_name, self.stock_fields["minute"])
    #         )
    #     conn.executemany(self.insert_batch_sql_command(table_name, fields), fetched)

    # def insert_day_data(self, code, fetched, fields, conn):
    #     _type = "day"
    #     table_name = self.table_dispatch(code, _type)
    #     if not self.has_table(table_name, conn):
    #         conn.execute(
    #             self.create_table_sql_command(table_name, self.stock_fields["day"])
    #         )
    #     conn.executemany(self.insert_batch_sql_command(table_name, fields), fetched)
    #     conn.execute(
    #         "UPDATE '{}' SET turn=0 WHERE turn='';".format(table_name)
    #     )  # ugly patch: in case tradestatus=0 then turn is null

    # def insert_feature_data(self, feature_codes, stacked):
    #     table_name = self.init_table_names["all_feature_data"]
    #     fields = list(self.stock_fields["global"].keys())

    #     conn = self.on()
    #     conn.executemany(self.insert_batch_sql_command(table_name, fields), stacked)
    #     self.off(conn)

    def get_all_codes(self):
        table_name = self.init_table_names["all_codes"]
        conn = self.on()
        col = conn[table_name]
        all_codes = [q["code"] for q in col.find({})]
        self.off()

        return all_codes

    def get_feature_codes(self):
        table_name = self.init_table_names["all_feature_codes"]
        conn = self.on()
        col = conn[table_name]
        feature_codes = [q["code"] for q in col.find({})]
        self.off()

        return feature_codes

    def get_feature_data(self, start_date, end_date, conn=None):
        all_feature_data = self.fetch_by_command(
            "SELECT date, code, feature FROM '{}'\
            WHERE date BETWEEN '{}' AND '{}';".format(
                self.init_table_names["all_feature_data"], start_date, end_date
            ),
            conn=conn,
        )
        all_feature_dict = dict()
        for date, code, feature in all_feature_data:
            codes = code.split(",")
            float_features = list(map(float, feature.split(",")))
            all_feature_dict[date] = dict(zip(codes, float_features))

        return all_feature_dict

    def get_latest_date(self, table_name, date_key="date", match={}):
        conn = self.on()
        col = conn[table_name]
        query_res = col.aggregate(
            [{"$sort": {date_key: -1}}, {"$match": match}, {"$limit": 1}]
        )

        try:
            q = query_res.next()
            self.off()
            if q.get(date_key) is not None:
                return q[date_key]
            else:
                raise f"No date key named {date_key}!"
        except:  # raise StopIteration
            self.off()
            return DAY_ZERO

    def get_cn_name(self, codes):
        table_name = self.init_table_names["all_codes"]
        conn = self.on()
        # name_data =
        name_data = self.fetch_by_command(
            "SELECT code,code_name FROM '{}' WHERE code IN ({});".format(
                self.init_table_names["all_codes"],
                str(codes)[1:-1],  # interesting bastard
            )
        )
        return dict(name_data)

    # def get_train_data(self, code, start_date, end_date):
    #     min30_table = self.table_dispatch(code, _type="min30")
    #     day_table = self.table_dispatch(code, _type="day")
    #     if not self.has_table(min30_table):  # this code is not stored in db
    #         return []

    #     conn = self.on()
    #     min30_data = self.fetch_by_command(
    #         "SELECT {} FROM '{}' \
    #         WHERE code='{}' AND date BETWEEN '{}' AND '{}';".format(
    #             ",".join(self.minute_train_cols),
    #             min30_table,
    #             code,
    #             start_date,
    #             end_date,
    #         ),
    #         conn=conn,
    #     )
    #     day_data = self.fetch_by_command(
    #         "SELECT {} FROM '{}' \
    #         WHERE code='{}' AND date BETWEEN '{}' AND '{}';".format(
    #             ",".join(self.day_train_cols), day_table, code, start_date, end_date
    #         ),
    #         conn=conn,
    #     )
    #     all_feature_dict = self.get_feature_data(start_date, end_date, conn=conn)
    #     self.off(conn)

    #     date_seq = [i[0] for i in day_data]
    #     date_dict = {i[0]: i for i in day_data}
    #     # make sure date is the first element for these 3 lines above

    #     preheat_days = 7
    #     result = []
    #     for each_min in min30_data:
    #         (
    #             date,
    #             _time,
    #             volume,
    #             _open,
    #             high,
    #             low,
    #             _close,
    #         ) = each_min  # should be correct
    #         date_index = date_seq.index(date)

    #         if date_index < preheat_days:
    #             continue
    #         if volume == 0:  # where tradeStatus is 0
    #             continue
    #         else:
    #             target_dates = date_seq[date_index - preheat_days : date_index]
    #             features = [
    #                 round((_close - _open) / _open * 100, 6),
    #                 round((high - low) / low * 100, 6),
    #             ]  # 2
    #             for target_date in target_dates:
    #                 features += list(date_dict[target_date][1:-5])  # days*6
    #                 features += [
    #                     all_feature_dict[target_date][k]
    #                     for k in self.feature_train_cols
    #                 ]  # 3*12 = 36 in total
    #                 d_open, d_high, d_low, d_close, d_preclose = date_dict[target_date][
    #                     -5:
    #                 ]
    #                 features += [
    #                     round((d_close - d_open) / d_open * 100, 6),
    #                     round((d_high - d_low) / d_low * 100, 6),
    #                     round((d_close - d_preclose) / d_preclose * 100, 6),
    #                 ]  # 3 * 3

    #             result.append(
    #                 {
    #                     "code": code,
    #                     "timestamp": date + " " + _time,
    #                     "close": _close,
    #                     "features": features,
    #                 }
    #             )

    #     return result


if __name__ == "__main__":
    her_operator = stockDatabaseOperator()
