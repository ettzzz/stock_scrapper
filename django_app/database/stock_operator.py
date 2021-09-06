
from .base_operator import sqliteBaseOperator
from config.static_vars import DAY_ZERO, IS_STOCK_WHOLE
from config.static_vars import selected_features
from utils.datetime_tools import get_delta_date

'''
索引不应该使用在较小的表上。 hit
索引不应该使用在有频繁的大批量的更新或插入操作的表上。 hit
索引不应该使用在含有大量的 NULL 值的列上。 not hit
索引不应该使用在频繁操作的列上。 not hit
'''

class stockDatabaseOperator(sqliteBaseOperator):
    def __init__(self, sql_dbfile_path):
        super().__init__(sql_dbfile_path)
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
                'code': ['TEXT'], # TO BE CREATED
                'date': ['DATE'],
                'time': ['TIME'],
                'volume': ['INTEGER'],
                'open': ['REAL'],
                'high': ['REAL'],
                'low': ['REAL'],
                'close': ['REAL']
            },
            'day': {
                'code': ['TEXT'], # TO BE CREATED
                'date': ['DATE'],
                'volume': ['INTEGER'],
                'isST': ['INTEGER'],
                'tradestatus': ['INTEGER'],
                'turn': ['REAL'], #  'DEFAULT 0', 'NOT NULL' not working as '' is not NULL
                'pctChg': ['REAL'],
                'peTTM': ['REAL'],  # 滚动市盈率
                'psTTM': ['REAL'],  # 滚动市销率 # will be deprecated soon
                'pcfNcfTTM': ['REAL'],  # 滚动市现率 # will be deprecated soon
                'pbMRQ': ['REAL'],  # 市净率 # will be deprecated soon
                'open': ['REAL'],
                'high': ['REAL'],
                'low': ['REAL'],
                'close': ['REAL'],
                'preclose': ['REAL']
            },
        }
        self.minute_train_cols = ['date','time','volume', 'open', 'high', 'low', 'close']
        self.day_train_cols = ['date', 'turn', 'pctChg', 'peTTM', 'psTTM', 'pcfNcfTTM', \
                               'pbMRQ', 'open', 'high', 'low', 'close', 'preclose']
        self.feature_train_cols = ['date'] + selected_features

    def purge_tables_with_caution(self, table_names=[]):
        table_names = list(self.init_table_names.values()) if not table_names else table_names
        for t in table_names:
            self.delete_table(t)

    def _table_dispatch(self, code, _type='min30'):
        '''
        input: code, _type, output: table_name
        _type: 'min30', 'day'
        '''
        if _type == 'min30':  # will return min30_sh_6000
            table_name = _type + '_' + code[:7].replace('.', '_')
        else:  # day, will return day_sh_600
            table_name = _type + '_' + code[:6].replace('.', '_')

        return table_name

    def _init_basic_tables(self):
        conn = self.on()
        for table in ['field', 'feature', 'whole_field']:
            conn.execute(
                self.create_table_sql_command(
                    self.init_table_names[table],
                    self.stock_fields[table])
            )
        self.off(conn)

    def _update_stock_list(self,
                           code_list,
                           feature_list,
                           whole_code_list,
                           purge=False,
                           ):
        if purge:
            self.purge_tables_with_caution()
        conn = self.on()
        code_list_fields = list(self.stock_fields['field'].keys())
        conn.executemany(
            self.insert_batch_sql_command(
                self.init_table_names['field'], code_list_fields
            ), code_list
        )
        whole_code_list_fields = list(self.stock_fields['whole_field'].keys())
        conn.executemany(
            self.insert_batch_sql_command(
                self.init_table_names['whole_field'], whole_code_list_fields
            ), whole_code_list
        )
        feature_list_fields = list(self.stock_fields['feature'].keys())
        conn.executemany(
            self.insert_batch_sql_command(
                self.init_table_names['feature'], feature_list_fields
            ), feature_list
        )
        self.off(conn)
        '''
        # from here it's updating train-related data:
        feature_codes = his_operator.get_feature_codes() # if len(feature_codes) = 0 then it's fresh new
        start_date = '2019-01-01'
        end_date = '2019-01-31'
        stacks = his_scraper.scrape_feature_data(feature_codes, start_date, end_date)
        his_operator.insert_feature_data(feature_codes, stacks)
        '''

    def _baostock_timestamper(self, single_fetched):
        mdts = single_fetched[2][:14] # just for min_fetched, index=2 is timestamp
        # y = mdts[:4]
        # m = mdts[4:6]
        # d = mdts[6:8]
        H = mdts[8:10]
        M = mdts[10:12]
        S = mdts[12:]
        standard_timestamp = '{}:{}:{}'.format(H, M, S)
        single_fetched[2] = standard_timestamp
        return single_fetched

    def generate_scrape_config(self, code, start_date, end_date, _type):
        config = {
            'code': code,
            # 'fields': '', # to_be_added in this function,
            'start_date': start_date,
            'end_date': end_date,
            # 'frequency': '', # to_be_added in this function,
            'adjustflag': '1'
        }

        if _type.startswith('min'):
            config['fields'] = ','.join(list(self.stock_fields['minute'].keys()))
            config['frequency'] = _type[3:]  # '30' or '5'
        else:
            config['fields'] = ','.join(list(self.stock_fields['day'].keys()))
            config['frequency'] = 'd'
        return config

    def insert_min30_data(self, code, fetched, fields, conn):
        _type = 'min30'
        fetched = list(map(self._baostock_timestamper, fetched))
        table_name = self._table_dispatch(code, _type)
        conn.execute(
            self.create_table_sql_command(
                table_name,
                self.stock_fields['minute'])
        )
        conn.executemany(
            self.insert_batch_sql_command(table_name, fields), fetched
        )


    def insert_day_data(self, code, fetched, fields, conn):
        _type = 'day'
        table_name = self._table_dispatch(code, _type)
        conn.execute(
            self.create_table_sql_command(
                table_name,
                self.stock_fields['day'])
        )
        conn.executemany(
            self.insert_batch_sql_command(table_name, fields), fetched
        )
        conn.execute(
            "UPDATE '{}' SET turn=0 WHERE turn='';".format(table_name)
        ) # ugly patch: in case tradestatus=0 then turn is null

    def insert_feature_data(self, feature_codes, stacked):
        table_name = self.init_table_names['global']
        global_field_dict = {'date': ['DATE']}
        fields = ['date']
        for code in feature_codes:
            code = code[0]
            global_field_dict[code.replace('.', '_')] = ['REAL']
            fields.append(code.replace('.', '_'))

        conn = self.on()
        conn.execute(
            self.create_table_sql_command(table_name, global_field_dict)
        )
        conn.executemany(
            self.insert_batch_sql_command(table_name, fields), stacked
        )
        self.off(conn)

    def get_feature_codes(self):
        feature_codes = self.fetch_by_command(
            "SELECT code FROM '{}';".format(self.init_table_names['feature'])
        )
        return feature_codes

    def get_all_codes(self, is_train=not IS_STOCK_WHOLE):
        if is_train:
            table_name = self.init_table_names['field']
        else:
            table_name = self.init_table_names['whole_field']
        all_codes = self.fetch_by_command(
            "SELECT code FROM '{}';".format(table_name)
        )
        return all_codes

    def get_latest_date(self, _type='min30', code='sh.600006'):
        try:
            if _type in ['min30', 'day']:
                table_name = self._table_dispatch(code, _type)
            else:
                table_name = self.init_table_names['global']

            latest_date = self.fetch_by_command(
                "SELECT MAX(date) FROM '{}';".format(table_name)
            )
            return latest_date[0][0]  # [('2019-12-31',)]
        except:
            return get_delta_date(DAY_ZERO, -1)  # in case that global table is not created

    def get_cn_name(self, codes):
        name_data = self.fetch_by_command(
            "SELECT code,code_name FROM '{}' WHERE code IN ({});".format(
                self.init_table_names['whole_field'],
                str(codes)[1:-1]  # interesting bastard
            )
        )
        return dict(name_data)


    def get_train_data(self, code, start_date, end_date):
        min30_table = self._table_dispatch(code, _type='min30')
        day_table = self._table_dispatch(code, _type='day')
        feature_table = self.init_table_names['global']
        if not self.table_info(min30_table):  # this code is not stored in db
            return []

        conn = self.on()
        min30_data = self.fetch_by_command(
            "SELECT {} FROM '{}' \
            WHERE code='{}' AND date BETWEEN '{}' AND '{}';".format(
                ','.join(self.minute_train_cols), min30_table,
                 code, start_date, end_date),
            conn=conn
        )
        day_data = self.fetch_by_command(
            "SELECT {} FROM '{}' \
            WHERE code='{}' AND date BETWEEN '{}' AND '{}';".format(
                ','.join(self.day_train_cols), day_table,
                code, start_date, end_date),
            conn=conn
        )
        all_feature_data = self.fetch_by_command(
            "SELECT {} FROM '{}'\
            WHERE date BETWEEN '{}' AND '{}';".format(
                ','.join(self.feature_train_cols), feature_table,
                start_date, end_date),
            conn=conn
        )
        self.off(conn)

        date_seq = [i[0] for i in day_data]
        date_dict = {i[0]: i for i in day_data}
        all_feature_dict = {i[0]: i for i in all_feature_data}
        # make sure date is the first element for these 3 lines above

        result = []
        for each_min in min30_data:
            date, _time, volume, _open, high, low, _close = each_min  # should be correct
            date_index = date_seq.index(date)

            if date_index < 3:
                continue
            if volume == 0:  # where tradeStatus is 0
                continue
            else:
                target_dates = date_seq[date_index - 3: date_index]
                features = [round((_close - _open)/_open*100, 6), round((high - low)/low*100, 6)]  # 2
                for target_date in target_dates:
                    features += list(date_dict[target_date][1:-5])  # 3*6 = 18 in total
                    features += list(all_feature_dict[target_date][1:])  # 3*12 = 36 in total
                    d_open, d_high, d_low, d_close, d_preclose = date_dict[target_date][-5:]
                    features += [round((d_close - d_open)/d_open*100, 6),
                                 round((d_high-d_low)/d_low*100, 6),
                                 round((d_close-d_preclose)/d_preclose*100, 6)]  # 3 * 3

                result.append({
                    'code': code,
                    'timestamp': date + ' ' + _time,
                    'close': _close,
                    'features': features
                })

        return result

    def get_partial_live_data(self, codes, dates):
        start_date = dates[0]
        end_date = dates[-1]
        results = dict()
        conn = self.on()
        for code in codes:
            day_data = self.fetch_by_command(
                "SELECT {} FROM '{}' \
                WHERE code='{}' AND date BETWEEN '{}' AND '{}';".format(
                ','.join(self.day_train_cols), self._table_dispatch(code, _type='day'),
                code, start_date, end_date
                ),
                conn=conn
            )
            all_feature_data = self.fetch_by_command(
                "SELECT {} FROM '{}'\
                WHERE date BETWEEN '{}' AND '{}';".format(
                ','.join(self.feature_train_cols), self.init_table_names['global'],
                start_date, end_date),
                conn=conn
            )
            date_dict = {i[0]: i for i in day_data}
            all_feature_dict = {i[0]: i for i in all_feature_data}

            features = []  # should be 2 but here is blank
            for target_date in dates:
                features += list(date_dict[target_date][1:-5])  # 3*6 = 18 in total
                features += list(all_feature_dict[target_date][1:])  # 3*12 = 36 in total
                d_open, d_high, d_low, d_close, d_preclose = date_dict[target_date][-5:]
                features += [round((d_close - d_open)/d_open*100, 6),
                             round((d_high-d_low)/d_low*100, 6),
                             round((d_close-d_preclose)/d_preclose*100, 6)]  # 3*3=9 in total

            results[code] = features
        self.off(conn)
        return results
