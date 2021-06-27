
from .base_operator import sqliteBaseOperator
from config.static_vars import DAY_ZERO

class stockDatabaseOperator(sqliteBaseOperator):
    '''
    TODO:
        5.给date字段加index
    '''
    def __init__(self, sql_dbfile_path):
        super().__init__(sql_dbfile_path)
        self.init_table_names = {
            'field': 'all_zz500_codes',
            'feature': 'all_feature_codes',
            'global': 'all_feature_data'
            }
        self.stock_fields = {
            'field': {
                'updateDate':['DATE', 'NOT NULL'],
                'code': ['TEXT', 'NOT NULL'],
                'code_name': ['TEXT']
                },
            'feature': {
                'code': ['TEXT', 'NOT NULL'],
                'code_name': ['TEXT'],
                'code_fullname': ['TEXT'],
                'update_from': ['TEXT'],
                'orgnization': ['TEXT'],
                'description': ['TEXT']
                },
            'minute':{
                'date':['DATE'],
                'time': ['TIME'],
                'volume': ['INTEGER'],
                'open': ['REAL'],
                'high': ['REAL'],
                'low': ['REAL'],
                'close': ['REAL']
                },
            'day':{
                'date': ['DATE'],
                'volume': ['INTEGER'],
                'isST': ['INTEGER'],
                'tradestatus': ['INTEGER'],
                'turn': ['REAL'], # used an ugly patch in update function, or could use ['REAL', 'DEFAULT 0']
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

        conn = self.on()
        for table in ['field', 'feature']:
            conn.execute(
                self.create_table_sql_command(
                    self.init_table_names[table],
                    self.stock_fields[table])
                )
        self.off(conn)


    def purge_tables_with_caution(self, table_names = []):
        table_names = list(self.init_table_names.values()) if not table_names else table_names
        for t in table_names:
            self.delete_table(t)


    def _update_stock_list(self, code_list, feature_list, purge=False):
        '''
        Run this function first and insert feature data later
        Could be an absolutely fragile function, no exception handling at all.
        just a reminder:
            all mandatory params are from stock_scraper.scrape_pool_data
            code_list -> bs.query_zz500_stocks
            feature_list -> beautifulsoup of 4 tables
        '''
        if purge:
            self.purge_tables_with_caution()
        conn = self.on()

        code_list_fields = list(self.stock_fields['field'].keys())
        conn.executemany(
            self.insert_batch_sql_command(
                self.init_table_names['field'], code_list_fields
                ), code_list
            )

        feature_list_fields = list(self.stock_fields['feature'].keys())
        conn.executemany(
            self.insert_batch_sql_command(
                self.init_table_names['feature'], feature_list_fields
                ),feature_list
            )

        self.off(conn)
        '''
        # we still need some extra procedures to build an available database,
        # which is kind of stupid for now because scraper and operator are
        # different classes

        a, b = his_scraper.scrape_pool_data() # len(a) = 50
        c = his_scraper.scrape_feature_list() # len(c) = 185
        his_operator._update_stock_list(a, c)


        # from here it's updating train-related data:
        feature_codes = his_operator.get_feature_codes() # if len(feature_codes) = 0 then it's fresh new
        start_date = '2019-01-01'
        end_date = '2019-01-31'
        stacks = his_scraper.scrape_feature_data(feature_codes, start_date, end_date)
        his_operator.insert_feature_data(feature_codes, stacks)
        '''


    def generate_scrape_config(self, code, start_date, end_date, _type):
        config = {
            'code': code,
            'fields': 'to_be_added',
            'start_date': start_date,
            'end_date': end_date,
            'frequency': 'to_be_added',
            'adjustflag': '1'
            }
        if _type.startswith('m'):
            config['fields'] = ','.join(list(self.stock_fields['minute'].keys()))
            config['frequency'] = '30'
        else:
            config['fields'] = ','.join(list(self.stock_fields['day'].keys()))
            config['frequency'] = 'd'
        return config


    def insert_min30_data(self, code, fetched, fields):
        for idx, f in enumerate(fetched):
            fetched[idx][1] = ':'.join([f[1][8:10], f[1][10:12], f[1][12:14]]) #  hh:mm:ss

        table_name = 'min30_{}'.format(code.replace('.', '_'))
        conn = self.on()
        conn.execute(
                self.create_table_sql_command(
                    table_name,
                    self.stock_fields['minute'])
                )
        conn.executemany(
            self.insert_batch_sql_command(table_name, fields), fetched
            )

        self.off(conn)


    def insert_day_data(self, code, fetched, fields):
        table_name = 'day_{}'.format(code.replace('.', '_'))
        conn = self.on()
        conn.execute(
                self.create_table_sql_command(
                    table_name,
                    self.stock_fields['day'])
                )
        conn.executemany(
            self.insert_batch_sql_command(table_name, fields), fetched
            )
        conn.execute(
            "UPDATE {} SET turn=0 WHERE turn='';".format(table_name)
        ) # ugly patch: in case tradestatus=0 then turn is null
        self.off(conn)


    def get_feature_codes(self):
        feature_codes = self.fetch_by_command(
            "SELECT code FROM {};".format(self.init_table_names['feature'])
        )
        return feature_codes


    def get_all_codes(self):
        all_codes = self.fetch_by_command(
            "SELECT code FROM {};".format(self.init_table_names['field'])
        )
        return all_codes


    def get_latest_date(self):
        try:
            latest_date = self.fetch_by_command(
                "SELECT MAX(date) FROM {};".format(self.init_table_names['global'])
            )
            return latest_date[0][0] # [('2019-12-31',)]
        except:
            return DAY_ZERO # in case that global table is not created


    def insert_feature_data(self, feature_codes, stacked):
        # Here feature means global feature, that 562 long array in the very beginning
        table_name = self.init_table_names['global']

        global_field_dict = {'date': ['DATE']}
        fields = ['date']
        for code in feature_codes:
            global_field_dict[code[0].replace('.', '_')] = ['REAL']
            fields.append(code[0].replace('.', '_'))

        conn = self.on()
        conn.execute(
            self.create_table_sql_command(
                self.init_table_names['global'],
                global_field_dict)
            )

        conn.executemany(
            self.insert_batch_sql_command(table_name, fields), stacked
            )

        self.off(conn)


    def get_train_data(self, code, start_date, end_date):
        '''
        features =
        2: 30min diff of close and open, 30min diff of high and low
        6: day diff turn,pctChg,peTTM√,psTTM,pcfNcfTTM,pbMRQ√
        185: day diff [185 indices]
        '''

        min30_table = 'min30_{}'.format(code.replace('.', '_'))
        day_table = 'day_{}'.format(code.replace('.', '_'))
        feature_table =  self.init_table_names['global']

        if not self.table_info(min30_table): # this code is not stored in db
            return []

        min30_data = self.fetch_by_command(
            "SELECT * FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                min30_table, start_date, end_date)
            )
        day_data = self.fetch_by_command(
            "SELECT uid,date,turn,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ\
                FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                day_table, start_date, end_date)
            )
        all_feature_data = self.fetch_by_command(
            "SELECT * FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                feature_table, start_date, end_date)
            )
        date_seq = [i[1] for i in day_data]
        date_dict = {i[1]: i for i in day_data}
        all_feature_dict = {i[1]: i for i in all_feature_data}

        result = []
        for each_min in min30_data:
            uid, date, _time, volume, _open, high, low, _close = each_min # should be correct
            # uid, date, _time, _open, high, low, _close, volume = each_min # should not be correct
            date_index = date_seq.index(date)

            if date_index == 0:
                continue
            if volume == 0:
                continue
            else:
                target_date = date_seq[date_index - 1]
                # [round((low - volume)/volume, 6), round((_open - high)/high, 6)] + \
                #
                features = [round((_close - _open)/_open*100, 6), round((high - low)/low*100, 6)] + \
                            list(date_dict[target_date][2:]) + \
                            list(all_feature_dict[target_date][2:]) # 2, 6 and 185
                result.append({
                    'code': code,
                    'timestamp': date + ' ' + _time,
                    'close': _close,
                    'features': features
                    })

        return result
    
    
    def _get_train_data(self, code, start_date, end_date):
        '''
        features =
        2: 30min diff of close and open, 30min diff of high and low
        6: day diff turn,pctChg,peTTM√,psTTM,pcfNcfTTM,pbMRQ√
        185: day diff [185 indices]
        '''

        min30_table = 'min30_{}'.format(code.replace('.', '_'))
        day_table = 'day_{}'.format(code.replace('.', '_'))
        feature_table =  self.init_table_names['global']
        sf = ['sh.000001', 'sh.000003', 'sz.399908', 'sz.399909',\
                             'sz.399910', 'sz.399911', 'sz.399912', 'sz.399913',\
                            'sz.399914', 'sz.399915', 'sz.399916', 'sz.399917']
        selected_features = list(map(lambda x: x.replace('.','_'), sf))

        if not self.table_info(min30_table): # this code is not stored in db
            return []

        min30_data = self.fetch_by_command(
            "SELECT * FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                min30_table, start_date, end_date)
            )
        day_data = self.fetch_by_command(
            "SELECT uid,date,turn,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,open,high,low,close,preclose\
                FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                day_table, start_date, end_date)
            )
        all_feature_data = self.fetch_by_command(
            "SELECT uid,date,{} FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                ','.join(selected_features), feature_table, start_date, end_date)
            )
        date_seq = [i[1] for i in day_data]
        date_dict = {i[1]: i for i in day_data}
        all_feature_dict = {i[1]: i for i in all_feature_data}
        

        result = []
        for each_min in min30_data:
            uid, date, _time, volume, _open, high, low, _close = each_min # should be correct
            date_index = date_seq.index(date)

            if date_index < 3:
                continue
            if volume == 0: # where tradeStatus is 0
                continue
            else:
                target_dates = date_seq[date_index - 3: date_index]
                features = [round((_close - _open)/_open*100, 6), round((high - low)/low*100, 6)] # 2
                for target_date in target_dates: 
                    features += list(date_dict[target_date][2:-5]) # 3*6 = 18 in total
                    features += list(all_feature_dict[target_date][2:]) # 3*12 = 36 in total
                    d_open, d_high, d_low, d_close, d_preclose = date_dict[target_date][-5:]
                    features += [round((d_close - d_open)/d_open*100, 6),
                                 round((d_high-d_low)/d_low*100, 6),
                                 round((d_close-d_preclose)/d_preclose*100, 6)]
                
                result.append({
                    'code': code,
                    'timestamp': date + ' ' + _time,
                    'close': _close,
                    'features': features
                    })

        return result


# start_date = '2015-01-01'
# end_date = '2021-05-31'
# codes = her_operator.get_feature_codes()
# codes = list(map(lambda x: x[0], codes))

# codes = ['sh.000001', 'sh.000002', 'sh.000003']
# codes_str = ','.join(list(map(lambda x: x.replace('.', '_'), codes)))
# sh1 = her_operator.fetch_by_command("SELECT {} FROM {} WHERE date BETWEEN '{}' AND '{}';".format(codes_str, feature_table, start_date, end_date))
# a = np.corrcoef(np.array(sh1).T)
# sel = []
# for idx, i in enumerate(a[0]):
#     if not i:
#         continue
#     if i <= 0.6:
#         sel.append(codes[idx])

# her_operator.fetch_by_command("SELECT code,code_name, code_fullname FROM all_feature_codes WHERE code IN ({});".format(str(sel)[1:-1]))



    def get_live_data(self, code, min30_data):
        '''
        min30_data should be shaped into [(a,b,c,d),(e,f,g,h)..] format
        '''
        day_table = 'day_{}'.format(code.replace('.', '_'))
        feature_table =  self.init_table_names['global']
        latest_trade_date = self.get_latest_date()

        day_data = self.fetch_by_command(
            "SELECT uid,date,turn,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ\
                FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                day_table, latest_trade_date, latest_trade_date)
            )
        all_feature_data = self.fetch_by_command(
            "SELECT * FROM {} WHERE date BETWEEN '{}' AND '{}';".format(
                feature_table, latest_trade_date, latest_trade_date)
            )

        date_dict = {i[1]: i for i in day_data}
        all_feature_dict = {i[1]: i for i in all_feature_data}

        result = []
        for each_min in min30_data: # though this min30_data list only contains one tuples
            date, _time, _open, high, low, _close = each_min
            features = [round((_close - _open)/_open, 6), round((high - low)/low, 6)] + \
                        list(date_dict[latest_trade_date][2:]) + \
                        list(all_feature_dict[latest_trade_date][2:]) # 2, 6 and 185
            result.append({
                'code': code,
                'timestamp': date + ' ' + _time,
                'close': _close,
                'features': features
                })

        return result


    def get_cn_name(self, codes_str):
        codes = codes_str.split(',')    
        name_data = self.fetch_by_command(
            "SELECT code,code_name FROM {} WHERE code IN ({});".format(
                self.init_table_names['field'], 
                str(codes)[1:-1]
                )
            )
        return dict(name_data)
