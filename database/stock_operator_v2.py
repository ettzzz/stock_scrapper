

import sqlite3

import pandas as pd

from utils import gabber

class StockDatabase:
    def __init__(self, db_name='stock_data.db'):
        self.conn = sqlite3.connect(db_name)
        self.text_cols = [
            "code",
            "date",
            "adjustflag",
            "tradestatus",
            "isST"
        ]
        
    def if_table_exist(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if cursor.fetchone() is not None:
            cursor.close()
            return True
        return False

    def create_tables(self, df, table_name):
        if len(df) == 0:
            gabber.info("empty df when creating tables")
            return
        
        # Create the SQL statement for creating the table
        columns = ', '.join([f"{col} REAL" if col not in self.text_cols else f"{col} TEXT" for col in df.columns])
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns},
                UNIQUE(code, date)
            );
        '''
        # Execute the SQL statement to create the table
        self.conn.execute(create_table_sql)

        self.conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_code ON {table_name} (code);')
        self.conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} (date);')
        self.conn.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_code_date ON {table_name} (code, date);')

        self.conn.commit()

    def insert_record(self, df, table_name):
        if len(df) == 0:
            return
        df['date'] = pd.to_datetime(df['date'])  # Ensure date is in datetime format
        df.to_sql(table_name, self.conn, if_exists='append', index=False)


    def fetch_record(self, table_name, code,  start_date="", end_date=""):
        # table_name = f"{temporal}_record"
        where_clause = []
        if len(start_date) > 0:
            where_clause.append(f"date >= '{start_date}'")
        if len(end_date) > 0:
            where_clause.append(f"date <= '{end_date}'")

        if len(where_clause) == 0:
            clause = ""
        elif len(where_clause) == 1:
            clause = "AND " + where_clause[0]
        else:
            clause = "AND " + where_clause[0] + " AND " + where_clause[1]


        sql = f"SELECT * FROM {table_name} WHERE code='{code}' {clause};"
        df = pd.read_sql_query(sql, self.conn)
        return df

    def get_latest_date(self, temporal, code):
        cursor = self.conn.cursor()
        try:
            table_name = f"{temporal}_record"
            if not self.if_table_exist(table_name):
                return None
            sql = f"SELECT MAX(date) FROM {table_name} WHERE code='{code}';"
            cursor.execute(sql)
            result = cursor.fetchone()
            # Return the latest date or None if there are no dates
            return result[0] if result[0] is not None else None
        except sqlite3.Error as e:
            gabber.error(f"An error occurred when getting latest date: {e}")
            return None
        finally:
            # Close the database connection
            cursor.close()

    def close(self):
        self.conn.close()


