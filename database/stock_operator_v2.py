

import sqlite3

import pandas as pd

# from database.base_sqlite import SqliteOperator


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

    def create_tables(self, df, table_name):
        if len(df) == 0:
            print("empty df when creating tables")
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
        print(create_table_sql)
        
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


    def fetch_record(self, temporal, code, start_date=None, end_date=None):
        table_name = f"{temporal}_record"

        if not start_date or not end_date:
            sql = f"SELECT * FROM {table_name} WHERE code='{code}';"
        else:
            sql = f"SELECT * FROM {table_name} WHERE code='{code}' AND date BETWEEN '{start_date}' AND '{end_date}';"
        df = pd.read_sql_query(sql, self.conn)
        return df

    def get_latest_date(self, temporal, code):
        cursor = self.conn.cursor()
        try:
            table_name = f"{temporal}_record"
            sql = f"SELECT MAX(date) FROM {table_name} WHERE code='{code}';"
            cursor.execute(sql)
            result = cursor.fetchone()
            # Return the latest date or None if there are no dates
            return result[0] if result[0] is not None else None
        except sqlite3.Error as e:
            print(f"An error occurred when getting latest date: {e}")
            return None
        finally:
            # Close the database connection
            cursor.close()

    def close(self):
        self.conn.close()


