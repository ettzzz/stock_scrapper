




from datetime import datetime, timedelta

from scrapper.baostock_scrapper_v2 import stockScrapper
from database.stock_operator_v2 import StockDatabase

scrapper = stockScrapper()
db = StockDatabase()


all_codes = scrapper.get_all_codes_df()
all_codes = all_codes[
    (all_codes["type"] == "1")
].reset_index()

today = datetime.now().strftime("%Y-%m-%d")


def get_next_date(input_date):
    # Parse the input date string into a datetime object
    date_object = datetime.strptime(input_date, "%Y-%m-%d %H:%M:%S")
    # Add one day to the date object
    next_date = date_object + timedelta(days=1)
    # Return the next date as a string in the same format
    return next_date.strftime("%Y-%m-%d")


for idx, row in all_codes.iterrows():
    code = row["code"]
    start_date = row["ipoDate"]
    end_date = row["outDate"] if row["outDate"] != "" else today
    for frequency in ["d", "w", "m"]:
        table_name = f"{frequency}_record"
        latest_date = db.get_latest_date(frequency, code)
        if latest_date is None:
            df = scrapper.get_k_data_df(code, frequency, start_date, end_date)
            db.create_tables(df, table_name)
            db.insert_record(df, table_name)
        else:
            next_date = get_next_date(latest_date)
            df = scrapper.get_k_data_df(code, frequency, next_date, end_date)
            # print(f"code {code} latest_date {latest_date}, next_date {next_date}, end_date {end_date} len {len(df)}")
            db.insert_record(df, table_name)
    print(f"{idx}/{len(all_codes)}, {code} done!")
