import datetime

from scrapper.baostock_scrapper_v2 import stockScrapper
from database.stock_operator_v2 import StockDatabase
from utils import gabber


def main():
    scrapper = stockScrapper()
    db = StockDatabase()
    
    all_codes_df = scrapper.get_all_codes_df()
    all_codes_df = all_codes_df[
        (all_codes_df["type"] == "1")
    ].reset_index(drop=True)

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    for frequency in ["d", "w", "m"]:
        table_name = f"{frequency}_record"
        table_exist = db.if_table_exist(table_name)
        for idx, row in all_codes_df.iterrows():
            code = row["code"]
            if row["outDate"] != "":
                continue
            end_date = today
            try:
                start_date = db.get_latest_date(frequency, code)
                if start_date is None: # not exist
                    start_date = row["ipoDate"]
                else: # need update
                    start_date = start_date.split(" ")[0]
                    start_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
                
                # if start_date == end_date: ## the code has quitted market and we have all its data.
                #     continue
                gabber.info(f"update_rawdata for {code}-{frequency}, {idx+1}/{len(all_codes_df)}, start_date: {start_date}, end_date: {end_date}")

                df = scrapper.get_k_data_df(code, frequency, start_date, end_date)
                if not table_exist:
                    db.create_tables(df, table_name)
                db.insert_record(df, table_name)
                
            except Exception as e:
                gabber.error(f"error {code} {e}")

if __name__ == "__main__":
    main()