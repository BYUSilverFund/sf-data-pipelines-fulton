from pathlib import Path
import polars as pl
from datetime import date, timedelta

from pipelines.utils.barra_datasets import barra_returns
from pipelines.utils import s3
from pipelines.utils.tickers import barrid_ticker_join

rename_column_mapping = {
    "!Barrid": "barrid",
    "Price": "price",
    "Currency": "currency",
    "DlyReturn%": 'daily_return',
    "DataDate": "date"
}

def historical_data_flow(date_:date) -> None:

    def read_file_into_df(zip_path: Path, filename: str) -> pl.DataFrame:

        '''
        Function to read the zipfile
        '''
        import io
        import zipfile

        with zipfile.ZipFile(zip_path, "r") as z:
            # 3. Read the raw bytes and inspect first few lines
            raw_bytes = z.read(filename)
            if len(raw_bytes) == 0:
                raise ValueError(f"File '{filename}' inside the zip is empty (0 bytes).")

            # 4. Parse with Polars with tolerant settings for Barra data
            return pl.read_csv(
                io.BytesIO(raw_bytes),
                skip_rows=1,
                separator="|",
                infer_schema_length=10000,
                truncate_ragged_lines=True,
                ignore_errors=True,
            )

    def format_s3_file_name(date_: date) -> str: 
        return date_.strftime('%Y/%m/%d.parquet')

    print(date_)
    while date_ < date.today():
        # 1. get current date, unzip yesterdays file, clean data
        print(f"pulling historical data for {date_}")
        zip_path = barra_returns.daily_zip_folder_path(date_)
        file_name = barra_returns.file_name(date_)
        try:
            df = read_file_into_df(zip_path, file_name)
        except:
            print(f"no file for {date_}")
            date_ += timedelta(days=1)
            continue

        df = df.drop("Capt", "PriceSource")
        df = df.rename(rename_column_mapping)

        # 2. Get tickers and join to history file on barrid
        barrids_ticker_df = barrid_ticker_join(date_)

        history_with_tickers = (
            df
            .join(barrids_ticker_df, on="barrid", how="left")
            .filter(pl.col("ticker").is_not_null())
            .sort("barrid")
        )

        # # . Write cleaned file to S3
        s3.write_parquet(
            bucket_name="barra-stock-history",
            file_name=format_s3_file_name(date_),
            file_data=history_with_tickers,
        )

        date_ += timedelta(days=1)

    # # 3. Log completion
    print("Historical data flow complete")


