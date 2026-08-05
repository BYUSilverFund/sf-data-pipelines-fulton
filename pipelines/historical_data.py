from pathlib import Path
import polars as pl
import os
from datetime import date, timedelta
import boto3

from pipelines.utils.barra_datasets import barra_returns
from pipelines.utils import s3

def historical_data_flow() -> None:

    def read_file_into_df(zip_path: Path, filename: str) -> pl.DataFrame:

        '''
        daily = True means we read the daily file be default, set to False to read the history file
        '''
        import io
        import zipfile


        with zipfile.ZipFile(zip_path, "r") as z:
            df = pl.read_csv(
                io.BytesIO(z.read(filename)),
                skip_rows=1,
                separator="|",
            )
        return df

    def format_s3_file_name(date_: date) -> str: 
        return date_.strftime('%Y/%m/%d.parquet')

    # 1. get current date, unzip yesterdays file, clean data
    yesterday = date.today() - timedelta(days=1)
    print(f"pulling historical data for {yesterday}")
    zip_path = barra_returns.daily_zip_folder_path(yesterday)
    file_name = barra_returns.file_name(yesterday)
    df = read_file_into_df(zip_path, file_name)
    df = df.drop("Capt", "PriceSource")

    # # 2. Write cleaned file to S3
    s3.write_parquet(
        bucket_name="barra-stock-history",
        file_name=format_s3_file_name(yesterday),
        file_data=df,
    )

    # # 3. Log completion
    print("Historical data flow complete")


