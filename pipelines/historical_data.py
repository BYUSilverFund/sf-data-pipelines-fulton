from pathlib import Path
import polars as pl
from datetime import date, timedelta

from pipelines.utils.barra_datasets import barra_returns
from pipelines.utils import s3

def historical_data_flow(date_:date) -> None:

    def read_file_into_df(zip_path: Path, filename: str) -> pl.DataFrame:

        '''
        Function to read the zipfile
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
        date_ += timedelta(days=1)

        # # 2. Write cleaned file to S3
        s3.write_parquet(
            bucket_name="barra-stock-history",
            file_name=format_s3_file_name(date_),
            file_data=df,
        )

    # # 3. Log completion
    print("Historical data flow complete")


