import datetime as dt
import io
import zipfile

import polars as pl

from pipelines.utils.barrids import get_barrids
from pipelines.utils.barra_datasets import barra_ids

tickers_column_mapping = {
    "!Barrid": "barrid",
    "AssetIDType": "asset_id_type",
    "AssetID": "asset_id",
    "StartDate": "start_date",
    "EndDate": "end_date",
}


def _clean_tickers(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(tickers_column_mapping)
        .filter(pl.col("asset_id_type").eq("LOCALID"))
        .rename({"asset_id": "ticker"})
        .with_columns(pl.col("ticker").str.replace("US", ""))
        .sort("barrid")
        .select("barrid", "ticker")
    )


def get_tickers(date_: dt.date):

    # use barra_datasets to get file paths
    zip_folder_path = barra_ids.daily_zip_folder_path(date_)
    file_name = barra_ids.file_name(date_)

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=1,
            separator="|",
        )

        return _clean_tickers(df)

def barrid_ticker_join(date_: dt.date)-> pl.DataFrame:
    tickers_df = get_tickers(date_)

    barrids_df = (
        get_barrids(date_)
        .join(tickers_df, on="barrid", how="left")
        .filter(pl.col("ticker").is_not_null())
        .select("barrid", "ticker")
        .sort("barrid")
    )

    return barrids_df
