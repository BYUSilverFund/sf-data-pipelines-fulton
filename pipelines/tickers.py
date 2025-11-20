import datetime as dt
import io
import zipfile

import polars as pl

from pipelines.variables import FACTORS, ROOT

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
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOW_XSEDOL_ID_{date_str_1}.zip"
    file_name = f"USA_XSEDOL_Asset_ID.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=1,
            separator="|",
        )

        return _clean_tickers(df)
