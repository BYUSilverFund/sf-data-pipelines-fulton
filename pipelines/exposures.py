import datetime as dt
import io
import zipfile

import polars as pl

from pipelines.variables import FACTORS, ROOT

exposures_column_mapping = {
    "!Barrid": "barrid",
    "Factor": "factor",
    "Exposure": "exposure",
    "DataDate": "date",
}


def _clean_exposures(df: pl.DataFrame, barrids: list[str]) -> pl.DataFrame:
    return (
        df.rename(exposures_column_mapping)
        .filter(pl.col("barrid").ne("[End of File]"), pl.col("barrid").is_in(barrids))
        .with_columns(pl.col("date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d"))
        .pivot(index=["date", "barrid"], on="factor", values="exposure")
        .sort("barrid")
        .select("date", "barrid", *FACTORS)
    )


def get_stock_exposures(date_: dt.date, barrids: list[str]) -> pl.DataFrame:
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_{date_str_1}.zip"
    file_name = f"USSLOWL_100_Asset_Exposure.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return _clean_exposures(df, barrids)


def get_etf_exposures(date_: dt.date, barrids: list[str]) -> pl.DataFrame:
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOWL_100_ETF_{date_str_1}.zip"
    file_name = f"USSLOWL_ETF_100_Asset_Exposure.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return _clean_exposures(df, barrids)
