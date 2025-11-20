import datetime as dt
import io
import zipfile

import polars as pl

from pipelines.variables import FACTORS, ROOT

root_ids_column_mapping = {
    "!Barrid": "barrid",
    "Name": "name",
    "Instrument": "instrument",
    "IssuerID": "issuer_id",
    "ISOCountryCode": "iso_country_code",
    "ISOCurrencyCode": "iso_currency_code",
    "RootID": "root_id",
    "StartDate": "start_date",
    "EndDate": "end_date",
}


def _clean_root_ids(df: pl.DataFrame, date_: dt.date) -> pl.DataFrame:
    return (
        df.rename(root_ids_column_mapping)
        .with_columns(
            pl.col("start_date", "end_date")
            .cast(pl.String)
            .str.strptime(pl.Date, "%Y%m%d")
        )
        .filter(
            pl.col("barrid").ne("[End of File]"),
            pl.col("instrument").is_in(["STOCK", "ETF", "ADR", "CROSS_LIST"]),
            pl.col("start_date").le(date_),
            pl.col("end_date").ge(date_),
            pl.col("iso_country_code").eq("USA"),
        )
        .select("barrid")
    )


def get_barrids(date_: dt.date):
    date_str_1 = date_.strftime("%y%m%d")
    date_str_2 = date_.strftime("%Y%m%d")

    zip_folder_path = f"{ROOT}/bime/SMD_USSLOW_XSEDOL_ID_{date_str_1}.zip"
    file_name = f"USA_Asset_Identity.{date_str_2}"

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=1,
            separator="|",
        )

        return _clean_root_ids(df, date_)
