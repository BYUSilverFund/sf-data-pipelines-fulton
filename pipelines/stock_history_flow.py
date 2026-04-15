import datetime as dt
import io
import zipfile

import polars as pl
from tqdm import tqdm

from pipelines.utils import s3, get_market_dates_in_range
from pipelines.variables import ROOT


stock_history_column_mapping = {
    "!Barrid": "barrid",
    "Price": "price",
    "Mktcap": "market_cap",
    "PrcSrc": "price_source",
    "Currency": "currency",
    "Return": "return",
    "DataDate": "date",
}

asset_identity_column_mapping = {
    "!Barrid": "barrid",
    "Name": "name",
    "Instrument": "instrument",
}

tickers_column_mapping = {
    "!Barrid": "barrid",
    "AssetIDType": "asset_id_type",
    "AssetID": "ticker",
}

STOCK_HISTORY_COLUMNS = [
    "barrid", "ticker", "name", "instrument",
    "price", "market_cap", "price_source", "currency", "return", "date",
]


def _clean_stock_history(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(stock_history_column_mapping, strict=False)
        .filter(pl.col("barrid").ne("[End of File]"))
        .with_columns(pl.col("date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d"))
        .select(["barrid", "price", "market_cap", "price_source", "currency", "return", "date"])
    )


def _clean_asset_identity(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(asset_identity_column_mapping, strict=False)
        .filter(pl.col("barrid").ne("[End of File]"))
        .select("barrid", "name", "instrument")
    )


def _clean_tickers(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(tickers_column_mapping, strict=False)
        .filter(pl.col("asset_id_type").eq("LOCALID"))
        .with_columns(pl.col("ticker").str.replace("US", ""))
        .select("barrid", "ticker")
    )



def stock_history_backfill_flow(start_date, end_date) -> None:
    start_date = start_date.date() if hasattr(start_date, "date") else start_date
    end_date = end_date.date() if hasattr(end_date, "date") else end_date

    years = list(range(start_date.year, end_date.year + 1))
    market_dates = get_market_dates_in_range(start_date, end_date)

    frames = []
    for date_ in tqdm(market_dates, desc="Reading zip files"):
        date_str_1 = date_.strftime("%y%m%d")
        date_str_2 = date_.strftime("%Y%m%d")

        price_zip_path = f"{ROOT}/us/usslow/SMD_USSLOWL_100_{date_str_1}.zip"
        id_zip_path = f"{ROOT}/bime/SMD_USSLOW_XSEDOL_ID_{date_str_1}.zip"

        try:
            with zipfile.ZipFile(price_zip_path, "r") as zf:
                raw = io.BytesIO(zf.read(f"USSLOW_Daily_Asset_Price.{date_str_2}"))
                price_df = _clean_stock_history(pl.read_csv(raw, skip_rows=1, separator="|"))
        except (FileNotFoundError, KeyError):
            continue

        try:
            with zipfile.ZipFile(id_zip_path, "r") as zf:
                ticker_df = _clean_tickers(
                    pl.read_csv(io.BytesIO(zf.read(f"USA_XSEDOL_Asset_ID.{date_str_2}")), skip_rows=1, separator="|")
                )
                identity_df = _clean_asset_identity(
                    pl.read_csv(io.BytesIO(zf.read(f"USA_Asset_Identity.{date_str_2}")), skip_rows=1, separator="|")
                )
            price_df = price_df.join(ticker_df, on="barrid", how="left").join(identity_df, on="barrid", how="left")
        except (FileNotFoundError, KeyError):
            price_df = price_df.with_columns(
                pl.lit(None).cast(pl.String).alias("ticker"),
                pl.lit(None).cast(pl.String).alias("name"),
                pl.lit(None).cast(pl.String).alias("instrument"),
            )

        frames.append(price_df.select(STOCK_HISTORY_COLUMNS).sort(["barrid", "date"]))

    combined = pl.concat(frames)

    for year in tqdm(years, desc="Writing to S3"):
        year_data = combined.filter(pl.col("date").dt.year().eq(year))
        s3.write_parquet(
            bucket_name="barra-stock-history",
            file_name=f"stock_history_{year}.parquet",
            file_data=year_data,
        )


def stock_history_daily_flow() -> None:
    today = dt.date.today()
    start = dt.date(today.year - 1, 1, 1)
    stock_history_backfill_flow(start, today)


if __name__ == '__main__':
    start = dt.date(1995, 1, 1)
    end = dt.date(2026, 12, 31)
    stock_history_backfill_flow(start, end)
