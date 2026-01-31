import io
import zipfile
import datetime as dt

import polars as pl

from pipelines.utils import s3
from pipelines.utils import get_last_market_date
from pipelines.variables import ROOT


def _clean_return_factors(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename({'!Factor': 'factor', 'DlyReturn': 'dly_return', 'DataDate': 'date'})
        .with_columns(
            pl.col('dly_return').cast(pl.Utf8).str.strip_chars().cast(pl.Float64),
            pl.col('date').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d'),
        )
        .select(pl.col('date'), pl.col('factor'), pl.col('dly_return'))
    )


def return_factors_daily_flow() -> None:
    date_ = get_last_market_date()[0]

    date_str_1 = date_.strftime('%y%m%d')
    date_str_2 = date_.strftime('%Y%m%d')

    # try bime first, then us
    zip_paths = [
        f"{ROOT}/bime/SMD_USSLOWL_100_{date_str_1}.zip",
        f"{ROOT}/us/SMD_USSLOWL_100_{date_str_1}.zip",
    ]

    file_name = f'USSLOWL_100_DlyFacRet.{date_str_2}'

    zf = None
    for zp in zip_paths:
        try:
            zf = zipfile.ZipFile(zp, 'r')
            break
        except FileNotFoundError:
            zf = None

    if zf is None:
        raise FileNotFoundError(f'Could not find SMD zip for date {date_} in paths: {zip_paths}')

    with zf:
        raw = io.BytesIO(zf.read(file_name))

        df = pl.read_csv(raw, skip_rows=2, separator='|')

    cleaned = _clean_return_factors(df)

    # upload to S3
    s3.write_parquet(
        bucket_name='barra-factor-returns',
        file_name=f'latest_return_factors.parquet',
        file_data=cleaned,
    )
