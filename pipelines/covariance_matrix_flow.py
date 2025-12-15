import polars as pl

import pipelines.utils.s3
from pipelines.barrids import get_barrids
from pipelines.covariance_matrix import construct_covariance_matrix
from pipelines.covariances import get_factor_covariances
from pipelines.exposures import get_etf_exposures, get_stock_exposures
from pipelines.specific_risk import get_etf_specific_risk, get_stock_specific_risk
from pipelines.tickers import get_tickers
from pipelines.utils import get_last_market_date


def covariance_matrix_daily_flow() -> None:
    date_ = get_last_market_date()[0]

    # 1. Get barrids and tickers
    tickers_df = get_tickers(date_)

    barrids_df = (
        get_barrids(date_)
        .join(tickers_df, on="barrid", how="left")
        .filter(pl.col("ticker").is_not_null())
        .select("barrid", "ticker")
        .sort("barrid")
    )

    barrids = barrids_df["barrid"].to_list()
    tickers = barrids_df["ticker"].to_list()
    ticker_mapping = {barrid: ticker for barrid, ticker in zip(barrids, tickers)}

    # 2. Get exposures
    stock_exposures = get_stock_exposures(date_, barrids)
    etf_exposures = get_etf_exposures(date_, barrids)

    exposures: pl.DataFrame = pl.concat([stock_exposures, etf_exposures])

    # 3. Get covariances
    covariances = get_factor_covariances(date_)

    # 4. Get specific risk
    stock_specific_risk = get_stock_specific_risk(date_, barrids)
    etf_specific_risk = get_etf_specific_risk(date_, barrids)
    specific_risk: pl.DataFrame = pl.concat([stock_specific_risk, etf_specific_risk])

    # 5. Construct covariance matrix
    covariance_matrix = construct_covariance_matrix(
        exposures, covariances, specific_risk
    )

    # 6. Re-key covariance matrix
    covariance_matrix_re_keyed = (
        covariance_matrix.rename(ticker_mapping, strict=False)
        .with_columns(pl.col("barrid").replace(ticker_mapping))
        .rename({"barrid": "ticker"})
    )

    tickers = covariance_matrix_re_keyed["ticker"].unique().sort().to_list()

    covariance_matrix_clean = covariance_matrix_re_keyed.select(
        pl.lit(date_).alias("date"), "ticker", *sorted(tickers)
    ).sort("ticker")

    # 7. Upload to s3
    utils.s3.write_parquet(
        bucket_name="barra-covariance-matrices",
        file_name="latest.parquet",
        file_data=covariance_matrix_clean,
    )
        # upload factor exposures to S3
    utils.s3.write_parquet(
        bucket_name="barra-factor-exposures",
        file_name="latest.parquet",
        file_data=exposures,
    )
