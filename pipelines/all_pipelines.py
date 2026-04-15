import datetime as dt

from pipelines.covariance_matrix_flow import covariance_matrix_daily_flow
from pipelines.return_factors_flow import return_factors_daily_flow
from pipelines.stock_history_flow import stock_history_daily_flow, stock_history_backfill_flow


def covariance_matrix_pipeline() -> None:
    covariance_matrix_daily_flow()

def return_factors_pipeline() -> None:
    return_factors_daily_flow()

def stock_history_daily_pipeline() -> None:
    stock_history_daily_flow()

def stock_history_backfill_pipeline(start_date, end_date) -> None:
    stock_history_backfill_flow(start_date, end_date)
