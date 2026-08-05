from pipelines.covariance_matrix_flow import covariance_matrix_daily_flow
from pipelines.return_factors_flow import return_factors_daily_flow
from pipelines.historical_data import historical_data_flow


def covariance_matrix_pipeline() -> None:
    covariance_matrix_daily_flow()

def return_factors_pipeline() -> None:
    return_factors_daily_flow()

def historical_data_pipeline(start_date) -> None:
    historical_data_flow(start_date)
