import numpy as np
import polars as pl


def construct_covariance_matrix(
    exposures: pl.DataFrame, covariances: pl.DataFrame, specific_risk: pl.DataFrame
) -> pl.DataFrame:
    barrids = exposures["barrid"].to_list()

    exposures = (
        exposures.drop("date", "barrid")
        .with_columns(
            pl.all().fill_null(0)  # fill null factor exposures
        )
        .to_numpy()
    )

    covariances = (
        covariances.drop("date", "factor_1")
        .with_columns(pl.all().truediv(100**2))
        .to_numpy()
    )

    mask = np.isnan(covariances)
    covariances[mask] = covariances.T[mask]  # fill symetric values

    specific_risk = (
        specific_risk.drop("barrid")
        .with_columns(pl.all().truediv(100))
        .to_numpy()
        .flatten()
    )

    # Square the specific risk to get specific variance
    specific_variance = np.diag(specific_risk**2)

    covariance_matrix = exposures @ covariances @ exposures.T + specific_variance

    covariance_matrix_df = (
        pl.from_numpy(covariance_matrix, barrids)
        .with_columns(pl.Series(barrids).alias("barrid"))
        .select("barrid", *barrids)
    )

    return covariance_matrix_df
