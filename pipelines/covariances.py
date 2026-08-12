import datetime as dt
import io
import zipfile

import polars as pl

from pipelines.utils.variables import FACTORS, DATA_ROOT
from pipelines.utils.barra_datasets import barra_covariances

covariances_column_mapping = {
    "!Factor1": "factor_1",
    "Factor2": "factor_2",
    "VarCovar": "covariance",
    "DataDate": "date",
}


def _clean_covariances(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.rename(covariances_column_mapping)
        .filter(pl.col("factor_1").ne("[End of File]"))
        .with_columns(
            pl.col("date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d"),
            pl.col("covariance").str.strip_chars().cast(pl.Float64),
        )
        .pivot(index=["date", "factor_1"], on="factor_2", values="covariance")
        .sort("factor_1")
        .select("date", "factor_1", *FACTORS)
    )


def get_factor_covariances(date_: dt.date):

    zip_folder_path = barra_covariances.daily_zip_folder_path(date_)
    file_name = barra_covariances.file_name(date_)

    with zipfile.ZipFile(zip_folder_path, "r") as zip_folder:
        df = pl.read_csv(
            io.BytesIO(zip_folder.read(file_name)),
            skip_rows=2,
            separator="|",
        )

        return _clean_covariances(df)
