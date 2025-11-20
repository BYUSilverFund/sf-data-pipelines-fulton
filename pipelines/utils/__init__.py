from datetime import date

import exchange_calendars as xcals
import polars as pl


def get_last_market_date(
    current_date: date | None = None, n_days: int = 1
) -> list[date]:
    current_date = current_date or date.today()

    df = (
        pl.from_pandas(xcals.get_calendar("XNYS").schedule)
        # Cast date types
        .with_columns(pl.col("close").cast(pl.Date).alias("date"))
        # Get previous date
        .with_columns(pl.col("date").shift(1).alias("previous_date"))
    )

    market_dates = df["date"].to_list()

    # If today is not a market date, use the next closest.
    if current_date not in market_dates:
        current_date = next(d for d in market_dates if d > current_date)

    df = (
        df
        # Filter
        .filter(pl.col("date").le(current_date))
        # Sort
        .sort("date")["previous_date"]
        # Get last previous date
        .tail(n_days)
        .to_list()
    )

    return df
