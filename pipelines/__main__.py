import datetime as dt

import click

from pipelines.all_pipelines import (
    covariance_matrix_pipeline,
    return_factors_pipeline,
    stock_history_daily_pipeline,
    stock_history_backfill_pipeline,
)

@click.group()
def cli():
    """Main CLI entrypoint."""
    pass


@cli.command()
def covariance_matrix():
    click.echo(f"Running covariance matrix daily flow: {dt.date.today()}.")
    covariance_matrix_pipeline()
    click.echo("Flow completed successfully!")


@cli.command()
def return_factors():
    click.echo(f"Running return_factors_pipeline: {dt.date.today()}.")
    return_factors_pipeline()
    click.echo("Flow completed successfully!")


@cli.command()
def stock_history_daily():
    click.echo(f"Running stock history daily flow: {dt.date.today()}.")
    stock_history_daily_pipeline()
    click.echo("Flow completed successfully!")


@cli.command()
@click.option("--start", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(dt.date(1995, 1, 1)), show_default=True)
@click.option("--end", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(dt.date.today()), show_default=True)
def stock_history_backfill(start, end):
    click.echo(f"Running stock history backfill from {start} to {end}.")
    stock_history_backfill_pipeline(start, end)
    click.echo("Flow completed successfully!")


if __name__ == "__main__":
    cli()
