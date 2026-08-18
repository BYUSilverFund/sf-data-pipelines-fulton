import datetime as dt

import click

from pipelines.all_pipelines import (
    covariance_matrix_pipeline,
    return_factors_pipeline,
    historical_data_pipeline,
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
@click.option(
    '--since',
    type=click.DateTime(formats=['%Y-%m-%d']),
    default=str(dt.date.today() - dt.timedelta(days=1)),
    help='Start date in YYYY-MM-DD format. Defaults to yesterday.'
)
def historical_data(since):
    click.echo(f"Running historical_data_pipeline: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")

    start_date = since.date() if isinstance(since, dt.datetime) else since
    historical_data_pipeline(start_date)
    click.echo("Flow completed successfully!")
    

if __name__ == "__main__":
    cli()
