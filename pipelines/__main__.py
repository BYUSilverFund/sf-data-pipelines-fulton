import datetime as dt

import click

from pipelines.all_pipelines import covariance_matrix_pipeline


@click.group()
def cli():
    """Main CLI entrypoint."""
    pass


@cli.command()
def covariance_matrix():
    click.echo(f"Running covariance matrix daily flow: {dt.date.today()}.")
    covariance_matrix_pipeline()
    click.echo("Flow completed successfully!")


if __name__ == "__main__":
    cli()
