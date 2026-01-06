#!/usr/bin/env python3
"""Main CLI entry point for WebETL."""

import sys
import logging
from datetime import datetime
import click
from extract.dispatch import RunTracker, Dispatcher
from transform.transform import Transform
from load.load import Load

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """WebETL - Web content extraction, transformation, and loading pipeline."""
    pass


# ============================================================================
# ETL Pipeline Commands
# ============================================================================

@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--source", "-s", help="Specific source name to process (processes all if not specified)")
def run(config_file, source):
    """Run full ETL pipeline: extract, transform, and load.

    Extract uses current time, transform and load use today's date.
    """
    # Date for transform/load (today)
    date = datetime.now().strftime("%Y-%m-%d")

    click.echo(f"Running full ETL pipeline for {config_file}")
    if source:
        click.echo(f"  Source: {source}")
    click.echo(f"  Transform/Load date: {date}")

    # Extract
    click.echo("\n[1/3] Extracting data...")
    dispatcher = Dispatcher(path=config_file, source_name=source)
    dispatcher.execute_jobs()
    dispatcher.save_results()
    click.echo("  ✓ Extraction complete")

    # Transform
    click.echo("\n[2/3] Transforming data...")
    transform = Transform(data_date=date)
    transform.process_jobs()
    click.echo("  ✓ Transformation complete")

    # Load
    click.echo("\n[3/3] Loading data...")
    load = Load(data_date=date)
    load.process_jobs()
    click.echo("  ✓ Loading complete")

    click.echo("\n✓ ETL pipeline completed successfully")


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--source", "-s", help="Specific source name to extract")
def extract(config_file, source):
    """Extract data from sources defined in config file."""
    click.echo(f"Extracting data from {config_file}")
    if source:
        click.echo(f"  Source: {source}")

    dispatcher = Dispatcher(path=config_file, source_name=source)
    dispatcher.execute_jobs()
    dispatcher.save_results()

    click.echo("✓ Extraction complete")


@cli.command()
@click.option("--date", "-d", help="Data date to transform (defaults to today, format: YYYY-MM-DD)")
def transform(date):
    """Transform extracted data using LLM."""
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")

    click.echo(f"Transforming data for {date}")

    transform_instance = Transform(data_date=date)
    transform_instance.process_jobs()

    click.echo("✓ Transformation complete")


@cli.command()
@click.option("--date", "-d", help="Data date to load (defaults to today, format: YYYY-MM-DD)")
def load(date):
    """Load transformed data into final format."""
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")

    click.echo(f"Loading data for {date}")

    load_instance = Load(data_date=date)
    load_instance.process_jobs()

    click.echo("✓ Loading complete")


# ============================================================================
# Fetch History Management Commands
# ============================================================================

@cli.command()
@click.option("--limit", "-l", default=100, help="Number of recent fetches to show")
def fetches(limit):
    """Show recently fetched URLs."""
    tracker = RunTracker()
    results = tracker.get_latest_fetches(limit=limit)

    if not results:
        click.echo("No fetched URLs found")
        return

    for url, source_name, fetch_datetime in results:
        click.echo(f"{fetch_datetime}\t{source_name}\t{url}")


@cli.command()
@click.argument("url")
def has_fetched(url):
    """Check if a URL has been fetched."""
    tracker = RunTracker()
    result = tracker.has_been_fetched(url)

    if result:
        click.echo("yes")
        sys.exit(0)
    else:
        click.echo("no")
        sys.exit(1)


@cli.command()
@click.argument("source")
@click.confirmation_option(prompt="Are you sure you want to delete all URLs for this source?")
def delete_source(source):
    """Delete all fetched URLs for a specific source."""
    tracker = RunTracker()
    count = tracker.delete_by_source(source)
    click.echo(f"Deleted {count} URLs for source '{source}'")


@cli.command()
@click.argument("url")
def delete_url(url):
    """Delete a specific URL from fetch history."""
    tracker = RunTracker()
    count = tracker.delete_by_url(url)

    if count > 0:
        click.echo(f"Deleted URL: {url}")
    else:
        click.echo(f"URL not found: {url}")


@cli.command()
@click.confirmation_option(prompt="⚠️  Are you sure you want to delete ALL fetched URLs?")
def delete_all():
    """Delete all fetched URLs from history."""
    tracker = RunTracker()
    count = tracker.delete_all()
    click.echo(f"Deleted {count} URLs")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
