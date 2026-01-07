#!/usr/bin/env python3
"""Main CLI entry point for WebETL."""

import logging
from pathlib import Path
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
# Project Setup Commands
# ============================================================================

@cli.command()
@click.option("--name", "-n", default="sources.yml", help="Name of the sources config file")
@click.option("--force", "-f", is_flag=True, help="Overwrite existing files")
def init(name, force):
    """Initialize a new WebETL project with directory structure and template config."""
    project_root = Path.cwd()

    click.echo("Initializing WebETL project...")

    # Create data directory structure
    directories = [
        project_root / "data" / "jobs",
        project_root / "data" / "raw",
        project_root / "data" / "silver",
        project_root / "data" / "gold",
    ]

    for directory in directories:
        if directory.exists():
            click.echo(f"  ✓ {directory.relative_to(project_root)}/ already exists")
        else:
            directory.mkdir(parents=True, exist_ok=True)
            click.echo(f"  + Created {directory.relative_to(project_root)}/")

    # Create template sources.yml file
    sources_file = project_root / name

    if sources_file.exists() and not force:
        click.echo(f"  ! {name} already exists (use --force to overwrite)")
    else:
        template = '''source:
  # Example: Extract from RSS feed
  - name: example_rss
    start: https://example.com/feed.xml
    extract:
      ftype: rss
      fields:
        - name: title
          selector: title
        - name: link
          selector: link
        - name: description
          selector: description

  # Example: Navigate website and extract content
  - name: example_website
    start: https://example.com/articles
    navigate:
      - step: "article links"
        ftype: html
        selector: //a[@class='article-link']/@href
    extract:
      ftype: html
      fields:
        - name: title
          selector: //h1/text()
        - name: content
          selector: //article//text()
    # Optional: Transform with LLM
    transform:
      LLM:
        - name: summarize
          input: [content]
          output: summary
          model: gpt-4
          prompt: "Summarize this article in 2-3 sentences."
    # Optional: Generate output files
    load:
      xml:
        fields:
          - field: summary
            name: description
          - field: title
            name: title
      json:
        fields:
          - field: title
            name: title
          - field: summary
            name: summary
'''
        with open(sources_file, 'w') as f:
            f.write(template)
        click.echo(f"  + Created {name}")

    # Create .env template
    env_file = project_root / ".env.example"
    if env_file.exists() and not force:
        click.echo(f"  ✓ .env.example already exists")
    else:
        env_template = '''# OpenAI API Key (required for LLM transformations)
OPENAI_API_KEY=your_api_key_here
'''
        with open(env_file, 'w') as f:
            f.write(env_template)
        click.echo(f"  + Created .env.example")

    # Create .gitignore
    gitignore_file = project_root / ".gitignore"
    if gitignore_file.exists() and not force:
        click.echo(f"  ✓ .gitignore already exists")
    else:
        gitignore_content = '''# WebETL data and environment
data/
.env

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
.venv
'''
        with open(gitignore_file, 'w') as f:
            f.write(gitignore_content)
        click.echo(f"  + Created .gitignore")

    click.echo("\n✓ WebETL project initialized!")
    click.echo("\nNext steps:")
    click.echo(f"  1. Edit {name} to define your data sources")
    click.echo("  2. Copy .env.example to .env and add your OPENAI_API_KEY (if using LLM transforms)")
    click.echo(f"  3. Run: webetl run {name}")


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
    click.echo(f"Running full ETL pipeline for {config_file}")
    if source:
        click.echo(f"  Source: {source}")

    try:
        # Extract
        click.echo("\n[1/3] Extracting data...")
        dispatcher = Dispatcher(path=config_file, source_name=source)
        dispatcher.execute_jobs()
        dispatcher.save_results()
        click.echo("  ✓ Extraction complete")

        # Transform
        click.echo("\n[2/3] Transforming data...")
        transform = Transform()
        click.echo(f"  Transform/Load date: {transform.dm.data_date}")
        transform.process_jobs()
        click.echo("  ✓ Transformation complete")

        # Load
        click.echo("\n[3/3] Loading data...")
        load = Load()
        load.process_jobs()
        click.echo("  ✓ Loading complete")

        click.echo("\n✓ ETL pipeline completed successfully")
    except ValueError as e:
        click.echo(f"\n✗ Error: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option("--source", "-s", help="Specific source name to extract")
def extract(config_file, source):
    """Extract data from sources defined in config file."""
    click.echo(f"Extracting data from {config_file}")
    if source:
        click.echo(f"  Source: {source}")

    try:
        dispatcher = Dispatcher(path=config_file, source_name=source)
        dispatcher.execute_jobs()
        dispatcher.save_results()
    except ValueError as e:
        click.echo(f"✗ Error: {e}", err=True)
        raise click.Abort()

    click.echo("✓ Extraction complete")


@cli.command()
@click.argument("date", required=False, default=None)
def transform(date):
    """Transform extracted data using LLM.

    DATE: Optional date string (YYYY-MM-DD). Defaults to today if not specified.

    Examples:
        webetl transform              # Uses today's date
        webetl transform 2024-01-15   # Uses specified date
    """
    transform_instance = Transform(data_date=date)

    click.echo(f"Transforming data for {transform_instance.dm.data_date}")
    transform_instance.process_jobs()

    click.echo("✓ Transformation complete")


@cli.command()
@click.argument("date", required=False, default=None)
def load(date):
    """Load transformed data into final format.

    DATE: Optional date string (YYYY-MM-DD). Defaults to today if not specified.

    Examples:
        webetl load              # Uses today's date
        webetl load 2024-01-15   # Uses specified date
    """
    load_instance = Load(data_date=date)

    click.echo(f"Loading data for {load_instance.dm.data_date}")
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
@click.argument("date", required=False, default=None)
def reset_tracking(date):
    """Reset fetch tracking for a specific date.

    DATE: Optional date string (YYYY-MM-DD). Defaults to today if not specified.

    This will allow URLs fetched on the specified date to be re-fetched,
    useful for re-running failed extractions.

    Examples:
        webetl reset-tracking              # Reset today's tracking
        webetl reset-tracking 2024-01-15   # Reset tracking for specific date
    """
    tracker = RunTracker()

    # Display which date is being reset
    display_date = date if date else datetime.now().strftime("%Y-%m-%d")

    # Confirm action
    if not click.confirm(f"Reset fetch tracking for {display_date}? This will allow re-fetching URLs from that date."):
        click.echo("Cancelled.")
        return

    count = tracker.reset_tracking_by_date(date)
    click.echo(f"✓ Reset tracking for {display_date}: {count} URLs can now be re-fetched")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
