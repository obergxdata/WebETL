<p align="center">
  <img src="assets/logo.svg" alt="WebETL Logo" width="400">
</p>

# WebETL

A flexible web content extraction, transformation, and loading (ETL) pipeline for navigating websites, extracting data, transforming it with LLMs, and generating structured output.

## Features

- **Multi-step Navigation**: Navigate through websites, RSS feeds, and JSON APIs in multiple steps to discover content
- **Flexible Extraction**: Extract data from HTML, RSS feeds, JSON APIs, and PDF documents
- **LLM Transformation**: Transform and analyze extracted content using large language models (OpenAI)
- **Multiple Output Formats**: Save results as JSON or generate RSS feeds
- **Smart URL Tracking**: Automatic tracking of fetched URLs to prevent duplicate processing
  - Each URL can only be fetched once per source (persistent across runs)
  - Stored in SQLite database for reliability
  - Use `--no-track` to bypass or `reset-tracking` to clear history
- **Concurrent Processing**: Multi-process extraction for efficient and stable data collection

## Installation

### From Source (Development)

```bash
# Clone the repository
git clone https://github.com/obergxdata/WebETL
cd WebETL

# Install in editable mode
make install-dev

# Or using pip directly
pip install -e ".[dev]"
```

### From PyPI

```bash
pip install xwebetl
```

## Quick Start

### Initialize a New Project

```bash
# Create a new WebETL project with directory structure and template config
webetl init

# Or specify a custom config file name
webetl init --name my_sources.yml
```

This creates:
- `data/` directory structure (raw/, silver/, gold/)
- `sources.yml` template configuration file
- `.env.example` for environment variables
- `.gitignore` for version control

### Using the CLI

```bash
# Run full ETL pipeline from a source configuration
webetl run sources.yml                                  # All sources, today's date
webetl run sources.yml -s my_source                     # Specific source
webetl run sources.yml -d 2024-01-15                    # Specific date
webetl run sources.yml -s my_source -d 2024-01-15       # Source and date
webetl run sources.yml --no-track                       # Disable URL tracking

# Run individual stages
webetl extract sources.yml                              # Extract all sources
webetl extract sources.yml -s my_source                 # Extract specific source
webetl extract sources.yml --no-track                   # Extract without tracking

webetl transform sources.yml                            # Transform all sources, today's date
webetl transform sources.yml -s my_source               # Transform specific source
webetl transform sources.yml -d 2024-01-15              # Transform specific date

webetl load sources.yml                                 # Load all sources, today's date
webetl load sources.yml -s my_source                    # Load specific source
webetl load sources.yml -d 2024-01-15                   # Load specific date

# Manage fetch history
webetl fetches --limit 50                               # Show recent fetches
webetl reset-tracking                                   # Reset today's tracking
webetl reset-tracking 2024-01-15                        # Reset specific date
```

### Using the Python API

```python
from xwebetl import Dispatcher, Transform, Load

# Extract data from sources
dispatcher = Dispatcher(path="sources.yml", source_name="my_source")
dispatcher.execute_jobs()
dispatcher.save_results()

# Extract without tracking (allows re-fetching)
dispatcher = Dispatcher(path="sources.yml", source_name="my_source", no_track=True)
dispatcher.execute_jobs()
dispatcher.save_results()

# Transform with LLM
transform = Transform(path="sources.yml")  # All sources, today's date
transform = Transform(path="sources.yml", source_name="my_source")  # Specific source
transform = Transform(path="sources.yml", data_date="2024-01-01")  # Specific date
transform = Transform(path="sources.yml", source_name="my_source", data_date="2024-01-01")
transform.process_jobs()

# Load into final format
load = Load(path="sources.yml")  # All sources, today's date
load = Load(path="sources.yml", source_name="my_source")  # Specific source
load = Load(path="sources.yml", data_date="2024-01-01")  # Specific date
load.process_jobs()
```

Or create a simple automation script:

```python
#!/usr/bin/env python3
"""Daily ETL automation script."""
from xwebetl import Dispatcher, Transform, Load
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_etl(config_file: str, source_name: str = None):
    """Run full ETL pipeline for a source."""
    # Extract
    logger.info(f"Extracting data from {source_name or 'all sources'}...")
    dispatcher = Dispatcher(path=config_file, source_name=source_name)
    dispatcher.execute_jobs()
    dispatcher.save_results()

    # Transform (automatically uses today's date)
    logger.info("Transforming data...")
    transform = Transform(path=config_file, source_name=source_name)
    transform.process_jobs()

    # Load
    logger.info("Loading data...")
    load = Load(path=config_file, source_name=source_name)
    load.process_jobs()

    logger.info("ETL pipeline completed!")

if __name__ == "__main__":
    run_etl("sources.yml", source_name="my_source")
```

## Configuration

WebETL uses YAML configuration files to define data sources. Here's an example:

```yaml
source:
  - name: tech_blog
    start: https://blog.example.com/feed.xml
    no_track: true  # Optional: disable URL tracking for this source (default: false)

    # Multi-step navigation
    navigate:
      - ftype: rss
        selector: link

    # Extract fields from final pages
    extract:
      ftype: html
      fields:
        - name: title
          selector: //h1/text()
        - name: content
          selector: //article//text()
        - name: author
          selector: //meta[@name='author']/@content

    # Transform with LLM (optional)
    transform:
      LLM:
      - name: check_relevance
        input: [content]
        output: is_relevant
        model: gpt-4
        prompt: "Is this article about technology? Reply with YES or NO"
      - name: summarize
        input: [content]
        output: summary
        model: gpt-4
        prompt: "Summarize this article in 2-3 sentences"
        skip_if:
          field: is_relevant
          not_equals: "YES"

    # Output format (optional)
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
```

### Source Configuration Options

Each source in your configuration supports these options:

- **`name`** (required): Unique identifier for this source
- **`start`** (required): Starting URL for the extraction
- **`no_track`** (optional): Set to `true` to disable URL tracking for this source (default: `false`)
  - When enabled, URLs will always be re-fetched, ignoring fetch history
  - Useful for sources that need to be checked on every run
  - Can be combined with CLI `--no-track` flag (either setting will disable tracking)
- **`navigate`** (optional): Multi-step navigation rules
- **`extract`** (required): Fields to extract from final pages
- **`transform`** (optional): LLM transformation rules
- **`load`** (optional): Output format configuration

## Architecture

WebETL follows a classic ETL pattern with four main stages:

### 1. Source Definition
Define your data sources in YAML with navigation steps, extraction rules, transformations, and output formats.

### 2. Extract
- Navigate through websites using CSS/XPath selectors
- Extract data from HTML, RSS, JSON, and PDF sources
- Auto-resolve content type with `ftype: mixed`
- JSON extraction using dot notation (e.g., `items.title`)
- **URL Tracking**: Automatically tracks fetched URLs to prevent duplicates
  - Each URL can only be fetched once per source
  - Tracking data is stored in SQLite database (`data/runs.db`)
  - Use `--no-track` flag to bypass tracking and re-fetch URLs
  - Use `webetl reset-tracking` to clear tracking and allow re-fetching
- Concurrent processing for performance
- Save raw data to JSON

### 3. Transform
- Process extracted data with LLM prompts
- Chain multiple transformation steps
- Configurable models and prompts
- Save transformed data to JSON

### 4. Load
- Generate RSS feeds from transformed data
- Output JSON files
- Customizable output formats

## CLI Reference

### Project Setup

```bash
webetl init                          # Initialize new project in current directory
webetl init --name custom.yml        # Initialize with custom config filename
webetl init --force                  # Overwrite existing files
```

### ETL Commands

```bash
# Run full pipeline
webetl run <config.yml>                          # All sources, today's date
webetl run <config.yml> -s <source>              # Specific source
webetl run <config.yml> -d YYYY-MM-DD            # Specific date for transform/load
webetl run <config.yml> -s <source> -d YYYY-MM-DD  # Both options
webetl run <config.yml> --no-track               # Disable URL tracking

# Extract only
webetl extract <config.yml>                      # All sources
webetl extract <config.yml> -s <source>          # Specific source
webetl extract <config.yml> --no-track           # Without URL tracking

# Transform only
webetl transform <config.yml>                    # All sources, today's date
webetl transform <config.yml> -s <source>        # Specific source
webetl transform <config.yml> -d YYYY-MM-DD      # Specific date

# Load only
webetl load <config.yml>                         # All sources, today's date
webetl load <config.yml> -s <source>             # Specific source
webetl load <config.yml> -d YYYY-MM-DD           # Specific date
```

**Note:** The `run` command extracts data and then processes it. Use `-d` to specify a date for transform/load stages.

**URL Tracking:** By default, WebETL tracks fetched URLs to prevent re-processing. Use `--no-track` to disable this and re-fetch all URLs (useful for testing or forcing updates).

### Fetch Tracking Management

```bash
webetl fetches [--limit N]           # Show recently fetched URLs
webetl reset-tracking [YYYY-MM-DD]   # Reset tracking for date (allows re-fetching)
```

**Understanding URL Tracking:**

WebETL automatically tracks every URL it fetches to prevent duplicate processing. This tracking is:
- **Per-source**: Each source maintains its own fetch history
- **Persistent**: Stored in SQLite database (`data/runs.db`), survives between runs
- **Permanent**: Once fetched, a URL won't be re-fetched unless you explicitly reset tracking

**Common scenarios:**
- **Daily runs**: URLs are only fetched once, ever (unless content location changes)
- **Testing/debugging**: Use `--no-track` to bypass tracking temporarily
- **Force re-fetch**: Use `webetl reset-tracking` to clear history for specific dates
- **Clean slate**: Delete `data/runs.db` to clear all tracking history

### Common Options

```bash
--source, -s <name>                  # Process specific source only
--date, -d YYYY-MM-DD                # Process specific date (for transform/load)
--no-track                           # Disable URL tracking (for run/extract commands)
--help                               # Show help
--version                            # Show version
```

## Python API Reference

### Extract Module

```python
from xwebetl import Dispatcher
from xwebetl.extract.dispatch import RunTracker

# Extract data from sources
dispatcher = Dispatcher(path="sources.yml", source_name="my_source")
dispatcher.execute_jobs()
dispatcher.save_results()

# Extract without tracking (re-fetch all URLs)
dispatcher = Dispatcher(path="sources.yml", source_name="my_source", no_track=True)
dispatcher.execute_jobs()
dispatcher.save_results()

# Manage fetch history
tracker = RunTracker()
tracker.add_url(url="https://example.com", source_name="my_source")
tracker.has_been_fetched(url="https://example.com")
tracker.get_latest_fetches(limit=100)

# Advanced: Use Navigate for custom navigation logic
from xwebetl.extract.dispatch import Navigate
nav = Navigate(path="sources.yml", source_name="my_source")
nav.start()  # Populates nav.jobs with URLs to extract
# Access nav.jobs for custom processing
```

### Transform Module

```python
from xwebetl import Transform

# Process all sources for today's date
transform = Transform(path="sources.yml")
transform.process_jobs()

# Process specific source
transform = Transform(path="sources.yml", source_name="my_source")
transform.process_jobs()

# Process specific date
transform = Transform(path="sources.yml", data_date="2024-01-01")
transform.process_jobs()

# Both source and date
transform = Transform(path="sources.yml", source_name="my_source", data_date="2024-01-01")
transform.process_jobs()
```

### Load Module

```python
from xwebetl import Load

# Process all sources for today's date
load = Load(path="sources.yml")
load.process_jobs()

# Process specific source
load = Load(path="sources.yml", source_name="my_source")
load.process_jobs()

# Process specific date
load = Load(path="sources.yml", data_date="2024-01-01")
load.process_jobs()
```

### Source Management

```python
from xwebetl import Source

# Load and generate jobs from config
source = Source(path="sources.yml", source_name="my_source")
jobs = source.gen_jobs()  # Returns list[Job]
```

## Data Storage

WebETL organizes data in a structured directory:

```
data/
├── raw/
│   └── YYYY-MM-DD/       # Extracted raw data (JSON) by date
├── silver/
│   └── YYYY-MM-DD/       # Transformed data (JSON) by date
├── gold/
│   └── YYYY-MM-DD/       # Final output (RSS/JSON) by date
└── runs.db               # SQLite database for fetch tracking
```

**Note:** Job configurations are loaded directly from the YAML config file at runtime, not stored as separate files.

## Development

### Running Tests

```bash
make test
# Or
pytest
```

### Starting Test Server

```bash
make test-server        # Start on port 8888
make test-server-kill   # Stop test server
```

### Cleaning Build Artifacts

```bash
make clean
```

### Building Package

```bash
make build
```

## Configuration Examples

### Simple RSS Feed Extraction

```yaml
source:
  - name: news_feed
    start: https://news.example.com/rss
    extract:
      ftype: rss
      fields:
        - name: title
          selector: title
        - name: link
          selector: link
```

### Multi-Step Website Navigation

```yaml
source:
  - name: product_catalog
    start: https://shop.example.com/catalog
    navigate:
      - ftype: html
        selector: //a[@class='category']/@href
      - ftype: html
        selector: //a[@class='product']/@href
        must_contain: ["/product/"]  # OR logic: URL must contain "/product/"
    extract:
      ftype: html
      fields:
        - name: name
          selector: //h1[@class='product-name']/text()
        - name: price
          selector: //span[@class='price']/text()
```

### URL Filtering with must_contain and must_contain_all

Filter extracted URLs using pattern matching:

```yaml
source:
  - name: filtered_documents
    start: https://example.com/documents
    navigate:
      - ftype: html
        selector: //a/@href
        must_contain: [".pdf", ".doc"]        # OR logic: must have .pdf OR .doc
        must_contain_all: ["report", "2024"]  # AND logic: must have "report" AND "2024"
    extract:
      ftype: pdf
      fields: []
```

**Filter Logic:**
- `must_contain`: OR logic - URL must contain **at least one** of the specified strings
- `must_contain_all`: AND logic - URL must contain **all** of the specified strings
- When both are present: URL must satisfy **both** conditions

**Example matches for the above config:**
- `https://example.com/annual-report-2024.pdf` ✓ (has .pdf + report + 2024)
- `https://example.com/monthly-report-2024.doc` ✓ (has .doc + report + 2024)
- `https://example.com/report.pdf` ✗ (missing "2024")
- `https://example.com/2024.pdf` ✗ (missing "report")
- `https://example.com/report-2024.html` ✗ (missing .pdf or .doc)

### PDF Content Extraction

```yaml
source:
  - name: research_papers
    start: https://papers.example.com/latest.html
    navigate:
      - ftype: html
        selector: //a[contains(@href, '.pdf')]/@href
    extract:
      ftype: pdf
      fields: []  # Automatically extracts full text content
```

### JSON API Extraction

Extract data from JSON APIs using dot notation to navigate the JSON structure:

```yaml
source:
  # Direct JSON extraction (no navigation)
  - name: json_api
    start: https://api.example.com/articles.json
    extract:
      ftype: json
      fields:
        - name: title
          selector: items.title        # Extract all titles from items array
        - name: description
          selector: items.description   # Extract all descriptions
        - name: link
          selector: items.link          # Extract all links

  # Navigate from JSON to HTML pages
  - name: json_to_html
    start: https://api.example.com/articles.json
    navigate:
      - ftype: json
        selector: items.link            # Extract links from JSON
        max_items: 10                   # Optional: Limit to first 10 items
    extract:
      ftype: html
      fields:
        - name: title
          selector: //h1/text()
        - name: content
          selector: //article/text()
```

**JSON Selector Syntax:**
- Use dot notation to navigate: `parent.child.field`
- Automatically extracts from arrays: `items.title` extracts title from each item
- Works with nested objects: `data.results.name`

**Example JSON Structure:**
```json
{
  "items": [
    {
      "title": "Article 1",
      "description": "First article",
      "link": "/article-1.html"
    },
    {
      "title": "Article 2",
      "description": "Second article",
      "link": "/article-2.html"
    }
  ]
}
```

Using `selector: items.title` would extract: `["Article 1", "Article 2"]`

### Auto-Resolving Content Type (Mixed)

Use `ftype: mixed` to automatically detect whether content is HTML, RSS, JSON, or PDF:

```yaml
source:
  - name: auto_detect
    start: https://example.com/feed
    navigate:
      - ftype: mixed  # Automatically detects content type
        selector: link
        must_contain: [html]
    extract:
      ftype: mixed  # Automatically detects content type
      fields:
        - name: title
          selector: title
        - name: content
          selector: description
```

### Multiple XPath Selectors

HTML selectors support multiple XPath expressions using the pipe character (`|`). The first matching XPath will be used:

```yaml
source:
  - name: fallback_selectors
    start: https://example.com
    navigate:
      - ftype: html
        # Try first XPath, if not found try second
        selector: //div[@id='article-body']/p[4]/a/@href|//div[@id='alt-body']/p[4]/a/@href
    extract:
      ftype: html
      fields:
        - name: title
          selector: //h1[@class='main-title']/text()|//h1[@class='alt-title']/text()
```

### Disabling URL Tracking Per Source

By default, WebETL tracks all fetched URLs to prevent duplicate processing. You can disable this per source using `no_track: true`:

```yaml
source:
  # Regular source with tracking enabled (default)
  - name: weekly_newsletter
    start: https://example.com/newsletter/feed.xml
    extract:
      ftype: rss
      fields:
        - name: title
          selector: title

  # Source that always re-fetches URLs (no tracking)
  - name: live_dashboard
    start: https://example.com/dashboard/feed.xml
    no_track: true  # Always fetch, ignore history
    extract:
      ftype: rss
      fields:
        - name: status
          selector: title
```

**When to use `no_track: true`:**
- Sources that update frequently and should be checked every run
- Live dashboards or status pages
- Sources where you always want fresh data regardless of previous fetches
- Testing and debugging specific sources

**Tracking behavior:**
- CLI flag `--no-track`: Disables tracking for ALL sources in the run
- Config `no_track: true`: Disables tracking ONLY for that specific source
- If either is set to `true`, tracking is disabled

## Requirements

- Python >= 3.11
- Dependencies:
  - lxml >= 4.9.0
  - requests >= 2.31.0
  - feedparser >= 6.0.0
  - PyYAML >= 6.0
  - openai >= 1.0.0
  - pypdfium2 >= 3.0.0
  - click >= 8.0.0

## Environment Variables

```bash
OPENAI_API_KEY=your_api_key_here  # Required for LLM transformations
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

- Issues: https://github.com/obergxdata/WebETL/issues
- Documentation: https://github.com/obergxdata/WebETL#readme
