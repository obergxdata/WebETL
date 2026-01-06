# WebETL

A flexible web content extraction, transformation, and loading (ETL) pipeline for navigating websites, extracting data, transforming it with LLMs, and generating structured output.

## Features

- **Multi-step Navigation**: Navigate through websites and RSS feeds in multiple steps to discover content
- **Flexible Extraction**: Extract data from HTML, RSS feeds, and PDF documents
- **LLM Transformation**: Transform and analyze extracted content using large language models (OpenAI)
- **Multiple Output Formats**: Save results as JSON or generate RSS feeds
- **Duplicate Prevention**: Automatic tracking of fetched URLs to prevent redundant processing
- **Concurrent Processing**: Multi-threaded extraction for efficient data collection

## Installation

### From Source (Development)

```bash
# Clone the repository
git clone https://github.com/yourusername/webetl
cd webetl

# Install in editable mode
make install-dev

# Or using pip directly
pip install -e ".[dev]"
```

### From PyPI (When Published)

```bash
pip install webetl
```

## Quick Start

### Using the CLI

```bash
# Run full ETL pipeline from a source configuration
webetl run sources.yml --source my_source

# Run individual stages
webetl extract sources.yml --source my_source  # Extract (no date needed)
webetl transform --date 2024-01-01             # Transform specific date data
webetl load --date 2024-01-01                  # Load specific date data

# Manage fetch history
webetl fetches --limit 50              # Show recent fetches
webetl has-fetched https://example.com # Check if URL was fetched
webetl delete-source my_source         # Delete fetch history for source
webetl delete-url https://example.com  # Delete specific URL
```

### Using the Python API

```python
from extract import Dispatcher
from transform import Transform
from load import Load

# Extract data from sources
dispatcher = Dispatcher(path="sources.yml", source_name="my_source")
dispatcher.execute_jobs()
dispatcher.save_results()

# Transform with LLM
transform = Transform(data_date="2024-01-01")
transform.process_jobs()

# Load into final format
load = Load(data_date="2024-01-01")
load.process_jobs()
```

## Configuration

WebETL uses YAML configuration files to define data sources. Here's an example:

```yaml
source:
  - name: tech_blog
    start: https://blog.example.com/feed.xml

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
      - input: [content]
        output: summary
        model: gpt-4
        prompt: "Summarize this article in 2-3 sentences"
      - input: [content]
        output: topics
        model: gpt-4
        prompt: "Extract 3-5 main topics from this article as a comma-separated list"

    # Output format (optional)
    load:
      format: rss
      title: Tech Blog Summaries
      description: Summarized articles from tech blog
      link: https://example.com
```

## Architecture

WebETL follows a classic ETL pattern with four main stages:

### 1. Source Definition
Define your data sources in YAML with navigation steps, extraction rules, transformations, and output formats.

### 2. Extract
- Navigate through websites using CSS/XPath selectors
- Extract data from HTML, RSS, and PDF sources
- Track fetched URLs to prevent duplicates
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

### ETL Commands

```bash
webetl run <config.yml>              # Run full ETL pipeline (uses today's date)
webetl extract <config.yml>          # Extract data from sources only
webetl transform --date YYYY-MM-DD   # Transform extracted data for specific date
webetl load --date YYYY-MM-DD        # Load transformed data for specific date
```

**Note:** The `run` command extracts data and then processes it using today's date for transform/load. If you want to process previously extracted data from a different date, use `transform` and `load` separately.

### History Management

```bash
webetl fetches [--limit N]           # Show recently fetched URLs
webetl has-fetched <url>             # Check if URL was fetched
webetl delete-source <name>          # Delete fetch history for source
webetl delete-url <url>              # Delete specific URL from history
webetl delete-all                    # Delete all fetch history (with confirmation)
```

### Options

```bash
webetl --help                        # Show help
webetl --version                     # Show version
```

## Python API Reference

### Extract Module

```python
from extract import Dispatcher, RunTracker

# Extract data from sources
dispatcher = Dispatcher(path="sources.yml", source_name="my_source")
dispatcher.execute_jobs()
dispatcher.save_results()

# Manage fetch history
tracker = RunTracker()
tracker.add_url(url="https://example.com", source_name="my_source")
tracker.has_been_fetched(url="https://example.com")
tracker.get_latest_fetches(limit=100)

# Advanced: Use Navigate for custom navigation logic
from extract import Navigate
nav = Navigate(path="sources.yml", source_name="my_source")
nav.start()  # Populates nav.jobs with URLs to extract
# Access nav.jobs for custom processing
```

### Transform Module

```python
from transform import Transform

# Process all jobs for a specific date
transform = Transform(data_date="2024-01-01")
transform.process_jobs()
```

### Load Module

```python
from load import Load

# Generate output files
load = Load(data_date="2024-01-01")
load.process_jobs()
```

### Source Management

```python
from source import Source

# Load and generate jobs from config
source = Source(path="sources.yml", source_name="my_source")
jobs = source.gen_jobs()
```

## Data Storage

WebETL organizes data in a structured directory:

```
data/
├── YYYY-MM-DD/           # Date-based data storage
│   ├── raw/              # Extracted raw data (JSON)
│   ├── jobs/             # Job configurations (pickle)
│   ├── silver/           # Transformed data (JSON)
│   └── gold/             # Final output (RSS/JSON)
└── runs.db               # SQLite database for fetch tracking
```

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
        must_contain: ["/product/"]
    extract:
      ftype: html
      fields:
        - name: name
          selector: //h1[@class='product-name']/text()
        - name: price
          selector: //span[@class='price']/text()
```

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

- Issues: https://github.com/yourusername/webetl/issues
- Documentation: https://github.com/yourusername/webetl#readme
