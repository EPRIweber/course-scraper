# Course Scraper

This project provides an asynchronous pipeline for collecting university course catalog data.  
The system crawls catalog sites, generates a scraping schema, extracts course data and
optionally classifies courses against a taxonomy before persisting everything to a SQL Server
backend.

## Features

- **Configurable sources** via `configs/sources.yaml`
- **Asynchronous crawler** to discover course pages
- **JSON/CSS-based scraping** driven by generated schemas
- **Optional classification** using large language models
- **Pluggable storage** with a SQL Server implementation

## Installation

1. Create and activate a Python 3.11+ environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Provide database credentials as environment variables:
   `DB_SERVER`, `DB_NAME`, `DB_USER`, `DB_PASS`.

## Running

Launch a scraping run by executing `main.py`:

```bash
python src/main.py
```

The script pulls enabled sources from the database, crawls each site, scrapes
course records and writes them back to storage.  Logs are written to the
console and the `logs` table in the database.

## Pipeline Overview

```
 SourceConfig -> Crawler -> Schema Manager -> Scraper -> Storage
```

1. **Crawler** collects course page URLs respecting the configured depth and filters.
2. **Schema Manager** derives a CSS/JSON schema from a sample catalog page.
3. **Scraper** applies the schema to each URL and returns structured course data.
4. **Storage** persists URLs, schemas, and extracted records in SQL Server.
5. **Classifier** (optional) labels courses using a taxonomy and saves the results.

## Repository Layout

- `src/` – application code
- `configs/` – YAML configuration files
- `requirements.txt` – Python dependencies

Use the provided modules as a reference when adding new sources or extending the pipeline.

## Automatic Config Generation (STILL IN DEVELOPMENT)

The `config_generator.py` script can automatically create minimal entries in
`configs/sources.yaml` for new schools. It performs a Google Programmable Search
Engine query for the school's course catalog, scans candidate pages for course
links and then appends a new `SourceConfig` section.

Or generate configs for many schools from a CSV file:

```bash
python src/config_generator.py
```

The generator requires `GOOGLE_API_KEY` and `GOOGLE_CX` environment variables
for accessing Google's Programmable Search Engine.
