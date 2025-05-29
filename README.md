# GA4 Relational Data Pipeline

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![SQLite](https://img.shields.io/badge/SQLite-3.36%2B-green)
![GA4](https://img.shields.io/badge/Google%20Analytics-Data%20API-lightgrey)

Automated pipeline to fetch Google Analytics 4 data and store it in a normalized SQLite database.

## Features
- Automated daily GA4 data collection
- Relational database schema with date dimension table
- Three data categories: User, Content, and Site metrics
- Incremental updates (only fetches new data)
- CSV exports for each dataset
- Retry mechanism with exponential backoff
- Comprehensive logging


## Prerequisites

Python 3.8+
GA4 Property with Data API access
Service account credentials JSON file

## Installation

1) Clone repository:
``` bash
git clone https://github.com/bisongoscar/automated-funneling.git
cd ga4-data-pipeline
```

3) Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate    # Windows
```
4) Install dependencies:
```bash
pip install google-analytics-data pandas python-dotenv tenacity
  ```

## Configuration

1) Create .env file:
```bash
GA4_PROPERTY_ID=YOUR_PROPERTY_ID
GA4_CREDENTIALS_PATH=/path/to/service-account.json
```
3) Database path can be modified in the script's CONFIG dictionary

## Usage
```bash
python ga4_pipeline.py
```

Outputs:

SQLite database: ga4_data.db
CSV files: users_metrics.csv, content_metrics.csv, site_metrics.csv
Log file: ga4_data_pipeline.log

## Error Handling

Automatic retries for API failures (3 attempts), Database transaction rollbacks on errors, Comprehensive error logging, Empty dataset detection.
