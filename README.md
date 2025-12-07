# Mutual Fund ETL Tool

A Python-based ETL (Extract, Transform, Load) tool for fetching, processing, and validating Indian mutual fund and financial market data.

## Overview

This tool provides a comprehensive solution for extracting financial data from multiple sources, validating the data quality, and storing it in structured formats. It focuses on Indian mutual funds, stock indices, and market data with built-in validation mechanisms to ensure data integrity.

Spring Boot or another upstream service now handles parsing uploaded statements and sends the parsed holdings to this FastAPI endpoint for enrichment. The Python component enriches the holdings with NAV metadata, AMCs, sector breakdowns, and Morningstar holdings so downstream systems can focus on scoring and reporting.

## Features

- **Multi-Source Data Fetching**: Integrates with multiple financial data providers:
  - `mftool`: Mutual fund NAV data
  - `mstarpy`: Morningstar data for holdings and sector breakdowns
  - `jugaad-data`: NSE index and stock market data
  - `yahooquery`: Additional market data

- **Data Validation**: Comprehensive validation for:
 - **Data Validation**: Comprehensive validation for:
  - NAV (Net Asset Value) data that powers enrichment lookups
  - Mutual fund holdings submitted in the parsed payload
  - Sector allocations and breakdowns retrieved from Morningstar
  - Index constituents and market data integrity (legacy validators)

- **Configurable Thresholds**: YAML-based configuration for validation rules and data source settings

- **Logging & Monitoring**: Built-in logging system to track data fetching operations and validation results

- **Export Capabilities**: Save data in multiple formats (JSON, CSV)

## Project Structure

```
MutualFund_ETL_TOOL/
├── config/                 # Configuration files
│   └── config.yaml        # Main configuration
├── data/                  # Output data directory
├── logs/                  # Application logs
├── src/                   # Source code
│   ├── fetchers/         # Data fetching modules
│   ├── validators/       # Data validation modules
│   └── utils/            # Utility functions
├── tests/                # Test suite
├── demo.py               # End-to-end demonstration script
└── requirements.txt      # Python dependencies
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd MutualFund_ETL_TOOL
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Start the Python enrichment service and have Spring Boot call it after it finishes parsing statements:

```bash
uvicorn etl_service.main:app --host 0.0.0.0 --port 8081
```

### Enrichment API

1. POST `/etl/enrich` with parsed holdings (no file paths or uploads). Example payload:
   ```json
   {
     "upload_id": "upload-001",
     "user_id": "user-789",
     "file_type": "pdf",
     "parsed_holdings": [
       {
         "fund_name": "HDFC Mid-Cap Growth",
         "units": 150.45,
         "nav": 1485.50,
         "value": 223495.48,
         "purchase_date": "2020-06-15"
       }
     ]
   }
   ```
2. The service validates the holdings, resolves schemes via `FundEnricher`, and replies with enriched fund metadata plus an `enrichment_quality` summary that lists success/failure counts and any warnings.

This keeps the parsing responsibility upstream while Python focuses on multi-source enrichment and quality reporting.

## Configuration

Edit `config/config.yaml` to customize:
- Validation thresholds (NAV ranges, sector minimums, etc.)
- Data source settings and timeouts
- Logging levels and output formats
- Export preferences

## Components

### Fetchers
- **MFToolFetcher**: Fetch mutual fund NAV and scheme details
- **MstarPyFetcher**: Retrieve holdings and sector data from Morningstar
- **JugaadDataFetcher**: Access NSE index and market data
- **YahooQueryFetcher**: Additional market data queries

### Validators
- **NAVValidator**: Validate NAV values, dates, and ranges
- **HoldingsValidator**: Verify completeness and accuracy of fund holdings
- **SectorValidator**: Check sector allocation percentages
- **IndexValidator**: Validate index constituent data

### Utilities
- **ConfigLoader**: Load and parse YAML configuration
- **FundResolver**: Resolve fund names to scheme codes
- **Logger**: Setup and manage application logging

## Output

Data is saved in the `data/` directory in configured formats:
- Raw data in JSON format
- Processed holdings in CSV format
- Validation reports with timestamps

## Requirements

- Python 3.8+
- See `requirements.txt` for complete list of dependencies

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]
