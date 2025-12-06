# Mutual Fund ETL Tool

A Python-based ETL (Extract, Transform, Load) tool for fetching, processing, and validating Indian mutual fund and financial market data.

## Overview

This tool provides a comprehensive solution for extracting financial data from multiple sources, validating the data quality, and storing it in structured formats. It focuses on Indian mutual funds, stock indices, and market data with built-in validation mechanisms to ensure data integrity.

## Features

- **Multi-Source Data Fetching**: Integrates with multiple financial data providers:
  - `mftool`: Mutual fund NAV data
  - `mstarpy`: Morningstar data for holdings and sector breakdowns
  - `jugaad-data`: NSE index and stock market data
  - `yahooquery`: Additional market data

- **Data Validation**: Comprehensive validation for:
  - NAV (Net Asset Value) data
  - Mutual fund holdings
  - Sector allocations and breakdowns
  - Index constituents and data integrity

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

Run the demo script to see the tool in action:

```bash
python demo.py
```

The demo showcases:
1. Fetching NAV data and validating against expected values
2. Retrieving mutual fund holdings and checking completeness
3. Getting sector breakdowns with validation
4. Fetching NSE index data and validating integrity
5. Logging all discrepancies and validation results

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
