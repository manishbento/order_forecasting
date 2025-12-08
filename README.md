# Costco Order Forecasting v6

A modular, extensible forecasting system for Costco order management.

## Project Structure

```
costco_order_forecasting_v6/
├── main.py                  # Main orchestrator - entry point
├── config/
│   ├── __init__.py
│   ├── settings.py          # Global configuration and parameters
│   └── scenarios.py         # Scenario definitions for testing
├── data/
│   ├── __init__.py
│   ├── loader.py            # Data loading from Fabric/DuckDB
│   └── prep.py              # Data preparation and cleaning
├── forecasting/
│   ├── __init__.py
│   ├── engine.py            # Core forecasting logic
│   ├── adjustments.py       # Uplift, promotion, and event adjustments
│   └── rounding.py          # Rounding and safety stock logic
├── weather/
│   ├── __init__.py
│   ├── fetch_visualcrossing.py   # VisualCrossing API fetcher
│   ├── fetch_accuweather.py      # AccuWeather API fetcher
│   └── loader.py                 # Weather data loader
├── export/
│   ├── __init__.py
│   ├── excel.py             # Excel export functionality
│   └── json_export.py       # JSON export functionality
├── utils/
│   ├── __init__.py
│   ├── fabric_lakehouse.py  # FabricDatalake connection utility
│   ├── fabric_warehouse.py  # FabricDatalakeWH connection utility
│   └── xl_writer.py         # Excel writer utility
├── sql/
│   └── forecast_base.sql    # SQL queries for data extraction
├── output/                  # Output directory for exports
│   ├── excel/
│   └── json/
├── data_store/              # Local DuckDB databases
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

## Installation

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running the Full Forecast Pipeline

```bash
python main.py
```

### Running Weather Data Fetch (Standalone)

```bash
# Fetch from VisualCrossing
python -m weather.fetch_visualcrossing

# Fetch from AccuWeather
python -m weather.fetch_accuweather
```

### Configuration

Edit `config/settings.py` to modify:
- Forecast date ranges
- Region codes
- Base coverage parameters
- K-factor (service level)
- Case pack sizes
- Week weights for EMA calculation

## Architecture Overview

### Step 1: Data Loading (`data/loader.py`)
- Loads historical sales, shipment, and shrink data from Fabric Datalake
- Loads configuration files (Excel)
- Creates local DuckDB tables for fast querying

### Step 2: Data Preparation (`data/prep.py`)
- Cleans and validates historical data
- Applies item substitutions
- Handles exceptional days exclusions

### Step 3: Weather Data (`weather/`)
- Fetches weather forecasts from multiple providers
- Stores in local DuckDB for fast lookup
- Can be run independently or as part of pipeline

### Step 4: Core Forecasting (`forecasting/engine.py`)
- Calculates sales velocity and volatility
- Computes weighted/EMA forecasts
- Applies base forecast logic

### Step 5: Adjustments (`forecasting/adjustments.py`)
- Promotional uplifts
- Holiday/event adjustments
- Region-specific modifications
- Stock-out response logic

### Step 6: Rounding & Safety Stock (`forecasting/rounding.py`)
- Case pack size rounding
- Intelligent round-up/round-down decisions
- Safety stock calculations
- Shrink-aware guardrails

### Step 7: Export (`export/`)
- Excel reports with formatted sheets
- JSON export for system integration
- Fabric Warehouse push (optional)

## Future Development

The modular structure supports:
- Pluggable forecasting models (Holt's, ARIMA, ML-based)
- Additional weather providers
- New adjustment rules
- Scenario testing framework
- Performance simulation

## Dependencies

See `requirements.txt` for full list. Key dependencies:
- `duckdb` - Local analytical database
- `polars` - Fast DataFrame operations
- `pandas` - Data manipulation
- `numpy` - Numerical calculations
- `pyodbc` - Database connectivity
- `xlsxwriter` - Excel file generation
- `requests` - API calls
