"""
Global Settings and Configuration
=================================
Central configuration for the forecasting system.
All configurable parameters are defined here for easy modification.
"""

from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
# Base directory of this project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Output directories
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
EXCEL_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "excel")
JSON_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "json")

# Data store directory (DuckDB databases)
DATA_STORE_DIR = os.path.join(BASE_DIR, "data_store")

# Configuration files
CONFIG_FILE_PATH = os.path.join(BASE_DIR, "config_files", "Configuration.xlsx")

# =============================================================================
# FORECAST DATE RANGE
# =============================================================================
# These define the date range for which forecasts will be generated
FORECAST_START_DATE_V = datetime(2025, 12, 11)
FORECAST_END_DATE_V = datetime(2025, 12, 14)

# String versions for SQL queries
FORECAST_START_DATE = FORECAST_START_DATE_V.strftime('%Y-%m-%d')
FORECAST_END_DATE = FORECAST_END_DATE_V.strftime('%Y-%m-%d')

# =============================================================================
# DATA LOADING DATE RANGE
# =============================================================================
# End date matches forecast end date
END_DATE_V = FORECAST_END_DATE_V
# Start date goes back 45 days for historical data
START_DATE_V = FORECAST_START_DATE_V - timedelta(days=45)

# String versions for SQL queries
START_DATE = START_DATE_V.strftime('%Y-%m-%d')
END_DATE = END_DATE_V.strftime('%Y-%m-%d')

# =============================================================================
# REGION CONFIGURATION
# =============================================================================
# List of region codes to process
REGION_CODES = ['BA', 'LA', 'SD', 'NE', 'SE', 'TE', 'MW']

# =============================================================================
# EXCEPTIONAL DAYS
# =============================================================================
# Days to be excluded from historical week calculations
# These typically include holidays and days with abnormal sales patterns
EXCEPTIONAL_DAYS = [
    '2025-07-01',  # Independence Day Holiday
    '2025-07-02',  # Independence Day Holiday
    '2025-07-03',  # Independence Day Holiday
    '2025-07-04',  # Independence Day Holiday
    '2025-09-01',  # Labor Day Holiday
    '2025-09-02',  # Next Day of Holiday
    '2025-09-24',  # Not Available Yet
    '2025-11-24',  # Not Available - Thanksgiving
    '2025-11-25',  # Not Available - Thanksgiving
    '2025-11-26',  # Not Available - Thanksgiving
    '2025-11-27',  # Thanksgiving Day
    '2025-11-28',  # Thanksgiving - Immediate
    '2025-11-29',  # Thanksgiving - Immediate 2
    '2025-11-30',  # Thanksgiving - Immediate 3
]

# =============================================================================
# ITEM CONFIGURATION
# =============================================================================
# Non-hero items that receive different treatment in forecasting
NON_HERO_ITEMS: list[int] = [
    1942690, 
    1940912,
    1816554,  # Maki Trio
    1713314   # Salmon Combo
]

# =============================================================================
# PLATTER CONFIGURATION
# =============================================================================
# Platters are normally excluded from forecasts (they're not in config_active).
# Use this setting to include platters for specific date/region combinations.
# Each entry should have:
#   - 'region': Region code (e.g., 'BA', 'LA', 'SD', 'NE', 'SE', 'TE')
#   - 'start_date': Start date for inclusion (YYYY-MM-DD format)
#   - 'end_date': End date for inclusion (YYYY-MM-DD format)
#   - 'items': Optional list of specific platter item numbers. If empty/None, all platters are included.
#
# Note: Platter case size is 3 (vs 6 for regular products), which is already handled in the query.

PLATTER_INCLUSIONS = [
    # Example configuration:
    {
        'region': 'BA',
        'start_date': '2025-12-04',
        'end_date': '2025-12-31',
        'items': None  # None means all platters, or specify [item_no1, item_no2, ...]
    },
]

# Default case pack sizes
PLATTER_CASE_SIZE = 3   # Case pack size for platters
REGULAR_CASE_SIZE = 6   # Case pack size for regular products

# =============================================================================
# STORE CONFIGURATION
# =============================================================================
# Inactive stores that should receive zero forecast
INACTIVE_STORES = [
    129,  # Santa Clara
    423,  # Sunnyvale
    187   # Gwinnett
]

# =============================================================================
# DEFAULT FORECAST PARAMETERS
# =============================================================================
# These are the default parameters used when not running scenario testing

DEFAULT_PARAMS = {
    'BASE_COVER': 0.05,                      # Base coverage on top of average sales forecast
    'BASE_COVER_SOLD_OUT': 0.06,             # Base coverage if last week was sold out
    'K_FACTOR': 0.25,                        # Service level factor for safety stock (~60% service level)
    'CASE_SIZE': 6,                          # Default case pack size for rounding
    'WEEK_WEIGHTS': (0.6, 0.2, 0.1, 0.1),    # Weights for 4 weeks (W1, W2, W3, W4)
    'HIGH_SHRINK_THRESHOLD': 0.15,           # Threshold for high shrink adjustment
    'ROUND_DOWN_SHRINK_THRESHOLD': 0.00      # Threshold for round down adjustment
}

# =============================================================================
# FINANCIAL PARAMETERS
# =============================================================================
# Food cost component for margin calculations
FOOD_COST_COMPONENT = 0.31

# =============================================================================
# BEHAVIOR FLAGS
# =============================================================================
# Control what actions the forecast pipeline performs
LOAD_DATA = True                  # Whether to load data from Fabric Datalake
EXPORT_JSON = True                # Whether to export forecasts to JSON
PUSH_FABRIC = False               # Whether to push results to Fabric Warehouse
GENERATE_FILE_OUTPUTS = True      # Whether to generate Excel/JSON output files
SCENARIO_TESTING = False          # Whether to run scenario testing
APPLY_WEATHER_ADJUSTMENT = True   # Whether to apply weather-based forecast adjustments

# =============================================================================
# WEATHER ADJUSTMENT PARAMETERS
# =============================================================================
# These control the weather-based forecast adjustment behavior
# Weather adjustments are based purely on severity - if weather is bad, sales drop
WEATHER_SEVERITY_THRESHOLD = 4.0  # Minimum severity score to trigger adjustment (0-10)
WEATHER_MAX_REDUCTION = 0.40      # Maximum reduction percentage (40%)

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================
# DuckDB database paths
SHRINK_DB_PATH = os.path.join(DATA_STORE_DIR, "shrink_data.db")
WEATHER_DB_PATH = os.path.join(DATA_STORE_DIR, "weather.db")
ACCUWEATHER_DB_PATH = os.path.join(DATA_STORE_DIR, "accuweather.db")

# =============================================================================
# FABRIC CONNECTION SETTINGS
# =============================================================================
# These are loaded from environment variables (set in .env file)
FABRIC_CLIENT_ID = os.environ.get('FABRIC_CLIENT_ID')
FABRIC_CLIENT_SECRET = os.environ.get('FABRIC_CLIENT_SECRET')
FABRIC_TENANT_ID = os.environ.get('FABRIC_TENANT_ID')

# SQL Endpoints
FABRIC_LAKEHOUSE_ENDPOINT = 'koj73uk36z4u5jlkoc2fsgwuqq-6rdov3bdqjjute3kn2jtfurh2q.datawarehouse.fabric.microsoft.com'
FABRIC_LAKEHOUSE_DB = 'BNTO_LH_300_GOLD'
FABRIC_WAREHOUSE_DB = 'BNTO_WH_300_GOLD'

# =============================================================================
# WEATHER API CONFIGURATION
# =============================================================================
VISUALCROSSING_API_KEY = os.environ.get('VISUALCROSSING_API_KEY')
ACCUWEATHER_API_KEY = os.environ.get('ACCUWEATHER_API_KEY')
OPENWEATHER_API_KEY = os.environ.get('OPENWEATHER_API_KEY')


def get_output_paths(region: str, forecast_date: str) -> dict:
    """
    Generate output file paths for a given region and date.
    
    Args:
        region: Region code (e.g., 'BA', 'LA')
        forecast_date: Forecast date string (YYYY-MM-DD)
    
    Returns:
        Dictionary with 'excel' and 'json' output paths
    """
    return {
        'excel': os.path.join(EXCEL_OUTPUT_DIR, f'Costco_{region}_PO_{forecast_date}.xlsx'),
        'json': os.path.join(JSON_OUTPUT_DIR, f'Costco_{region}_PO_{forecast_date}.json')
    }


def ensure_directories():
    """Create output directories if they don't exist."""
    for dir_path in [OUTPUT_DIR, EXCEL_OUTPUT_DIR, JSON_OUTPUT_DIR, DATA_STORE_DIR]:
        os.makedirs(dir_path, exist_ok=True)
