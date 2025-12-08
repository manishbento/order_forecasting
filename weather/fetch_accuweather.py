"""
AccuWeather Weather Fetcher
===========================
Fetches weather forecast data from AccuWeather API.

This script can be run standalone to fetch weather data for all stores:
    python -m weather.fetch_accuweather

The data is stored in JSON files and then processed into DuckDB.
"""

import os
import sys
import json
import glob
import duckdb
import pandas as pd
import requests
from datetime import datetime

# Add parent directory to path for imports when running standalone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings


# =============================================================================
# CONFIGURATION
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(SCRIPT_DIR, "accuweather_data")
DB_PATH = os.path.join(settings.DATA_STORE_DIR, "accuweather.db")

# Ensure directories exist
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_location_key(postal_code: str, api_key: str) -> str:
    """
    Get AccuWeather locationKey for a postal code.
    
    Args:
        postal_code: Store's postal code
        api_key: AccuWeather API key
        
    Returns:
        Location key string or None if not found
    """
    print(f"Fetching location key for {postal_code}...")
    url = "https://dataservice.accuweather.com/locations/v1/postalcodes/search"
    params = {"apikey": api_key, "q": postal_code}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and 'Key' in data[0]:
                location_key = data[0]['Key']
                print(f"Found location key: {location_key}")
                return location_key
            else:
                print(f"No location key found for: {postal_code}")
                return None
        else:
            print(f"Error fetching location key: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None


def get_accuweather_data(postal_code: str, store_no: str, api_key: str = None):
    """
    Fetch 5-day forecast from AccuWeather API.
    
    Args:
        postal_code: Store's postal code
        store_no: Store number for file naming
        api_key: AccuWeather API key
    """
    api_key = api_key or settings.ACCUWEATHER_API_KEY
    
    today_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"{store_no}_{today_date}.json"
    filepath = os.path.join(JSON_DIR, filename)

    # Skip if file exists
    if os.path.exists(filepath):
        print(f"Weather data for store {store_no} already exists. Skipping.")
        return

    location_key = get_location_key(postal_code, api_key)
    
    if not location_key:
        print(f"Skipping store {store_no} - no location key.")
        return

    print(f"Fetching 5-day forecast for store {store_no}...")
    
    url = f"https://dataservice.accuweather.com/forecasts/v1/daily/5day/{location_key}"
    params = {"apikey": api_key, "details": "true", "metric": "false"}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Weather data saved to {filepath}")
        else:
            print(f"Failed to fetch weather: {response.status_code}")
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def fetch_accuweather_for_all_stores(stores_df: pd.DataFrame):
    """
    Fetch AccuWeather data for all stores.
    
    Args:
        stores_df: DataFrame with 'postal_code' and 'store_no' columns
    """
    for _, row in stores_df.iterrows():
        postal_code = row["postal_code"]
        store_no = str(row["store_no"])
        get_accuweather_data(postal_code, store_no)


def process_accuweather_files(db_path: str = None):
    """
    Process all AccuWeather JSON files and load into DuckDB.
    
    Args:
        db_path: Path to DuckDB database
    """
    db_path = db_path or DB_PATH
    print("Processing AccuWeather files...")
    
    conn = duckdb.connect(db_path)
    
    # Create table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            store_no VARCHAR,
            date DATE,
            day_condition VARCHAR,
            day_low_rain INTEGER,
            day_medium_rain INTEGER,
            day_high_rain INTEGER,
            total_rain_expected REAL,
            temp_max REAL,
            temp_min REAL,
            realfeel_temp_max REAL,
            realfeel_temp_min REAL,
            hours_of_sun REAL,
            hours_of_rain REAL,
            day_short_phrase VARCHAR,
            day_long_phrase VARCHAR,
            PRIMARY KEY (store_no, date)
        )
    """)
    
    # Find all JSON files
    file_pattern = os.path.join(JSON_DIR, "*_*.json")
    json_files = glob.glob(file_pattern)
    
    if not json_files:
        print("No AccuWeather JSON files found.")
        conn.close()
        return

    all_weather_data = []

    for filepath in json_files:
        filename = os.path.basename(filepath)
        try:
            store_no = filename.split('_')[0]
            
            with open(filepath, 'r') as f:
                weather_json = json.load(f)
            
            for forecast in weather_json.get('DailyForecasts', []):
                # Parse date
                date_str = forecast.get('Date', '').split('T')[0]
                if not date_str:
                    continue
                
                day_data = forecast.get('Day', {})
                
                # Day condition
                day_condition = day_data.get('IconPhrase', 'N/A')
                
                # Rain data
                precip_val = float(day_data.get('Rain', {}).get('Value', 0.0) or 0.0)
                total_rain_expected = precip_val
                
                day_low_rain = 1 if (0 < precip_val <= 0.1) else 0
                day_medium_rain = 1 if (0.1 < precip_val <= 0.5) else 0
                day_high_rain = 1 if (precip_val > 0.5) else 0

                # Temperatures
                temp_max = float(forecast.get('Temperature', {}).get('Maximum', {}).get('Value', 0.0) or 0.0)
                temp_min = float(forecast.get('Temperature', {}).get('Minimum', {}).get('Value', 0.0) or 0.0)
                
                realfeel_temp_max = float(forecast.get('RealFeelTemperature', {}).get('Maximum', {}).get('Value', 0.0) or 0.0)
                realfeel_temp_min = float(forecast.get('RealFeelTemperature', {}).get('Minimum', {}).get('Value', 0.0) or 0.0)

                # Hours
                hours_of_sun = float(forecast.get('HoursOfSun', 0.0) or 0.0)
                hours_of_rain = float(day_data.get('HoursOfRain', 0.0) or 0.0)

                # Phrases
                day_short_phrase = day_data.get('ShortPhrase', 'N/A')
                day_long_phrase = day_data.get('LongPhrase', 'N/A')

                all_weather_data.append((
                    store_no, date_str, day_condition, day_low_rain, day_medium_rain,
                    day_high_rain, total_rain_expected, temp_max, temp_min,
                    realfeel_temp_max, realfeel_temp_min, hours_of_sun, hours_of_rain,
                    day_short_phrase, day_long_phrase
                ))

        except Exception as e:
            print(f"Error processing {filename}: {e}")

    # Insert into DuckDB
    if all_weather_data:
        df_columns = [
            'store_no', 'date', 'day_condition', 'day_low_rain',
            'day_medium_rain', 'day_high_rain', 'total_rain_expected',
            'temp_max', 'temp_min', 'realfeel_temp_max', 'realfeel_temp_min',
            'hours_of_sun', 'hours_of_rain', 'day_short_phrase', 'day_long_phrase'
        ]
        df_to_insert = pd.DataFrame(all_weather_data, columns=df_columns)
        
        conn.execute("INSERT OR REPLACE INTO weather SELECT * FROM df_to_insert")
        print(f"Processed {len(all_weather_data)} records into 'weather' table.")
    else:
        print("No weather data was processed.")

    conn.close()


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("AccuWeather Weather Data Fetcher")
    print("=" * 60)
    
    # Load store data
    try:
        from data.loader import DataLoader
        loader = DataLoader()
        loader.load_store_data()
        stores_df = loader.get_stores_df()
        
        # Fetch weather
        fetch_accuweather_for_all_stores(stores_df)
        
        # Process files
        process_accuweather_files()
        
        loader.disconnect()
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure to run data loader first to populate stores table.")
