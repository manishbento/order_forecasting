"""
OpenWeatherMap Weather Fetcher (One Call API 3.0)
=================================================
Fetches 8-day weather forecast data from OpenWeatherMap One Call API 3.0.

This script converts postal codes to coordinates (GeoCoding) and then 
fetches the daily forecast, including Severe Weather Alerts and a calculated
Severity Score (0-10).

**Integration Status**: ✅ Fully integrated into forecasting pipeline
- Weather data is loaded via `weather.loader.load_openweathermap_data()`
- Enriched into forecast rows with prefix `owm_*` 
- Severity scores and alerts are used in `forecasting.adjustments.apply_weather_adjustments()`

Usage:
    python -m weather.fetch_openweather
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
JSON_DIR = os.path.join(SCRIPT_DIR, "openweathermap_data")
DB_PATH = os.path.join(settings.DATA_STORE_DIR, "openweathermap.db")

# Ensure directories exist
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_geo_coordinates(postal_code: str, country_code: str = "US", api_key: str = None):
    """
    Convert postal code to Lat/Lon using OpenWeatherMap Geocoding API.
    """
    api_key = api_key or settings.OPENWEATHER_API_KEY
    postal_code = str(postal_code).strip()
    
    url = "http://api.openweathermap.org/geo/1.0/zip"
    query = f"{postal_code},{country_code}"
    params = {"zip": query, "appid": api_key}
    
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return data.get('lat'), data.get('lon'), data.get('name')
        else:
            print(f"Geocoding failed for {postal_code}: {response.status_code}")
            return None, None, None
    except requests.RequestException as e:
        print(f"Geocoding request error: {e}")
        return None, None, None


def get_openweathermap_data(postal_code: str, store_no: str, api_key: str = None):
    """
    Fetch 8-day daily forecast + Alerts using One Call API 3.0.
    """
    api_key = api_key or settings.OPENWEATHER_API_KEY
    
    today_date = datetime.now().strftime("%Y-%m-%d")
    filename = f"{store_no}_{today_date}.json"
    filepath = os.path.join(JSON_DIR, filename)

    if os.path.exists(filepath):
        print(f"Weather data for store {store_no} already exists. Skipping.")
        return

    # Step 1: Get Coordinates
    print(f"Geocoding store {store_no} ({postal_code})...")
    lat, lon, location_name = get_geo_coordinates(postal_code, api_key=api_key)
    
    if not lat or not lon:
        print(f"Skipping store {store_no} - could not geocode.")
        return

    # Step 2: Fetch One Call API 3.0
    # Included 'alerts' in the fetch now (removed from exclude list)
    print(f"Fetching forecast & alerts for {location_name}...")
    url = "https://api.openweathermap.org/data/3.0/onecall"
    
    params = {
        "lat": lat,
        "lon": lon,
        "exclude": "current,minutely,hourly", # Keep 'daily' and 'alerts'
        "units": "imperial", 
        "appid": api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            data['store_metadata'] = {
                'store_no': store_no,
                'postal_code': postal_code,
                'location_name': location_name
            }
            
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Weather data saved to {filepath}")
        else:
            print(f"Failed to fetch weather: {response.status_code}")
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def calculate_severity_score(wind_speed, rain_vol, snow_vol, active_alerts):
    """
    Calculates a 0-10 severity score based on weather conditions.
    Weighted to reflect impact on footfall (high sensitivity to alerts and precipitation).
    """
    score = 0
    
    # 1. Official Alerts (High impact on consumer behavior)
    if active_alerts:
        score += 6 
    
    # 2. Precipitation Factors (Inches)
    # Snow/Ice (Physical barrier to travel)
    if snow_vol >= 6.0: score += 10    # Immediate Shutdown
    elif snow_vol >= 2.0: score += 7   # Significant Travel Impact
    elif snow_vol >= 0.5: score += 4   # Cautionary
    
    # Rain (Deterrent)
    if rain_vol >= 1.5: score += 7     # Washout
    elif rain_vol >= 0.5: score += 4   # Heavy
    elif rain_vol >= 0.1: score += 2   # Wet/Unpleasant
    
    # 3. Wind Factors (Secondary factor)
    if wind_speed >= 50: score += 5    # Dangerous
    elif wind_speed >= 30: score += 2  # Unpleasant
        
    return min(score, 10)  # Cap at 10


def fetch_weather_for_all_stores(stores_df: pd.DataFrame):
    if 'postal_code' not in stores_df.columns or 'store_no' not in stores_df.columns:
        print("Error: DataFrame must contain 'postal_code' and 'store_no' columns.")
        return

    for _, row in stores_df.iterrows():
        postal_code = row["postal_code"]
        store_no = str(row["store_no"])
        get_openweathermap_data(postal_code, store_no)


def process_weather_files(db_path: str = None, force_purge: bool = False):
    db_path = db_path or DB_PATH
    print("Processing OpenWeatherMap files...")
    
    conn = duckdb.connect(db_path)
    
    if force_purge:
        conn.execute("DROP TABLE IF EXISTS weather")

    # Expanded table to include severity_score and alert_tags
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            store_no VARCHAR,
            date DATE,
            day_condition VARCHAR,
            day_low_rain INTEGER,
            day_medium_rain INTEGER,
            day_high_rain INTEGER,
            total_rain_expected REAL,
            total_snow_expected REAL,
            pop_probability REAL,
            temp_max REAL,
            temp_min REAL,
            humidity INTEGER,
            wind_speed REAL,
            severity_score INTEGER,
            alert_tags VARCHAR,
            latitude REAL,
            longitude REAL,
            PRIMARY KEY (store_no, date)
        )
    """)
    
    file_pattern = os.path.join(JSON_DIR, "*_*.json")
    json_files = glob.glob(file_pattern)
    
    if not json_files:
        print("No weather JSON files found.")
        conn.close()
        return

    all_weather_data = []

    for filepath in json_files:
        filename = os.path.basename(filepath)
        try:
            with open(filepath, 'r') as f:
                weather_json = json.load(f)
            
            meta = weather_json.get('store_metadata', {})
            store_no = meta.get('store_no', filename.split('_')[0])
            lat = weather_json.get('lat')
            lon = weather_json.get('lon')

            # Get Alerts
            alerts = weather_json.get('alerts', [])
            
            daily_forecasts = weather_json.get('daily', [])
            
            for day in daily_forecasts:
                dt_ts = day.get('dt')
                day_dt = datetime.fromtimestamp(dt_ts)
                date_str = day_dt.strftime('%Y-%m-%d')
                
                # Conditions
                weather_info = day.get('weather', [{}])[0]
                day_condition = weather_info.get('description', 'N/A')
                
                temp_data = day.get('temp', {})
                temp_max = float(temp_data.get('max', 0.0))
                temp_min = float(temp_data.get('min', 0.0))
                
                humidity = int(day.get('humidity', 0))
                wind_speed = float(day.get('wind_speed', 0.0))
                pop = float(day.get('pop', 0.0))
                
                rain_vol = float(day.get('rain', 0.0))
                snow_vol = float(day.get('snow', 0.0))
                
                total_rain_expected = rain_vol
                total_snow_expected = snow_vol
                
                # Categorize Rain
                day_low_rain = 1 if (0 < rain_vol <= 0.1) else 0
                day_medium_rain = 1 if (0.1 < rain_vol <= 0.5) else 0
                day_high_rain = 1 if (rain_vol > 0.5) else 0

                # Match Alerts to this specific Date
                # We check if the alert time window overlaps with this day (00:00 - 23:59)
                day_start_ts = day_dt.replace(hour=0, minute=0, second=0).timestamp()
                day_end_ts = day_dt.replace(hour=23, minute=59, second=59).timestamp()
                
                active_alerts = []
                for alert in alerts:
                    # Check overlap: (Alert Start <= Day End) AND (Alert End >= Day Start)
                    if alert.get('start') <= day_end_ts and alert.get('end') >= day_start_ts:
                        active_alerts.append(alert.get('event', 'Unknown Alert'))
                
                alert_tags = ", ".join(active_alerts) if active_alerts else None
                
                # Calculate Severity Score (0-10)
                severity_score = calculate_severity_score(wind_speed, rain_vol, snow_vol, active_alerts)

                all_weather_data.append((
                    store_no, date_str, day_condition, 
                    day_low_rain, day_medium_rain, day_high_rain, 
                    total_rain_expected, total_snow_expected, pop, 
                    temp_max, temp_min, humidity, wind_speed,
                    severity_score, alert_tags,
                    lat, lon
                ))

        except Exception as e:
            print(f"Error processing {filename}: {e}")

    if all_weather_data:
        df_columns = [
            'store_no', 'date', 'day_condition', 
            'day_low_rain', 'day_medium_rain', 'day_high_rain', 
            'total_rain_expected', 'total_snow_expected', 'pop_probability', 
            'temp_max', 'temp_min', 'humidity', 'wind_speed',
            'severity_score', 'alert_tags',
            'latitude', 'longitude'
        ]
        
        df_to_insert = pd.DataFrame(all_weather_data, columns=df_columns)
        
        conn.register("df_weather_temp", df_to_insert)
        conn.execute("INSERT OR REPLACE INTO weather SELECT * FROM df_weather_temp")
        conn.unregister("df_weather_temp")
        
        print(f"Processed {len(all_weather_data)} records into 'weather' table.")
    else:
        print("No weather data was processed.")

    conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("OpenWeatherMap Data Fetcher (One Call 3.0)")
    print("=" * 60)
    
    try:
        from data.loader import DataLoader
        loader = DataLoader()
        loader.load_store_data()
        stores_df = loader.get_stores_df()
        
        fetch_weather_for_all_stores(stores_df)
        process_weather_files()
        
        loader.disconnect()
        
    except ImportError:
        print("Warning: 'data.loader' not found. Running in mock/test mode.")
    except Exception as e:
        print(f"Error: {e}")