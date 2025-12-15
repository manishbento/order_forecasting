"""
Weather Data Loader
===================
Loads weather data from DuckDB databases into dictionaries for fast lookup.

This module provides functions to load weather data from:
- VisualCrossing (weather.db)
- AccuWeather (accuweather.db)
- OpenWeatherMap (weather.db in openweathermap_data/)
"""

import os
import duckdb
from typing import Dict, Tuple

from config import settings


def load_visualcrossing_data(db_path: str = None) -> Dict[Tuple[str, str], dict]:
    """
    Load weather data from VisualCrossing database.
    
    Args:
        db_path: Path to weather.db
        
    Returns:
        Dictionary keyed by (store_no, date) with weather info
    """
    db_path = db_path or settings.WEATHER_DB_PATH
    weather_data = {}
    
    if not os.path.exists(db_path):
        print(f"Weather database not found: {db_path}")
        return weather_data
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Check if enhanced schema exists by checking for severity_score column
        columns = conn.execute("PRAGMA table_info(weather)").fetchall()
        column_names = [col[1] for col in columns]
        has_severity = 'severity_score' in column_names
        
        if has_severity:
            # Enhanced schema with severity metrics
            rows = conn.execute("""
                SELECT 
                    store_no, date, day_condition, day_low_rain, day_medium_rain,
                    day_high_rain, total_rain_expected, latitude, longitude,
                    resolved_address, timezone,
                    -- Severity metrics
                    severity_score, severity_category, sales_impact_factor,
                    rain_severity, snow_severity, wind_severity,
                    visibility_severity, temp_severity,
                    -- Additional weather data
                    snow_amount, snow_depth, wind_speed, wind_gust,
                    temp_max, temp_min, visibility, severe_risk,
                    -- Precipitation details
                    precip_probability, precip_cover,
                    -- Atmosphere
                    humidity, cloud_cover
                FROM weather
            """).fetchall()
            
            for row in rows:
                key = (str(row[0]), str(row[1]))
                weather_data[key] = {
                    'day_condition': row[2],
                    'day_low_rain': row[3],
                    'day_medium_rain': row[4],
                    'day_high_rain': row[5],
                    'total_rain_expected': row[6],
                    'latitude': row[7],
                    'longitude': row[8],
                    'resolved_address': row[9],
                    'timezone': row[10],
                    # Severity metrics
                    'severity_score': row[11],
                    'severity_category': row[12],
                    'sales_impact_factor': row[13],
                    'rain_severity': row[14],
                    'snow_severity': row[15],
                    'wind_severity': row[16],
                    'visibility_severity': row[17],
                    'temp_severity': row[18],
                    # Additional weather data
                    'snow_amount': row[19],
                    'snow_depth': row[20],
                    'wind_speed': row[21],
                    'wind_gust': row[22],
                    'temp_max': row[23],
                    'temp_min': row[24],
                    'visibility': row[25],
                    'severe_risk': row[26],
                    # Precipitation details
                    'precip_probability': row[27],
                    'precip_cover': row[28],
                    # Atmosphere
                    'humidity': row[29],
                    'cloud_cover': row[30],
                }
        else:
            # Legacy schema
            rows = conn.execute("""
                SELECT 
                    store_no, date, day_condition, day_low_rain, day_medium_rain,
                    day_high_rain, total_rain_expected, latitude, longitude,
                    resolved_address, timezone
                FROM weather
            """).fetchall()
            
            for row in rows:
                key = (str(row[0]), str(row[1]))
                weather_data[key] = {
                    'day_condition': row[2],
                    'day_low_rain': row[3],
                    'day_medium_rain': row[4],
                    'day_high_rain': row[5],
                    'total_rain_expected': row[6],
                    'latitude': row[7],
                    'longitude': row[8],
                    'resolved_address': row[9],
                    'timezone': row[10],
                    # Set default severity values for legacy data
                    'severity_score': 0,
                    'severity_category': 'MINIMAL',
                    'sales_impact_factor': 1.0,
                }
        
        conn.close()
        print(f"Loaded {len(weather_data)} VisualCrossing weather records (enhanced={has_severity})")
    except Exception as e:
        print(f"Error loading VisualCrossing data: {e}")
    
    return weather_data


def load_accuweather_data(db_path: str = None) -> Dict[Tuple[str, str], dict]:
    """
    Load weather data from AccuWeather database.
    
    Args:
        db_path: Path to accuweather.db
        
    Returns:
        Dictionary keyed by (store_no, date) with weather info
    """
    db_path = db_path or settings.ACCUWEATHER_DB_PATH
    weather_data = {}
    
    if not os.path.exists(db_path):
        print(f"AccuWeather database not found: {db_path}")
        return weather_data
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        rows = conn.execute("""
            SELECT 
                store_no, date, day_condition, day_low_rain, day_medium_rain,
                day_high_rain, total_rain_expected, temp_max, temp_min,
                realfeel_temp_max, realfeel_temp_min, hours_of_sun, hours_of_rain,
                day_short_phrase, day_long_phrase
            FROM weather
        """).fetchall()
        
        for row in rows:
            key = (str(row[0]), str(row[1]))
            weather_data[key] = {
                'day_condition': row[2],
                'day_low_rain': row[3],
                'day_medium_rain': row[4],
                'day_high_rain': row[5],
                'total_rain_expected': row[6],
                'temp_max': row[7],
                'temp_min': row[8],
                'realfeel_temp_max': row[9],
                'realfeel_temp_min': row[10],
                'hours_of_sun': row[11],
                'hours_of_rain': row[12],
                'day_short_phrase': row[13],
                'day_long_phrase': row[14]
            }
        
        conn.close()
        print(f"Loaded {len(weather_data)} AccuWeather records")
    except Exception as e:
        print(f"Error loading AccuWeather data: {e}")
    
    return weather_data


def load_openweathermap_data(db_path: str = None) -> Dict[Tuple[str, str], dict]:
    """
    Load weather data from OpenWeatherMap database.
    
    Args:
        db_path: Path to openweathermap.db (in data_store/)
        
    Returns:
        Dictionary keyed by (store_no, date) with weather info
    """
    db_path = db_path or os.path.join(settings.DATA_STORE_DIR, "openweathermap.db")
    weather_data = {}
    
    if not os.path.exists(db_path):
        print(f"OpenWeatherMap database not found: {db_path}")
        return weather_data
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        rows = conn.execute("""
            SELECT 
                store_no, date, day_condition, day_low_rain, day_medium_rain,
                day_high_rain, total_rain_expected, total_snow_expected, 
                pop_probability, temp_max, temp_min, humidity, wind_speed,
                severity_score, alert_tags, latitude, longitude
            FROM weather
        """).fetchall()
        
        for row in rows:
            key = (str(row[0]), str(row[1]))
            weather_data[key] = {
                'day_condition': row[2],
                'day_low_rain': row[3],
                'day_medium_rain': row[4],
                'day_high_rain': row[5],
                'total_rain_expected': row[6],
                'total_snow_expected': row[7],
                'pop_probability': row[8],
                'temp_max': row[9],
                'temp_min': row[10],
                'humidity': row[11],
                'wind_speed': row[12],
                'severity_score': row[13],
                'alert_tags': row[14],
                'latitude': row[15],
                'longitude': row[16]
            }
        
        conn.close()
        print(f"Loaded {len(weather_data)} OpenWeatherMap records")
    except Exception as e:
        print(f"Error loading OpenWeatherMap data: {e}")
    
    return weather_data


def enrich_row_with_weather(row: dict, 
                            vc_data: Dict[Tuple[str, str], dict],
                            accu_data: Dict[Tuple[str, str], dict],
                            owm_data: Dict[Tuple[str, str], dict] = None) -> dict:
    """
    Enrich a forecast row with weather data from all providers.
    
    Args:
        row: Item-store forecast dictionary
        vc_data: VisualCrossing weather data dictionary
        accu_data: AccuWeather data dictionary
        owm_data: OpenWeatherMap data dictionary (optional)
        
    Returns:
        Updated row with weather fields populated
    """
    store_no = str(row['store_no'])
    forecast_date = str(row['date_forecast'])
    key = (store_no, forecast_date)
    
    # VisualCrossing data
    vc_info = vc_data.get(key, {})
    row['weather_day_condition'] = vc_info.get('day_condition')
    row['weather_day_low_rain'] = vc_info.get('day_low_rain')
    row['weather_day_medium_rain'] = vc_info.get('day_medium_rain')
    row['weather_day_high_rain'] = vc_info.get('day_high_rain')
    row['weather_total_rain_expected'] = vc_info.get('total_rain_expected')
    row['weather_latitude'] = vc_info.get('latitude')
    row['weather_longitude'] = vc_info.get('longitude')
    row['weather_resolved_address'] = vc_info.get('resolved_address')
    row['weather_timezone'] = vc_info.get('timezone')
    
    # Weather severity metrics from enhanced VisualCrossing
    row['weather_severity_score'] = vc_info.get('severity_score')
    row['weather_severity_category'] = vc_info.get('severity_category')
    row['weather_sales_impact_factor'] = vc_info.get('sales_impact_factor')
    row['weather_rain_severity'] = vc_info.get('rain_severity')
    row['weather_snow_severity'] = vc_info.get('snow_severity')
    row['weather_wind_severity'] = vc_info.get('wind_severity')
    row['weather_visibility_severity'] = vc_info.get('visibility_severity')
    row['weather_temp_severity'] = vc_info.get('temp_severity')
    row['weather_snow_amount'] = vc_info.get('snow_amount')
    row['weather_snow_depth'] = vc_info.get('snow_depth')
    row['weather_wind_speed'] = vc_info.get('wind_speed')
    row['weather_wind_gust'] = vc_info.get('wind_gust')
    row['weather_temp_max'] = vc_info.get('temp_max')
    row['weather_temp_min'] = vc_info.get('temp_min')
    row['weather_visibility'] = vc_info.get('visibility')
    row['weather_severe_risk'] = vc_info.get('severe_risk')
    row['weather_precip_probability'] = vc_info.get('precip_probability')
    row['weather_precip_cover'] = vc_info.get('precip_cover')
    row['weather_humidity'] = vc_info.get('humidity')
    row['weather_cloud_cover'] = vc_info.get('cloud_cover')
    
    # AccuWeather data
    accu_info = accu_data.get(key, {})
    row['accuweather_day_condition'] = accu_info.get('day_condition')
    row['accuweather_day_low_rain'] = accu_info.get('day_low_rain')
    row['accuweather_day_medium_rain'] = accu_info.get('day_medium_rain')
    row['accuweather_day_high_rain'] = accu_info.get('day_high_rain')
    row['accuweather_total_rain_expected'] = accu_info.get('total_rain_expected')
    row['accuweather_temp_max'] = accu_info.get('temp_max')
    row['accuweather_temp_min'] = accu_info.get('temp_min')
    row['accuweather_realfeel_temp_max'] = accu_info.get('realfeel_temp_max')
    row['accuweather_realfeel_temp_min'] = accu_info.get('realfeel_temp_min')
    row['accuweather_hours_of_sun'] = accu_info.get('hours_of_sun')
    row['accuweather_hours_of_rain'] = accu_info.get('hours_of_rain')
    row['accuweather_day_short_phrase'] = accu_info.get('day_short_phrase')
    row['accuweather_day_long_phrase'] = accu_info.get('day_long_phrase')
    
    # OpenWeatherMap data (if available)
    if owm_data:
        owm_info = owm_data.get(key, {})
        row['owm_day_condition'] = owm_info.get('day_condition')
        row['owm_day_low_rain'] = owm_info.get('day_low_rain')
        row['owm_day_medium_rain'] = owm_info.get('day_medium_rain')
        row['owm_day_high_rain'] = owm_info.get('day_high_rain')
        row['owm_total_rain_expected'] = owm_info.get('total_rain_expected')
        row['owm_total_snow_expected'] = owm_info.get('total_snow_expected')
        row['owm_pop_probability'] = owm_info.get('pop_probability')
        row['owm_temp_max'] = owm_info.get('temp_max')
        row['owm_temp_min'] = owm_info.get('temp_min')
        row['owm_humidity'] = owm_info.get('humidity')
        row['owm_wind_speed'] = owm_info.get('wind_speed')
        row['owm_severity_score'] = owm_info.get('severity_score')
        row['owm_alert_tags'] = owm_info.get('alert_tags')
        row['owm_latitude'] = owm_info.get('latitude')
        row['owm_longitude'] = owm_info.get('longitude')
        
        # Calculate derived weather features
        if owm_info.get('temp_max') and owm_info.get('temp_min'):
            row['owm_temp_range'] = owm_info.get('temp_max') - owm_info.get('temp_min')
        
        # Weather impact flags
        row['owm_has_precipitation'] = 1 if (owm_info.get('total_rain_expected', 0) > 0 or 
                                              owm_info.get('total_snow_expected', 0) > 0) else 0
        row['owm_has_severe_weather'] = 1 if owm_info.get('severity_score', 0) >= 6 else 0
        row['owm_has_alerts'] = 1 if owm_info.get('alert_tags') else 0
        row['owm_is_extreme_cold'] = 1 if owm_info.get('temp_min', 100) < 20 else 0
        row['owm_is_extreme_heat'] = 1 if owm_info.get('temp_max', 0) > 95 else 0
    
    return row


def load_all_weather_data() -> Tuple[Dict, Dict, Dict]:
    """
    Load weather data from all providers.
    
    Returns:
        Tuple of (visualcrossing_data, accuweather_data, openweathermap_data) dictionaries
    """
    vc_data = load_visualcrossing_data()
    accu_data = load_accuweather_data()
    owm_data = load_openweathermap_data()
    return vc_data, accu_data, owm_data
