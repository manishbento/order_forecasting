"""
VisualCrossing Weather Fetcher
==============================
Fetches weather forecast data from VisualCrossing API.

This script can be run standalone to fetch weather data for all stores:
    python -m weather.fetch_visualcrossing

The data is stored in JSON files and then processed into DuckDB.

Weather Severity Scoring System:
--------------------------------
The severity score is calculated from multiple weather factors to determine
the impact on sales forecasting. Higher severity = more weather impact.

Factors considered:
- Precipitation (rain/snow) amount and probability
- Wind speed and gusts
- Visibility
- Extreme temperatures
- Severe weather risk
- Weather conditions (thunderstorm, fog, etc.)

Severity Score Scale (0-10):
- 0-2: No/minimal impact (clear, mild conditions)
- 3-4: Low impact (light rain, breezy)
- 5-6: Moderate impact (steady rain, windy)
- 7-8: High impact (heavy rain, snow, storms)
- 9-10: Severe impact (dangerous conditions, blizzard, etc.)
"""

import os
import sys
import json
import glob
import duckdb
import pandas as pd
import requests
from datetime import datetime
from typing import Dict, List, Tuple, Any

# Add parent directory to path for imports when running standalone
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import settings


# =============================================================================
# CONFIGURATION
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(SCRIPT_DIR, "visualcrossing_data")
DB_PATH = os.path.join(settings.DATA_STORE_DIR, "weather.db")

# Weather severity thresholds
WEATHER_THRESHOLDS = {
    # Rain thresholds (inches)
    'rain_light': 0.1,        # Light rain
    'rain_moderate': 0.25,    # Moderate rain
    'rain_heavy': 0.5,        # Heavy rain
    'rain_extreme': 1.0,      # Extreme rain
    
    # Snow thresholds (inches)
    'snow_light': 1.0,        # Light snow
    'snow_moderate': 3.0,     # Moderate snow
    'snow_heavy': 6.0,        # Heavy snow
    'snow_extreme': 12.0,     # Blizzard conditions
    
    # Wind thresholds (mph)
    'wind_breezy': 15,        # Breezy
    'wind_windy': 25,         # Windy
    'wind_high': 40,          # High winds
    'wind_extreme': 58,       # Storm force
    
    # Temperature thresholds (Fahrenheit)
    'temp_cold': 32,          # Freezing
    'temp_very_cold': 20,     # Very cold
    'temp_extreme_cold': 0,   # Extreme cold
    'temp_hot': 90,           # Hot
    'temp_very_hot': 100,     # Very hot
    'temp_extreme_hot': 110,  # Extreme heat
    
    # Visibility thresholds (miles)
    'visibility_reduced': 5,  # Reduced visibility
    'visibility_low': 1,      # Low visibility
    'visibility_poor': 0.25,  # Poor visibility (fog)
}

# Condition keywords that impact severity
SEVERE_CONDITIONS = {
    # Severe conditions
    'thunderstorm': 3.5,
    'thunder': 3.0,
    'lightning': 3.0,
    'hail': 4.5,
    'tornado': 5.0,
    'hurricane': 5.0,
    'blizzard': 5.0,
    'ice': 4.0,
    'freezing rain': 4.5,
    'sleet': 3.5,
    
    # Common precipitation conditions - ADDED
    'heavy rain': 3.0,
    'rain': 1.5,
    'drizzle': 0.5,
    'heavy snow': 3.5,
    'snow': 2.0,
    'flurries': 1.0,
    'wintry mix': 3.0,
    
    # Visibility conditions
    'fog': 2.0,
    'mist': 1.0,
    'haze': 0.5,
    'smoke': 2.5,
    'dust': 2.0,
    
    # Wind conditions
    'windy': 1.5,
    'breezy': 0.5,
}

# Ensure directories exist
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


# =============================================================================
# SEVERITY CALCULATION FUNCTIONS
# =============================================================================

def calculate_rain_severity(precip: float, precip_prob: float) -> float:
    """
    Calculate rain severity score (0-10 scale).
    
    Args:
        precip: Precipitation amount in inches
        precip_prob: Precipitation probability (0-100)
        
    Returns:
        Rain severity score (0-10)
    """
    if precip <= 0 or precip_prob <= 0:
        return 0.0
    
    # Adjust precip by probability (weighted expected value)
    prob_factor = precip_prob / 100.0
    effective_precip = precip * prob_factor
    
    # Calculate base severity from amount
    if effective_precip >= WEATHER_THRESHOLDS['rain_extreme']:
        base_severity = 10.0
    elif effective_precip >= WEATHER_THRESHOLDS['rain_heavy']:
        base_severity = 7.0 + 3.0 * (effective_precip - WEATHER_THRESHOLDS['rain_heavy']) / (WEATHER_THRESHOLDS['rain_extreme'] - WEATHER_THRESHOLDS['rain_heavy'])
    elif effective_precip >= WEATHER_THRESHOLDS['rain_moderate']:
        base_severity = 4.0 + 3.0 * (effective_precip - WEATHER_THRESHOLDS['rain_moderate']) / (WEATHER_THRESHOLDS['rain_heavy'] - WEATHER_THRESHOLDS['rain_moderate'])
    elif effective_precip >= WEATHER_THRESHOLDS['rain_light']:
        base_severity = 2.0 + 2.0 * (effective_precip - WEATHER_THRESHOLDS['rain_light']) / (WEATHER_THRESHOLDS['rain_moderate'] - WEATHER_THRESHOLDS['rain_light'])
    else:
        base_severity = 2.0 * effective_precip / WEATHER_THRESHOLDS['rain_light']
    
    return min(10.0, base_severity)


def calculate_snow_severity(snow: float, snow_depth: float = 0) -> float:
    """
    Calculate snow severity score (0-10 scale).
    
    Snow depth (accumulation on ground) is a critical factor for travel conditions.
    Existing snow depth makes roads dangerous even without new snow.
    
    Args:
        snow: Snowfall amount in inches (new snow)
        snow_depth: Existing snow depth in inches (accumulation)
        
    Returns:
        Snow severity score (0-10)
    """
    if snow <= 0 and snow_depth <= 0:
        return 0.0
    
    # New snow has higher impact - directly affects travel during the day
    new_snow_impact = snow
    
    # Existing snow depth also impacts travel - especially above 4 inches
    # Roads may be icy, parking lots not cleared, sidewalks dangerous
    if snow_depth >= 12:
        depth_impact = 4.0  # Major accumulation - significant travel hazard
    elif snow_depth >= 8:
        depth_impact = 3.0  # Heavy accumulation
    elif snow_depth >= 4:
        depth_impact = 2.0  # Moderate accumulation
    elif snow_depth >= 2:
        depth_impact = 1.0  # Light accumulation
    else:
        depth_impact = snow_depth * 0.5  # Minimal accumulation
    
    # Combined impact - new snow is weighted more heavily
    total_impact = new_snow_impact + depth_impact
    
    if total_impact >= WEATHER_THRESHOLDS['snow_extreme']:
        return 10.0
    elif total_impact >= WEATHER_THRESHOLDS['snow_heavy']:
        return 7.0 + 3.0 * (total_impact - WEATHER_THRESHOLDS['snow_heavy']) / (WEATHER_THRESHOLDS['snow_extreme'] - WEATHER_THRESHOLDS['snow_heavy'])
    elif total_impact >= WEATHER_THRESHOLDS['snow_moderate']:
        return 4.0 + 3.0 * (total_impact - WEATHER_THRESHOLDS['snow_moderate']) / (WEATHER_THRESHOLDS['snow_heavy'] - WEATHER_THRESHOLDS['snow_moderate'])
    elif total_impact >= WEATHER_THRESHOLDS['snow_light']:
        return 2.0 + 2.0 * (total_impact - WEATHER_THRESHOLDS['snow_light']) / (WEATHER_THRESHOLDS['snow_moderate'] - WEATHER_THRESHOLDS['snow_light'])
    else:
        return 2.0 * total_impact / WEATHER_THRESHOLDS['snow_light']


def calculate_wind_severity(wind_speed: float, wind_gust: float = None) -> float:
    """
    Calculate wind severity score (0-10 scale).
    
    Args:
        wind_speed: Sustained wind speed in mph
        wind_gust: Wind gust speed in mph
        
    Returns:
        Wind severity score (0-10)
    """
    # Use the higher of sustained or gust (weighted)
    effective_wind = max(wind_speed, (wind_gust or 0) * 0.8)
    
    if effective_wind >= WEATHER_THRESHOLDS['wind_extreme']:
        return 10.0
    elif effective_wind >= WEATHER_THRESHOLDS['wind_high']:
        return 6.0 + 4.0 * (effective_wind - WEATHER_THRESHOLDS['wind_high']) / (WEATHER_THRESHOLDS['wind_extreme'] - WEATHER_THRESHOLDS['wind_high'])
    elif effective_wind >= WEATHER_THRESHOLDS['wind_windy']:
        return 3.0 + 3.0 * (effective_wind - WEATHER_THRESHOLDS['wind_windy']) / (WEATHER_THRESHOLDS['wind_high'] - WEATHER_THRESHOLDS['wind_windy'])
    elif effective_wind >= WEATHER_THRESHOLDS['wind_breezy']:
        return 1.0 + 2.0 * (effective_wind - WEATHER_THRESHOLDS['wind_breezy']) / (WEATHER_THRESHOLDS['wind_windy'] - WEATHER_THRESHOLDS['wind_breezy'])
    else:
        return effective_wind / WEATHER_THRESHOLDS['wind_breezy']


def calculate_visibility_severity(visibility: float) -> float:
    """
    Calculate visibility severity score (0-10 scale).
    
    Args:
        visibility: Visibility in miles
        
    Returns:
        Visibility severity score (0-10)
    """
    if visibility is None or visibility >= 10:
        return 0.0
    
    if visibility <= WEATHER_THRESHOLDS['visibility_poor']:
        return 8.0 + 2.0 * (WEATHER_THRESHOLDS['visibility_poor'] - visibility) / WEATHER_THRESHOLDS['visibility_poor']
    elif visibility <= WEATHER_THRESHOLDS['visibility_low']:
        return 5.0 + 3.0 * (WEATHER_THRESHOLDS['visibility_low'] - visibility) / (WEATHER_THRESHOLDS['visibility_low'] - WEATHER_THRESHOLDS['visibility_poor'])
    elif visibility <= WEATHER_THRESHOLDS['visibility_reduced']:
        return 2.0 + 3.0 * (WEATHER_THRESHOLDS['visibility_reduced'] - visibility) / (WEATHER_THRESHOLDS['visibility_reduced'] - WEATHER_THRESHOLDS['visibility_low'])
    else:
        return 2.0 * (10 - visibility) / (10 - WEATHER_THRESHOLDS['visibility_reduced'])


def calculate_temperature_severity(temp_max: float, temp_min: float) -> float:
    """
    Calculate temperature severity score (0-10 scale).
    
    Both extreme heat and cold impact sales.
    
    Args:
        temp_max: Maximum temperature in Fahrenheit
        temp_min: Minimum temperature in Fahrenheit
        
    Returns:
        Temperature severity score (0-10)
    """
    cold_severity = 0.0
    heat_severity = 0.0
    
    # Cold severity
    if temp_min <= WEATHER_THRESHOLDS['temp_extreme_cold']:
        cold_severity = 8.0 + 2.0 * (WEATHER_THRESHOLDS['temp_extreme_cold'] - temp_min) / 20
    elif temp_min <= WEATHER_THRESHOLDS['temp_very_cold']:
        cold_severity = 5.0 + 3.0 * (WEATHER_THRESHOLDS['temp_very_cold'] - temp_min) / (WEATHER_THRESHOLDS['temp_very_cold'] - WEATHER_THRESHOLDS['temp_extreme_cold'])
    elif temp_min <= WEATHER_THRESHOLDS['temp_cold']:
        cold_severity = 2.0 + 3.0 * (WEATHER_THRESHOLDS['temp_cold'] - temp_min) / (WEATHER_THRESHOLDS['temp_cold'] - WEATHER_THRESHOLDS['temp_very_cold'])
    
    # Heat severity
    if temp_max >= WEATHER_THRESHOLDS['temp_extreme_hot']:
        heat_severity = 8.0 + 2.0 * (temp_max - WEATHER_THRESHOLDS['temp_extreme_hot']) / 10
    elif temp_max >= WEATHER_THRESHOLDS['temp_very_hot']:
        heat_severity = 5.0 + 3.0 * (temp_max - WEATHER_THRESHOLDS['temp_very_hot']) / (WEATHER_THRESHOLDS['temp_extreme_hot'] - WEATHER_THRESHOLDS['temp_very_hot'])
    elif temp_max >= WEATHER_THRESHOLDS['temp_hot']:
        heat_severity = 2.0 + 3.0 * (temp_max - WEATHER_THRESHOLDS['temp_hot']) / (WEATHER_THRESHOLDS['temp_very_hot'] - WEATHER_THRESHOLDS['temp_hot'])
    
    return min(10.0, max(cold_severity, heat_severity))


def calculate_condition_severity(conditions: str) -> float:
    """
    Calculate severity from weather condition text.
    
    Args:
        conditions: Weather condition description string
        
    Returns:
        Condition severity score (0-5)
    """
    if not conditions:
        return 0.0
    
    conditions_lower = conditions.lower()
    max_severity = 0.0
    
    for keyword, severity in SEVERE_CONDITIONS.items():
        if keyword in conditions_lower:
            max_severity = max(max_severity, severity)
    
    return max_severity


def calculate_composite_severity(
    rain_severity: float,
    snow_severity: float,
    wind_severity: float,
    visibility_severity: float,
    temp_severity: float,
    condition_severity: float,
    severe_risk: float = 10,
    cloud_cover: float = 0,
    precip_cover: float = 0
) -> tuple:
    """
    Calculate composite weather severity score.
    
    BASED ONLY ON ACTUAL PRECIPITATION AMOUNTS (rain_severity, snow_severity).
    
    NOT used:
    - Temperature (people shop in cold/hot weather)
    - Condition text keywords (misleading - may say "snow" with 0 accumulation)
    
    Only factors that impact shopping:
    1. rain_severity - calculated from actual rain amount in inches
    2. snow_severity - calculated from actual snow amount in inches
    3. severe_risk - API's storm risk score (thunderstorms, hail, tornadoes)
    
    Secondary factors (only compound with significant precipitation):
    - Wind + precipitation = worse driving conditions
    - Poor visibility + precipitation = dangerous travel
    
    severerisk interpretation (from VisualCrossing docs):
    - <30: Low risk of convective storms
    - 30-70: Moderate risk
    - >70: High risk (thunderstorms, hail, tornadoes)
    
    Args:
        rain_severity: Rain severity score (0-10) based on actual rain amount
        snow_severity: Snow severity score (0-10) based on actual snow amount
        wind_severity: Wind severity score (0-10)
        visibility_severity: Visibility severity score (0-10)
        temp_severity: Temperature severity score (0-10) - NOT USED
        condition_severity: Condition-based severity (0-5) - NOT USED
        severe_risk: VisualCrossing severe risk score (0-100)
        cloud_cover: Cloud cover percentage (0-100) - NOT USED
        precip_cover: Proportion of hours with precipitation (0-100)
        
    Returns:
        Tuple of (composite_score, severity_category)
    """
    # ========================================================================
    # PRIMARY FACTOR: ACTUAL PRECIPITATION AMOUNTS ONLY
    # rain_severity and snow_severity are calculated from real inches of precip
    # ========================================================================
    precip_severity = max(rain_severity, snow_severity)
    
    # NOTE: We do NOT use condition_severity anymore - it's based on text keywords
    # which can be misleading (e.g., "Snow, Rain" with 0 actual precipitation)
    
    # ========================================================================
    # SEVERE RISK ASSESSMENT (from VisualCrossing API)
    # This captures thunderstorms, hail, tornadoes - actual dangerous weather
    # Only applies if severe_risk is genuinely high (>=30)
    # ========================================================================
    severe_risk_severity = 0.0
    if severe_risk is not None:
        if severe_risk >= 70:
            # High risk - dangerous storms possible
            severe_risk_severity = 8.0 + (severe_risk - 70) / 30 * 2  # 8-10
        elif severe_risk >= 50:
            # Moderate-high risk
            severe_risk_severity = 5.0 + (severe_risk - 50) / 20 * 3  # 5-8
        elif severe_risk >= 30:
            # Moderate risk
            severe_risk_severity = 2.0 + (severe_risk - 30) / 20 * 3  # 2-5
        # Below 30 = no significant storm risk, don't add to severity
    
    # ========================================================================
    # BASE SCORE CALCULATION
    # Based ONLY on actual precipitation amounts and severe storm risk
    # ========================================================================
    base_score = max(
        precip_severity,           # Rain or snow amount (actual inches)
        severe_risk_severity       # Storm risk from API (only if >= 30)
    )
    
    # ========================================================================
    # COMPOUNDING EFFECTS
    # Only apply when there IS significant precipitation (>= 2.0 severity)
    # ========================================================================
    compounding_bonus = 0.0
    
    if precip_severity >= 2:
        # Precipitation + wind = driving rain/snow, worse conditions
        if wind_severity >= 2:
            compounding_bonus += min(1.5, wind_severity * 0.25)
        
        # Precipitation + poor visibility = dangerous driving
        if visibility_severity >= 2:
            compounding_bonus += min(1.5, visibility_severity * 0.25)
        
        # Snow is inherently more impactful than rain (travel hazard, accumulation)
        if snow_severity > rain_severity:
            compounding_bonus += min(2.0, snow_severity * 0.25)
        
        # Severe weather risk compounds with existing precipitation
        if severe_risk_severity >= 3:
            compounding_bonus += min(1.5, severe_risk_severity * 0.2)
    
    # ========================================================================
    # DURATION/COVERAGE FACTORS
    # All-day rain/snow is worse than brief shower
    # Only applies if there's actual precipitation
    # ========================================================================
    if precip_cover is not None and precip_cover > 0 and precip_severity >= 1:
        if precip_cover >= 75:
            # Most of day has precipitation - significant impact
            compounding_bonus += min(1.5, precip_severity * 0.20)
        elif precip_cover >= 50:
            compounding_bonus += min(1.0, precip_severity * 0.15)
        elif precip_cover >= 25:
            compounding_bonus += min(0.5, precip_severity * 0.10)
    
    # ========================================================================
    # CALCULATE FINAL COMPOSITE
    # ========================================================================
    composite_score = base_score + compounding_bonus
    
    # Cap at 10
    composite_score = min(10.0, max(0.0, composite_score))
    
    # Determine category with adjusted thresholds
    if composite_score >= 7:
        category = 'SEVERE'
    elif composite_score >= 5:
        category = 'HIGH'
    elif composite_score >= 3:
        category = 'MODERATE'
    elif composite_score >= 1.5:
        category = 'LOW'
    else:
        category = 'MINIMAL'
    
    return round(composite_score, 2), category


def calculate_sales_impact_factor(severity_score: float, severity_category: str) -> float:
    """
    Convert severity score to a sales impact factor (multiplier).
    
    This factor is used to adjust forecasted quantities:
    - Factor of 1.0 = no adjustment
    - Factor < 1.0 = reduce forecast (bad weather expected)
    
    The relationship is non-linear - moderate weather has small impact,
    but severe weather has progressively larger impact.
    
    Args:
        severity_score: Composite severity score (0-10)
        severity_category: Severity category string
        
    Returns:
        Sales impact factor (0.5 - 1.0)
    """
    if severity_score <= 2:
        # Minimal weather - no impact
        return 1.0
    elif severity_score <= 4:
        # Low weather - slight impact (up to 5% reduction)
        return 1.0 - (severity_score - 2) * 0.025
    elif severity_score <= 6:
        # Moderate weather - moderate impact (5-15% reduction)
        return 0.95 - (severity_score - 4) * 0.05
    elif severity_score <= 8:
        # High weather - significant impact (15-30% reduction)
        return 0.85 - (severity_score - 6) * 0.075
    else:
        # Severe weather - major impact (30-50% reduction)
        return 0.70 - (severity_score - 8) * 0.10



def get_weather_data(postal_code: str, start_date: str, end_date: str, 
                     store_no: str, api_key: str = None):
    """
    Fetch weather data from VisualCrossing API.
    
    Args:
        postal_code: Store's postal code
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        store_no: Store number for file naming
        api_key: VisualCrossing API key
    """
    api_key = api_key or settings.VISUALCROSSING_API_KEY
    
    filename = f"{store_no}_{start_date}_{end_date}.json"
    filepath = os.path.join(JSON_DIR, filename)

    # Skip if file exists
    if os.path.exists(filepath):
        print(f"Weather data for store {store_no} already exists. Skipping.")
        return

    print(f"Fetching weather for store {store_no} ({postal_code}): {start_date} to {end_date}")
    
    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{postal_code}/{start_date}/{end_date}?key={api_key}"
    )
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
            print(f"Weather data saved to {filepath}")
        else:
            print(f"Failed to fetch weather: {response.status_code} {response.text}")
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def fetch_weather_for_all_stores(stores_df: pd.DataFrame, 
                                 start_date: str, end_date: str):
    """
    Fetch weather data for all stores.
    
    Args:
        stores_df: DataFrame with 'postal_code' and 'store_no' columns
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    """
    for _, row in stores_df.iterrows():
        postal_code = row["postal_code"]
        store_no = str(row["store_no"])
        get_weather_data(postal_code, start_date, end_date, store_no)


def process_weather_files(db_path: str = None, force_purge: bool = False):
    """
    Process all weather JSON files and load into DuckDB.
    
    Aggregates hourly data (8am-9pm) for each day and upserts
    results into the 'weather' table. Calculates severity scores
    for each day based on multiple weather factors.
    
    Args:
        db_path: Path to DuckDB database
        force_purge: If True, drops existing table
    """
    db_path = db_path or DB_PATH
    print("Processing VisualCrossing weather files...")

    conn = duckdb.connect(db_path)

    if force_purge:
        print("Force purge enabled. Dropping existing 'weather' table...")
        conn.execute("DROP TABLE IF EXISTS weather")

    # Create table with enhanced schema for severity scoring
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            store_no VARCHAR,
            date DATE,
            
            -- Basic condition info
            day_condition VARCHAR,
            day_description VARCHAR,
            day_icon VARCHAR,
            
            -- Rain metrics
            day_low_rain INTEGER,
            day_medium_rain INTEGER,
            day_high_rain INTEGER,
            total_rain_expected REAL,
            total_rain_actual REAL,
            precip_probability REAL,
            precip_cover REAL,
            precip_type VARCHAR,
            
            -- Snow metrics
            snow_amount REAL,
            snow_depth REAL,
            
            -- Wind metrics
            wind_speed REAL,
            wind_gust REAL,
            wind_direction REAL,
            
            -- Temperature metrics
            temp_max REAL,
            temp_min REAL,
            temp_avg REAL,
            feels_like_max REAL,
            feels_like_min REAL,
            feels_like_avg REAL,
            dew_point REAL,
            humidity REAL,
            
            -- Visibility and pressure
            visibility REAL,
            pressure REAL,
            cloud_cover REAL,
            
            -- Solar metrics
            solar_radiation REAL,
            solar_energy REAL,
            uv_index REAL,
            
            -- Severe weather indicators
            severe_risk REAL,
            
            -- Individual severity scores (0-10)
            rain_severity REAL,
            snow_severity REAL,
            wind_severity REAL,
            visibility_severity REAL,
            temp_severity REAL,
            condition_severity REAL,
            
            -- Composite severity metrics
            severity_score REAL,
            severity_category VARCHAR,
            sales_impact_factor REAL,
            
            -- Store location info
            latitude REAL,
            longitude REAL,
            resolved_address VARCHAR,
            timezone VARCHAR,
            
            -- Hourly analysis for business hours
            business_hours_avg_precip REAL,
            business_hours_max_precip REAL,
            business_hours_rain_hours INTEGER,
            business_hours_conditions VARCHAR,
            
            PRIMARY KEY (store_no, date)
        )
    """)

    # Find all JSON files
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
            store_no = filename.split('_')[0]

            with open(filepath, 'r') as f:
                weather_json = json.load(f)

            # Extract metadata
            latitude = weather_json.get('latitude')
            longitude = weather_json.get('longitude')
            resolved_address = weather_json.get('resolvedAddress')
            timezone = weather_json.get('timezone')

            # Process each day
            for day in weather_json.get('days', []):
                date = day.get('datetime')
                
                # Extract daily metrics
                temp_max = float(day.get('tempmax', 0) or 0)
                temp_min = float(day.get('tempmin', 0) or 0)
                temp_avg = float(day.get('temp', 0) or 0)
                feels_like_max = float(day.get('feelslikemax', 0) or 0)
                feels_like_min = float(day.get('feelslikemin', 0) or 0)
                feels_like_avg = float(day.get('feelslike', 0) or 0)
                dew_point = float(day.get('dew', 0) or 0)
                humidity = float(day.get('humidity', 0) or 0)
                
                precip = float(day.get('precip', 0) or 0)
                precip_prob = float(day.get('precipprob', 0) or 0)
                precip_cover = float(day.get('precipcover', 0) or 0)
                precip_type = ','.join(day.get('preciptype', []) or [])
                
                snow = float(day.get('snow', 0) or 0)
                snow_depth = float(day.get('snowdepth', 0) or 0)
                
                wind_speed = float(day.get('windspeed', 0) or 0)
                wind_gust = float(day.get('windgust', 0) or 0)
                wind_dir = float(day.get('winddir', 0) or 0)
                
                pressure = float(day.get('pressure', 0) or 0)
                visibility = float(day.get('visibility', 15) or 15)
                cloud_cover = float(day.get('cloudcover', 0) or 0)
                
                solar_radiation = float(day.get('solarradiation', 0) or 0)
                solar_energy = float(day.get('solarenergy', 0) or 0)
                uv_index = float(day.get('uvindex', 0) or 0)
                
                severe_risk = float(day.get('severerisk', 10) or 10)
                
                conditions = day.get('conditions', '')
                description = day.get('description', '')
                icon = day.get('icon', '')
                
                # Process business hours (8am-9pm)
                hourly_precip = []
                hourly_conditions = []
                rain_hours = 0
                
                for hour in day.get('hours', []):
                    hour_time = hour.get('datetime')
                    if "08:00:00" <= hour_time <= "21:00:00":
                        hourly_conditions.append(hour.get('conditions'))
                        
                        hour_precip = float(hour.get('precip', 0) or 0)
                        hour_precip_prob = float(hour.get('precipprob', 0) or 0) / 100.0
                        expected_precip = hour_precip * hour_precip_prob
                        hourly_precip.append(expected_precip)
                        
                        if hour_precip > 0 and hour_precip_prob > 0.3:
                            rain_hours += 1
                
                # Calculate business hours metrics
                if hourly_precip:
                    business_hours_avg_precip = sum(hourly_precip) / len(hourly_precip)
                    business_hours_max_precip = max(hourly_precip)
                else:
                    business_hours_avg_precip = 0
                    business_hours_max_precip = 0
                
                # Most common condition during business hours
                if hourly_conditions:
                    cond_series = pd.Series(hourly_conditions).dropna()
                    if not cond_series.empty:
                        mode_val = cond_series.mode()
                        business_hours_conditions = mode_val[0] if len(mode_val) > 0 else 'Unknown'
                    else:
                        business_hours_conditions = 'Unknown'
                else:
                    business_hours_conditions = conditions
                
                # Calculate rain levels
                total_rain_expected = sum(hourly_precip) if hourly_precip else precip * (precip_prob / 100)
                
                precip_series = pd.Series([float(h.get('precip', 0) or 0) for h in day.get('hours', []) 
                                          if "08:00:00" <= h.get('datetime', '') <= "21:00:00"])
                
                if not precip_series.empty:
                    day_low_rain = 1 if (precip_series.gt(0) & precip_series.le(0.1)).any() else 0
                    day_medium_rain = 1 if (precip_series.gt(0.1) & precip_series.le(0.5)).any() else 0
                    day_high_rain = 1 if precip_series.gt(0.5).any() else 0
                else:
                    day_low_rain = 1 if 0 < precip <= 0.1 else 0
                    day_medium_rain = 1 if 0.1 < precip <= 0.5 else 0
                    day_high_rain = 1 if precip > 0.5 else 0
                
                # Calculate individual severity scores
                rain_severity = calculate_rain_severity(precip, precip_prob)
                snow_severity = calculate_snow_severity(snow, snow_depth)
                wind_severity = calculate_wind_severity(wind_speed, wind_gust)
                visibility_severity = calculate_visibility_severity(visibility)
                temp_severity = calculate_temperature_severity(temp_max, temp_min)
                condition_severity = calculate_condition_severity(conditions)
                
                # Calculate composite severity with all factors
                severity_score, severity_category = calculate_composite_severity(
                    rain_severity, snow_severity, wind_severity,
                    visibility_severity, temp_severity, condition_severity,
                    severe_risk, cloud_cover, precip_cover
                )
                
                # Calculate sales impact factor
                sales_impact_factor = calculate_sales_impact_factor(severity_score, severity_category)

                all_weather_data.append((
                    store_no, date,
                    conditions, description, icon,
                    day_low_rain, day_medium_rain, day_high_rain,
                    total_rain_expected, precip, precip_prob, precip_cover, precip_type,
                    snow, snow_depth,
                    wind_speed, wind_gust, wind_dir,
                    temp_max, temp_min, temp_avg,
                    feels_like_max, feels_like_min, feels_like_avg,
                    dew_point, humidity,
                    visibility, pressure, cloud_cover,
                    solar_radiation, solar_energy, uv_index,
                    severe_risk,
                    rain_severity, snow_severity, wind_severity,
                    visibility_severity, temp_severity, condition_severity,
                    severity_score, severity_category, sales_impact_factor,
                    latitude, longitude, resolved_address, timezone,
                    business_hours_avg_precip, business_hours_max_precip,
                    rain_hours, business_hours_conditions
                ))

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            import traceback
            traceback.print_exc()

    # Insert into DuckDB
    if all_weather_data:
        df_to_insert = pd.DataFrame(
            all_weather_data,
            columns=[
                'store_no', 'date',
                'day_condition', 'day_description', 'day_icon',
                'day_low_rain', 'day_medium_rain', 'day_high_rain',
                'total_rain_expected', 'total_rain_actual', 'precip_probability', 'precip_cover', 'precip_type',
                'snow_amount', 'snow_depth',
                'wind_speed', 'wind_gust', 'wind_direction',
                'temp_max', 'temp_min', 'temp_avg',
                'feels_like_max', 'feels_like_min', 'feels_like_avg',
                'dew_point', 'humidity',
                'visibility', 'pressure', 'cloud_cover',
                'solar_radiation', 'solar_energy', 'uv_index',
                'severe_risk',
                'rain_severity', 'snow_severity', 'wind_severity',
                'visibility_severity', 'temp_severity', 'condition_severity',
                'severity_score', 'severity_category', 'sales_impact_factor',
                'latitude', 'longitude', 'resolved_address', 'timezone',
                'business_hours_avg_precip', 'business_hours_max_precip',
                'business_hours_rain_hours', 'business_hours_conditions'
            ]
        )

        conn.register("df_to_insert", df_to_insert)
        conn.execute("INSERT OR REPLACE INTO weather SELECT * FROM df_to_insert")
        conn.unregister("df_to_insert")

        print(f"Processed {len(all_weather_data)} records into 'weather' table.")
        
        # Print severity distribution summary
        severity_dist = df_to_insert['severity_category'].value_counts()
        print("\nSeverity Distribution:")
        for cat, count in severity_dist.items():
            print(f"  {cat}: {count}")
        
        avg_impact = df_to_insert['sales_impact_factor'].mean()
        print(f"\nAverage Sales Impact Factor: {avg_impact:.3f}")
    else:
        print("No weather data was processed.")

    conn.close()


# =============================================================================
# STANDALONE EXECUTION
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("VisualCrossing Weather Data Fetcher")
    print("=" * 60)
    
    # Default date range (can be overridden via command line)
    weather_start = settings.FORECAST_START_DATE
    weather_end = settings.FORECAST_END_DATE
    
    print(f"Date range: {weather_start} to {weather_end}")
    
    # Load store data
    try:
        from data.loader import DataLoader
        loader = DataLoader()
        loader.load_store_data()
        stores_df = loader.get_stores_df()
        
        # Fetch weather
        fetch_weather_for_all_stores(stores_df, weather_start, weather_end)
        
        # Process files
        process_weather_files(force_purge=True)
        
        loader.disconnect()
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure to run data loader first to populate stores table.")
