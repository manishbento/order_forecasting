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
    # Rain thresholds (inches) - actual precipitation amount
    'rain_trace': 0.05,       # Trace rain - barely noticeable
    'rain_light': 0.1,        # Light rain - minor inconvenience
    'rain_moderate': 0.25,    # Moderate rain - noticeable, some avoid trips
    'rain_heavy': 0.5,        # Heavy rain - difficult conditions
    'rain_extreme': 1.0,      # Extreme rain - flooding risk
    
    # Snow thresholds (inches) - NEW snowfall
    'snow_trace': 0.5,        # Trace snow - barely accumulates
    'snow_light': 1.0,        # Light snow - some accumulation
    'snow_moderate': 3.0,     # Moderate snow - road treatment needed
    'snow_heavy': 6.0,        # Heavy snow - significant travel impact
    'snow_extreme': 12.0,     # Blizzard conditions
    
    # Snow depth thresholds (inches) - EXISTING accumulation on ground
    'depth_minimal': 2.0,     # Minimal - roads likely clear
    'depth_light': 4.0,       # Light - some roads may be slick
    'depth_moderate': 8.0,    # Moderate - parking lots may be bad
    'depth_heavy': 12.0,      # Heavy - travel hazardous
    
    # Wind thresholds (mph)
    'wind_calm': 10,          # Calm
    'wind_breezy': 15,        # Breezy - noticeable but fine
    'wind_windy': 25,         # Windy - carts, doors difficult
    'wind_high': 40,          # High winds - dangerous with precip
    'wind_extreme': 58,       # Storm force - stay home
    
    # Temperature thresholds (Fahrenheit) - only extremes matter
    'temp_cold': 32,          # Freezing - ice possible with precip
    'temp_very_cold': 15,     # Very cold - unpleasant outdoors
    'temp_extreme_cold': 0,   # Extreme cold - dangerous exposure
    'temp_hot': 95,           # Hot - normal summer day
    'temp_very_hot': 100,     # Very hot - heat advisory
    'temp_extreme_hot': 110,  # Extreme heat - dangerous
    
    # Visibility thresholds (miles)
    'visibility_clear': 10,   # Clear visibility
    'visibility_reduced': 5,  # Reduced visibility - noticeable
    'visibility_low': 1,      # Low visibility - driving difficult
    'visibility_poor': 0.25,  # Poor visibility - fog/blizzard
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
    
    Rain prevents shopping when it's heavy enough to:
    - Make driving unpleasant/dangerous
    - Make loading groceries miserable
    - Flood parking lots
    
    Args:
        precip: Precipitation amount in inches (expected total for day)
        precip_prob: Precipitation probability (0-100)
        
    Returns:
        Rain severity score (0-10)
    """
    if precip <= 0:
        return 0.0
    
    # Weight precipitation by probability
    # Example: 0.5" rain with 60% probability = 0.3" effective
    prob_factor = min(1.0, precip_prob / 100.0) if precip_prob > 0 else 0.5
    effective_precip = precip * prob_factor
    
    # No impact if trace amounts
    if effective_precip < WEATHER_THRESHOLDS['rain_trace']:
        return 0.0
    
    # Calculate severity based on effective precipitation
    # Using continuous scale for smoother transitions
    if effective_precip >= WEATHER_THRESHOLDS['rain_extreme']:
        # Extreme: 8-10
        excess = effective_precip - WEATHER_THRESHOLDS['rain_extreme']
        return min(10.0, 8.0 + min(2.0, excess * 2.0))
    elif effective_precip >= WEATHER_THRESHOLDS['rain_heavy']:
        # Heavy: 6-8
        range_size = WEATHER_THRESHOLDS['rain_extreme'] - WEATHER_THRESHOLDS['rain_heavy']
        progress = (effective_precip - WEATHER_THRESHOLDS['rain_heavy']) / range_size
        return 6.0 + 2.0 * progress
    elif effective_precip >= WEATHER_THRESHOLDS['rain_moderate']:
        # Moderate: 4-6
        range_size = WEATHER_THRESHOLDS['rain_heavy'] - WEATHER_THRESHOLDS['rain_moderate']
        progress = (effective_precip - WEATHER_THRESHOLDS['rain_moderate']) / range_size
        return 4.0 + 2.0 * progress
    elif effective_precip >= WEATHER_THRESHOLDS['rain_light']:
        # Light: 2-4
        range_size = WEATHER_THRESHOLDS['rain_moderate'] - WEATHER_THRESHOLDS['rain_light']
        progress = (effective_precip - WEATHER_THRESHOLDS['rain_light']) / range_size
        return 2.0 + 2.0 * progress
    else:
        # Trace to Light: 0-2
        range_size = WEATHER_THRESHOLDS['rain_light'] - WEATHER_THRESHOLDS['rain_trace']
        progress = (effective_precip - WEATHER_THRESHOLDS['rain_trace']) / range_size
        return 0.0 + 2.0 * progress


def calculate_snow_severity(snow: float, snow_depth: float = 0) -> float:
    """
    Calculate snow severity score (0-10 scale).
    
    Snow impacts shopping through:
    1. NEW SNOWFALL: Active snow makes driving dangerous, visibility poor
    2. EXISTING SNOW DEPTH: Roads/lots may be icy, not fully cleared
    
    Both factors contribute - a day with 2" new snow on top of 8" existing
    is much worse than 2" on bare ground.
    
    Args:
        snow: New snowfall amount in inches (forecast for day)
        snow_depth: Existing snow depth in inches (accumulation on ground)
        
    Returns:
        Snow severity score (0-10)
    """
    if snow <= 0 and snow_depth <= 0:
        return 0.0
    
    # ==========================================================================
    # PART 1: NEW SNOWFALL SEVERITY (0-8)
    # Active snow falling has high impact - visibility, accumulation, slippery
    # ==========================================================================
    new_snow_severity = 0.0
    
    if snow > 0:
        if snow >= WEATHER_THRESHOLDS['snow_extreme']:
            # Blizzard: 8+
            new_snow_severity = 8.0
        elif snow >= WEATHER_THRESHOLDS['snow_heavy']:
            # Heavy: 6-8
            range_size = WEATHER_THRESHOLDS['snow_extreme'] - WEATHER_THRESHOLDS['snow_heavy']
            progress = (snow - WEATHER_THRESHOLDS['snow_heavy']) / range_size
            new_snow_severity = 6.0 + 2.0 * progress
        elif snow >= WEATHER_THRESHOLDS['snow_moderate']:
            # Moderate: 4-6
            range_size = WEATHER_THRESHOLDS['snow_heavy'] - WEATHER_THRESHOLDS['snow_moderate']
            progress = (snow - WEATHER_THRESHOLDS['snow_moderate']) / range_size
            new_snow_severity = 4.0 + 2.0 * progress
        elif snow >= WEATHER_THRESHOLDS['snow_light']:
            # Light: 2-4
            range_size = WEATHER_THRESHOLDS['snow_moderate'] - WEATHER_THRESHOLDS['snow_light']
            progress = (snow - WEATHER_THRESHOLDS['snow_light']) / range_size
            new_snow_severity = 2.0 + 2.0 * progress
        elif snow >= WEATHER_THRESHOLDS['snow_trace']:
            # Trace: 0.5-2
            range_size = WEATHER_THRESHOLDS['snow_light'] - WEATHER_THRESHOLDS['snow_trace']
            progress = (snow - WEATHER_THRESHOLDS['snow_trace']) / range_size
            new_snow_severity = 0.5 + 1.5 * progress
        else:
            # Dusting: 0-0.5
            new_snow_severity = snow / WEATHER_THRESHOLDS['snow_trace'] * 0.5
    
    # ==========================================================================
    # PART 2: EXISTING SNOW DEPTH BONUS (0-5)
    # Ground accumulation affects travel even without new snow
    # - Roads may be icy from melt/refreeze
    # - Parking lots may not be fully cleared
    # - Sidewalks hazardous
    # ==========================================================================
    depth_bonus = 0.0
    
    if snow_depth > 0:
        if snow_depth >= WEATHER_THRESHOLDS['depth_heavy']:
            # 12"+ on ground: +4-5 (travel definitely hazardous)
            excess = snow_depth - WEATHER_THRESHOLDS['depth_heavy']
            depth_bonus = 4.0 + min(1.0, excess / 6.0)  # Caps at +5
        elif snow_depth >= WEATHER_THRESHOLDS['depth_moderate']:
            # 8-12" on ground: +3-4 (significant accumulation)
            range_size = WEATHER_THRESHOLDS['depth_heavy'] - WEATHER_THRESHOLDS['depth_moderate']
            progress = (snow_depth - WEATHER_THRESHOLDS['depth_moderate']) / range_size
            depth_bonus = 3.0 + 1.0 * progress
        elif snow_depth >= WEATHER_THRESHOLDS['depth_light']:
            # 4-8" on ground: +2-3 (roads may be slick)
            range_size = WEATHER_THRESHOLDS['depth_moderate'] - WEATHER_THRESHOLDS['depth_light']
            progress = (snow_depth - WEATHER_THRESHOLDS['depth_light']) / range_size
            depth_bonus = 2.0 + 1.0 * progress
        elif snow_depth >= WEATHER_THRESHOLDS['depth_minimal']:
            # 2-4" on ground: +1-2 (some impact)
            range_size = WEATHER_THRESHOLDS['depth_light'] - WEATHER_THRESHOLDS['depth_minimal']
            progress = (snow_depth - WEATHER_THRESHOLDS['depth_minimal']) / range_size
            depth_bonus = 1.0 + 1.0 * progress
        else:
            # <2" on ground: +0-1 (minimal impact)
            depth_bonus = snow_depth / WEATHER_THRESHOLDS['depth_minimal']
    
    # ==========================================================================
    # COMBINE: New snow severity + Depth bonus, capped at 10
    # ==========================================================================
    total_severity = new_snow_severity + depth_bonus
    
    # Special case: No new snow but significant depth still impacts travel
    if snow <= 0 and snow_depth > 0:
        # Existing depth alone can create moderate severity (ice, uncleared lots)
        total_severity = depth_bonus * 1.5  # Amplify depth impact when sole factor
    
    return min(10.0, total_severity)


def calculate_wind_severity(wind_speed: float, wind_gust: float = None) -> float:
    """
    Calculate wind severity score (0-10 scale).
    
    Wind impacts shopping through:
    - Difficulty controlling shopping carts
    - Doors hard to manage (especially for elderly)
    - Walking to/from store unpleasant
    - Combined with rain/snow: driving conditions worsen
    - Extreme: downed trees, power outages
    
    Args:
        wind_speed: Sustained wind speed in mph
        wind_gust: Wind gust speed in mph (optional)
        
    Returns:
        Wind severity score (0-10)
    """
    if wind_speed <= 0:
        return 0.0
    
    # Use sustained speed, but consider gusts (gusts at 80% weight)
    # Gusts are brief but can be dangerous
    gust_contribution = (wind_gust * 0.8) if wind_gust and wind_gust > wind_speed else 0
    effective_wind = max(wind_speed, gust_contribution)
    
    # Calm winds: no impact
    if effective_wind < WEATHER_THRESHOLDS['wind_calm']:
        return 0.0
    
    # Calculate severity
    if effective_wind >= WEATHER_THRESHOLDS['wind_extreme']:
        # Storm force: 8-10 (dangerous conditions)
        excess = effective_wind - WEATHER_THRESHOLDS['wind_extreme']
        return min(10.0, 8.0 + min(2.0, excess / 15.0))
    elif effective_wind >= WEATHER_THRESHOLDS['wind_high']:
        # High winds: 6-8 (dangerous with precip, difficult outdoors)
        range_size = WEATHER_THRESHOLDS['wind_extreme'] - WEATHER_THRESHOLDS['wind_high']
        progress = (effective_wind - WEATHER_THRESHOLDS['wind_high']) / range_size
        return 6.0 + 2.0 * progress
    elif effective_wind >= WEATHER_THRESHOLDS['wind_windy']:
        # Windy: 3-6 (carts difficult, unpleasant)
        range_size = WEATHER_THRESHOLDS['wind_high'] - WEATHER_THRESHOLDS['wind_windy']
        progress = (effective_wind - WEATHER_THRESHOLDS['wind_windy']) / range_size
        return 3.0 + 3.0 * progress
    elif effective_wind >= WEATHER_THRESHOLDS['wind_breezy']:
        # Breezy: 1-3 (noticeable but manageable)
        range_size = WEATHER_THRESHOLDS['wind_windy'] - WEATHER_THRESHOLDS['wind_breezy']
        progress = (effective_wind - WEATHER_THRESHOLDS['wind_breezy']) / range_size
        return 1.0 + 2.0 * progress
    else:
        # Calm to breezy: 0-1
        range_size = WEATHER_THRESHOLDS['wind_breezy'] - WEATHER_THRESHOLDS['wind_calm']
        progress = (effective_wind - WEATHER_THRESHOLDS['wind_calm']) / range_size
        return 0.0 + 1.0 * progress


def calculate_visibility_severity(visibility: float) -> float:
    """
    Calculate visibility severity score (0-10 scale).
    
    Poor visibility (fog, heavy snow, smoke) makes driving dangerous.
    This primarily impacts willingness to travel, not shopping itself.
    
    Args:
        visibility: Visibility in miles
        
    Returns:
        Visibility severity score (0-10)
    """
    if visibility is None or visibility >= WEATHER_THRESHOLDS['visibility_clear']:
        return 0.0
    
    if visibility <= WEATHER_THRESHOLDS['visibility_poor']:
        # Dense fog/blizzard: 8-10 (driving dangerous)
        # Below 0.25 miles, can't see intersection ahead
        return min(10.0, 8.0 + 2.0 * (WEATHER_THRESHOLDS['visibility_poor'] - visibility) / WEATHER_THRESHOLDS['visibility_poor'])
    elif visibility <= WEATHER_THRESHOLDS['visibility_low']:
        # Low visibility: 5-8 (driving difficult)
        range_size = WEATHER_THRESHOLDS['visibility_low'] - WEATHER_THRESHOLDS['visibility_poor']
        progress = (WEATHER_THRESHOLDS['visibility_low'] - visibility) / range_size
        return 5.0 + 3.0 * progress
    elif visibility <= WEATHER_THRESHOLDS['visibility_reduced']:
        # Reduced visibility: 2-5 (noticeable)
        range_size = WEATHER_THRESHOLDS['visibility_reduced'] - WEATHER_THRESHOLDS['visibility_low']
        progress = (WEATHER_THRESHOLDS['visibility_reduced'] - visibility) / range_size
        return 2.0 + 3.0 * progress
    else:
        # Slightly reduced: 0-2
        range_size = WEATHER_THRESHOLDS['visibility_clear'] - WEATHER_THRESHOLDS['visibility_reduced']
        progress = (WEATHER_THRESHOLDS['visibility_clear'] - visibility) / range_size
        return 0.0 + 2.0 * progress


def calculate_temperature_severity(temp_max: float, temp_min: float) -> float:
    """
    Calculate temperature severity score (0-10 scale).
    
    IMPORTANT: Temperature alone rarely prevents shopping.
    - People shop in 20°F weather (dress warm, car heat)
    - People shop in 95°F weather (car AC, store AC)
    
    Only EXTREME temperatures impact shopping behavior:
    - Extreme cold (<0°F): Car won't start, frostbite risk
    - Extreme heat (>105°F): Heat stroke risk, car unbearable
    
    Temperature DOES matter when combined with precipitation:
    - Rain at 33°F = possible freezing rain (dangerous)
    - Snow at 34°F = slushy mess, refreezes at night
    
    This function returns low severity for temperature alone.
    The composite function handles temperature + precipitation interaction.
    
    Args:
        temp_max: Maximum temperature in Fahrenheit
        temp_min: Minimum temperature in Fahrenheit
        
    Returns:
        Temperature severity score (0-3, intentionally capped low)
    """
    if temp_min is None and temp_max is None:
        return 0.0
    
    temp_min = temp_min if temp_min is not None else 50
    temp_max = temp_max if temp_max is not None else 70
    
    cold_severity = 0.0
    heat_severity = 0.0
    
    # Cold severity (only extreme cold matters on its own)
    if temp_min <= WEATHER_THRESHOLDS['temp_extreme_cold']:
        # Below 0°F: dangerous cold
        cold_severity = 2.0 + min(1.0, (WEATHER_THRESHOLDS['temp_extreme_cold'] - temp_min) / 20)
    elif temp_min <= WEATHER_THRESHOLDS['temp_very_cold']:
        # 0-15°F: very cold but manageable
        range_size = WEATHER_THRESHOLDS['temp_very_cold'] - WEATHER_THRESHOLDS['temp_extreme_cold']
        progress = (WEATHER_THRESHOLDS['temp_very_cold'] - temp_min) / range_size
        cold_severity = 1.0 + 1.0 * progress
    elif temp_min <= WEATHER_THRESHOLDS['temp_cold']:
        # 15-32°F: cold but normal winter
        range_size = WEATHER_THRESHOLDS['temp_cold'] - WEATHER_THRESHOLDS['temp_very_cold']
        progress = (WEATHER_THRESHOLDS['temp_cold'] - temp_min) / range_size
        cold_severity = 0.0 + 1.0 * progress
    
    # Heat severity (only extreme heat matters on its own)
    if temp_max >= WEATHER_THRESHOLDS['temp_extreme_hot']:
        # Above 110°F: dangerous heat
        heat_severity = 2.0 + min(1.0, (temp_max - WEATHER_THRESHOLDS['temp_extreme_hot']) / 10)
    elif temp_max >= WEATHER_THRESHOLDS['temp_very_hot']:
        # 100-110°F: very hot but AC helps
        range_size = WEATHER_THRESHOLDS['temp_extreme_hot'] - WEATHER_THRESHOLDS['temp_very_hot']
        progress = (temp_max - WEATHER_THRESHOLDS['temp_very_hot']) / range_size
        heat_severity = 1.0 + 1.0 * progress
    elif temp_max >= WEATHER_THRESHOLDS['temp_hot']:
        # 95-100°F: hot but normal summer
        range_size = WEATHER_THRESHOLDS['temp_very_hot'] - WEATHER_THRESHOLDS['temp_hot']
        progress = (temp_max - WEATHER_THRESHOLDS['temp_hot']) / range_size
        heat_severity = 0.0 + 1.0 * progress
    
    # Return max of cold/heat, capped at 3 (temperature alone is minor factor)
    return min(3.0, max(cold_severity, heat_severity))


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
    precip_cover: float = 0,
    temp_min: float = None,
    conditions: str = None
) -> tuple:
    """
    Calculate composite weather severity score (0-10).
    
    This represents HOW LIKELY CUSTOMERS ARE TO STAY HOME due to weather.
    
    PRIMARY FACTORS (what actually prevents shopping):
    1. Precipitation amount (rain or snow) - driving/loading difficulty
    2. Existing snow depth (in snow_severity) - travel hazards
    3. Severe storm risk - dangerous conditions
    4. Ice/freezing conditions - most dangerous road condition
    
    SECONDARY FACTORS (compound the primary):
    - Wind with precipitation - driving rain/snow
    - Poor visibility with precipitation - dangerous driving
    - Precipitation duration/coverage - all-day vs brief
    
    NOT HEAVILY WEIGHTED:
    - Temperature alone (people shop in cold/hot)
    - Cloud cover (doesn't prevent shopping)
    - Condition text keywords (can be misleading)
    
    Args:
        rain_severity: Rain severity score (0-10)
        snow_severity: Snow severity score (0-10), includes snow_depth impact
        wind_severity: Wind severity score (0-10)
        visibility_severity: Visibility severity score (0-10)
        temp_severity: Temperature severity score (0-3, capped low)
        condition_severity: Condition text severity (0-5)
        severe_risk: VisualCrossing severe risk (0-100)
        cloud_cover: Cloud cover percentage (0-100)
        precip_cover: Hours with precipitation percentage (0-100)
        temp_min: Minimum temperature (for ice detection)
        conditions: Weather conditions text (for ice detection)
        
    Returns:
        Tuple of (composite_score, severity_category)
    """
    # ==========================================================================
    # STEP 1: BASE PRECIPITATION SEVERITY
    # The primary driver - actual rain/snow amounts
    # ==========================================================================
    precip_severity = max(rain_severity or 0, snow_severity or 0)
    
    # ==========================================================================
    # STEP 2: SEVERE STORM RISK (from VisualCrossing API)
    # Captures thunderstorms, hail, tornadoes - actual dangerous weather
    # Only applies if severe_risk is genuinely high (>=30)
    # ==========================================================================
    severe_risk_severity = 0.0
    if severe_risk is not None and severe_risk > 0:
        if severe_risk >= 70:
            # High risk - dangerous storms expected
            severe_risk_severity = 8.0 + min(2.0, (severe_risk - 70) / 15)
        elif severe_risk >= 50:
            # Moderate-high risk
            severe_risk_severity = 5.0 + 3.0 * (severe_risk - 50) / 20
        elif severe_risk >= 30:
            # Moderate risk
            severe_risk_severity = 2.0 + 3.0 * (severe_risk - 30) / 20
        # Below 30 = low storm risk, no additional severity
    
    # ==========================================================================
    # STEP 3: ICE/FREEZING CONDITIONS CHECK
    # This is the MOST DANGEROUS road condition
    # Rain or drizzle near freezing can create black ice
    # ==========================================================================
    ice_severity = 0.0
    has_ice_conditions = False
    
    # Check for explicit ice/freezing rain in conditions
    if conditions:
        conditions_lower = conditions.lower()
        if any(ice_word in conditions_lower for ice_word in ['ice', 'freezing rain', 'sleet', 'glaze']):
            has_ice_conditions = True
            ice_severity = 7.0  # Ice is inherently dangerous
    
    # Check for rain near freezing (potential black ice)
    if rain_severity and rain_severity > 0 and temp_min is not None:
        if temp_min <= 34 and temp_min >= 28:
            # Temperature where rain could freeze on contact
            has_ice_conditions = True
            ice_severity = max(ice_severity, 5.0 + rain_severity * 0.3)
    
    # ==========================================================================
    # STEP 4: BASE SCORE CALCULATION
    # Maximum of precipitation, storm risk, and ice conditions
    # ==========================================================================
    base_score = max(
        precip_severity,           # Rain or snow amount
        severe_risk_severity,      # Storm risk from API
        ice_severity               # Ice/freezing conditions
    )
    
    # ==========================================================================
    # STEP 5: COMPOUNDING EFFECTS
    # Only apply when there IS significant base weather (>= 2.0)
    # ==========================================================================
    compounding_bonus = 0.0
    
    if base_score >= 2:
        # Wind makes precipitation worse (driving rain/snow)
        if wind_severity and wind_severity >= 3:
            compounding_bonus += min(1.5, wind_severity * 0.3)
        
        # Poor visibility with precipitation is dangerous
        if visibility_severity and visibility_severity >= 3:
            compounding_bonus += min(1.5, visibility_severity * 0.3)
        
        # Snow inherently worse than rain (accumulation, slippery roads)
        if snow_severity and snow_severity > (rain_severity or 0):
            compounding_bonus += min(1.0, snow_severity * 0.15)
        
        # Ice conditions get extra bonus (worst road condition)
        if has_ice_conditions:
            compounding_bonus += 1.5
        
        # Severe weather risk compounds with existing conditions
        if severe_risk_severity >= 3:
            compounding_bonus += min(1.0, severe_risk_severity * 0.15)
    
    # ==========================================================================
    # STEP 6: DURATION/COVERAGE FACTOR
    # All-day precipitation worse than brief shower
    # ==========================================================================
    if precip_cover is not None and precip_cover > 0 and precip_severity >= 1:
        if precip_cover >= 75:
            # Most of day has precipitation - sustained impact
            compounding_bonus += min(1.0, precip_severity * 0.15)
        elif precip_cover >= 50:
            compounding_bonus += min(0.7, precip_severity * 0.10)
        elif precip_cover >= 25:
            compounding_bonus += min(0.4, precip_severity * 0.06)
    
    # ==========================================================================
    # STEP 7: VISIBILITY STANDALONE (fog can prevent shopping alone)
    # Dense fog without precipitation is still dangerous
    # ==========================================================================
    if visibility_severity and visibility_severity >= 6 and base_score < visibility_severity:
        # Dense fog (visibility < 0.5 miles) can be primary factor
        base_score = max(base_score, visibility_severity * 0.8)
    
    # ==========================================================================
    # STEP 8: CALCULATE FINAL COMPOSITE
    # ==========================================================================
    composite_score = base_score + compounding_bonus
    
    # Cap at 10
    composite_score = min(10.0, max(0.0, composite_score))
    
    # ==========================================================================
    # STEP 9: DETERMINE CATEGORY
    # Based on expected customer behavior
    # ==========================================================================
    if composite_score >= 8:
        category = 'SEVERE'      # Most stay home
    elif composite_score >= 6:
        category = 'HIGH'        # Only essential trips
    elif composite_score >= 4:
        category = 'MODERATE'    # Many avoid unnecessary trips
    elif composite_score >= 2:
        category = 'LOW'         # Some may delay trips
    else:
        category = 'MINIMAL'     # Normal shopping behavior
    
    return round(composite_score, 2), category


def calculate_sales_impact_factor(severity_score: float, severity_category: str) -> float:
    """
    Convert severity score to a sales impact factor (multiplier).
    
    This factor represents expected sales as a proportion of normal:
    - Factor of 1.0 = 100% normal sales (no weather impact)
    - Factor of 0.8 = 80% normal sales (20% reduction expected)
    
    Based on severity categories:
    - MINIMAL (0-2): No significant impact
    - LOW (2-4): Some customers delay trips, 3-7% reduction
    - MODERATE (4-6): Many avoid unnecessary trips, 7-15% reduction
    - HIGH (6-8): Only essential trips, 15-30% reduction
    - SEVERE (8-10): Most stay home, 30-50% reduction
    
    Args:
        severity_score: Composite severity score (0-10)
        severity_category: Severity category string
        
    Returns:
        Sales impact factor (0.50 - 1.00)
    """
    if severity_score < 2:
        # MINIMAL: No meaningful impact
        return 1.00
    elif severity_score < 4:
        # LOW: 3-7% reduction (linear interpolation)
        # severity 2 → 0.97, severity 4 → 0.93
        return 1.00 - (severity_score - 2) * 0.02
    elif severity_score < 6:
        # MODERATE: 7-15% reduction
        # severity 4 → 0.93, severity 6 → 0.85
        return 0.96 - (severity_score - 4) * 0.04
    elif severity_score < 8:
        # HIGH: 15-30% reduction  
        # severity 6 → 0.85, severity 8 → 0.70
        return 0.88 - (severity_score - 6) * 0.075
    else:
        # SEVERE: 30-50% reduction
        # severity 8 → 0.70, severity 10 → 0.50
        return 0.73 - (severity_score - 8) * 0.10



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
                # Use total_rain_expected (business hours only) instead of precip (daily total)
                # total_rain_expected is already probability-weighted, so we pass 100% as probability
                if total_rain_expected > 0:
                    rain_severity = calculate_rain_severity(total_rain_expected, 100)
                else:
                    rain_severity = 0.0
                snow_severity = calculate_snow_severity(snow, snow_depth)
                wind_severity = calculate_wind_severity(wind_speed, wind_gust)
                visibility_severity = calculate_visibility_severity(visibility)
                temp_severity = calculate_temperature_severity(temp_max, temp_min)
                condition_severity = calculate_condition_severity(conditions)
                
                # Calculate composite severity with all factors
                # Pass temp_min and conditions for ice detection
                severity_score, severity_category = calculate_composite_severity(
                    rain_severity, snow_severity, wind_severity,
                    visibility_severity, temp_severity, condition_severity,
                    severe_risk, cloud_cover, precip_cover,
                    temp_min=temp_min, conditions=conditions
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
