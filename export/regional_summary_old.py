"""
Regional Summary Excel Export Module
====================================
Creates comprehensive regional summary reports for stakeholder review.

This module generates a multi-sheet Excel workbook with:
1. Daily Summary - Aggregated metrics by forecast date with trends and expected shrink
2. Store Summary - Daily breakdown by store with weather indicators
3. Weather Impact - Weather impact summary with indicator logos
4. Item Details - Full item/store level detail with weather indicators

The export is designed to provide stakeholders with a complete view of
the forecasting process and weather adjustments.
"""

import os
import polars as pl
import xlsxwriter
from datetime import datetime

from config import settings


# =============================================================================
# WEATHER INDICATOR ICONS (imported from weather_adjustment for consistency)
# =============================================================================

WEATHER_ICONS = {
    'clear': '☀️',
    'partly_cloudy': '⛅',
    'cloudy': '☁️',
    'rain_light': '🌦️',
    'rain': '🌧️',
    'rain_heavy': '⛈️',
    'thunderstorm': '🌩️',
    'snow_light': '🌨️',
    'snow': '❄️',
    'snow_heavy': '🌨️❄️',
    'fog': '🌫️',
    'wind': '💨',
    'extreme_cold': '🥶',
    'extreme_heat': '🥵',
    'severe': '⚠️',
    'unknown': '❓'
}

SEVERITY_ICONS = {
    'MINIMAL': '✅',
    'LOW': '🟢',
    'MODERATE': '🟡',
    'HIGH': '🟠',
    'SEVERE': '🔴'
}


def get_weather_indicator_icon(condition: str, severity_category: str = None,
                                snow_amount: float = 0, rain_amount: float = 0,
                                temp_min: float = None, temp_max: float = None,
                                wind_speed: float = 0, severity_score: float = 0) -> str:
    """
    Get weather indicator icon based on conditions.
    
    Args:
        condition: Weather condition string
        severity_category: Severity category (SEVERE, HIGH, MODERATE, LOW, MINIMAL)
        snow_amount: Snow amount in inches
        rain_amount: Rain amount in inches
        temp_min: Minimum temperature
        temp_max: Maximum temperature
        wind_speed: Wind speed in mph
        severity_score: Overall severity score
        
    Returns:
        Weather indicator icon string
    """
    if not condition:
        return SEVERITY_ICONS.get(severity_category, '❓')
    
    condition_lower = condition.lower()
    
    # Severe conditions
    if severity_score >= 7 or severity_category == 'SEVERE':
        return WEATHER_ICONS['severe']
    
    # Snow
    if snow_amount >= 6 or 'blizzard' in condition_lower:
        return WEATHER_ICONS['snow_heavy']
    if snow_amount >= 2 or 'snow' in condition_lower:
        return WEATHER_ICONS['snow']
    if snow_amount > 0:
        return WEATHER_ICONS['snow_light']
    
    # Rain/storms
    if 'thunder' in condition_lower or 'storm' in condition_lower:
        return WEATHER_ICONS['thunderstorm']
    if rain_amount >= 1:
        return WEATHER_ICONS['rain_heavy']
    if rain_amount >= 0.25 or 'rain' in condition_lower:
        return WEATHER_ICONS['rain']
    if rain_amount > 0 or 'drizzle' in condition_lower or 'shower' in condition_lower:
        return WEATHER_ICONS['rain_light']
    
    # Fog
    if 'fog' in condition_lower or 'mist' in condition_lower:
        return WEATHER_ICONS['fog']
    
    # Wind
    if wind_speed >= 30:
        return WEATHER_ICONS['wind']
    
    # Temperature extremes
    if temp_min is not None and temp_min < 20:
        return WEATHER_ICONS['extreme_cold']
    if temp_max is not None and temp_max > 100:
        return WEATHER_ICONS['extreme_heat']
    
    # Clear/cloudy
    if 'clear' in condition_lower or 'sunny' in condition_lower:
        return WEATHER_ICONS['clear']
    if 'partly' in condition_lower or 'partial' in condition_lower:
        return WEATHER_ICONS['partly_cloudy']
    if 'cloud' in condition_lower or 'overcast' in condition_lower:
        return WEATHER_ICONS['cloudy']
    
    return SEVERITY_ICONS.get(severity_category, WEATHER_ICONS['clear'])


# =============================================================================
# COLOR PALETTES FOR CONDITIONAL FORMATTING
# =============================================================================

# Professional color palette
COLORS = {
    'header_bg': '#2F5496',           # Dark blue
    'header_font': '#FFFFFF',          # White
    'subheader_bg': '#5B9BD5',        # Medium blue
    'subheader_font': '#FFFFFF',       # White
    'section_bg': '#D6DCE5',          # Light gray-blue
    'total_bg': '#E2EFDA',            # Light green
    'alt_row_bg': '#F2F2F2',          # Light gray for alternating rows
    
    # Conditional formatting colors
    'good_bg': '#C6EFCE',             # Light green
    'good_font': '#006100',           # Dark green
    'warning_bg': '#FFEB9C',          # Light yellow/orange
    'warning_font': '#9C6500',        # Dark orange
    'bad_bg': '#FFC7CE',              # Light red
    'bad_font': '#9C0006',            # Dark red
    'neutral_bg': '#F2F2F2',          # Light gray
    
    # Weather severity colors
    'severe_bg': '#FF6B6B',           # Red
    'high_bg': '#FFA94D',             # Orange
    'moderate_bg': '#FFE066',         # Yellow
    'low_bg': '#69DB7C',              # Green
    'minimal_bg': '#A9E34B',          # Light green
}


# =============================================================================
# SQL QUERIES FOR SUMMARY DATA
# =============================================================================

def get_daily_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for daily summary metrics with trends and expected shrink."""
    # Build inactive stores filter
    inactive_stores_filter = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_filter = f"AND fr.store_no NOT IN ({inactive_stores_str})"
    
    return f'''
        SELECT
            fr.date_forecast AS forecast_date,
            strftime(fr.date_forecast, '%A') AS day_name,
            COUNT(DISTINCT fr.store_no) AS store_count,
            COUNT(DISTINCT fr.item_no) AS item_count,
            COUNT(*) AS line_count,
            
            -- Forecast quantities
            SUM(COALESCE(fr.forecast_qty_pre_store_pass, fr.forecast_quantity)) AS total_forecast_pre_store_pass,
            SUM(COALESCE(fr.store_level_adjustment_qty, 0)) AS total_store_level_adj,
            SUM(COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity)) AS total_forecast_pre_weather,
            SUM(fr.forecast_quantity) AS total_forecast_qty,
            SUM(COALESCE(fr.weather_adjustment_qty, 0)) AS total_weather_adj,
            
            -- Forecast Average (Expected Sales)
            SUM(fr.forecast_average) AS total_forecast_average,
            
            -- Shipped Trend (W4 > W3 > W2 > W1)
            SUM(fr.w4_shipped) AS w4_shipped_total,
            SUM(fr.w3_shipped) AS w3_shipped_total,
            SUM(fr.w2_shipped) AS w2_shipped_total,
            SUM(fr.w1_shipped) AS w1_shipped_total,
            
            -- Sold Qty Trend (W4 > W3 > W2 > W1)
            SUM(fr.w4_sold) AS w4_sold_total,
            SUM(fr.w3_sold) AS w3_sold_total,
            SUM(fr.w2_sold) AS w2_sold_total,
            SUM(fr.w1_sold) AS w1_sold_total,
            
            -- Growth Metrics (Expected Sales vs History)
            CASE 
                WHEN SUM(fr.w1_sold) > 0 
                THEN ROUND((SUM(fr.forecast_average) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_sold), 4)
                ELSE 0 
            END AS growth_vs_w1_pct,
            
            CASE 
                WHEN SUM(fr.w2_sold) > 0 
                THEN ROUND((SUM(fr.forecast_average) - SUM(fr.w2_sold))::DOUBLE / SUM(fr.w2_sold), 4)
                ELSE 0 
            END AS growth_vs_w2_pct,

            -- Expected Shrink calculations
            -- 1. Based on forecast average (expected sales)
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.forecast_average))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_avg,
            
            -- 2. Based on last week sales
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_lw,
            
            -- 3. Based on two weeks prior sales
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w2_sold))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_2w,
            
            -- Last week shrink (actual)
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.w1_shipped) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS lw_shrink_pct,
            
            -- Weather severity by category (store counts)
            SUM(CASE WHEN fr.weather_severity_category = 'SEVERE' THEN 1 ELSE 0 END) AS severe_count,
            SUM(CASE WHEN fr.weather_severity_category = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
            SUM(CASE WHEN fr.weather_severity_category = 'MODERATE' THEN 1 ELSE 0 END) AS moderate_count,
            SUM(CASE WHEN fr.weather_severity_category = 'LOW' THEN 1 ELSE 0 END) AS low_count,
            SUM(CASE WHEN fr.weather_severity_category = 'MINIMAL' OR fr.weather_severity_category IS NULL THEN 1 ELSE 0 END) AS minimal_count,
            
            -- Weather metrics
            ROUND(AVG(COALESCE(fr.weather_severity_score, 0)), 2) AS avg_weather_severity,
            SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_weather_adjusted,
            
            -- Change from last week
            SUM(fr.forecast_quantity) - SUM(fr.w1_shipped) AS delta_from_lw,
            
            -- Delta % change from LW
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS delta_from_lw_pct
            
        FROM forecast_results fr
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        GROUP BY fr.date_forecast
        ORDER BY fr.date_forecast
    '''


def get_store_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for store-level summary BY DATE with weather indicators."""
    # Build inactive stores filter
    inactive_stores_filter = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_filter = f"AND fr.store_no NOT IN ({inactive_stores_str})"
    
    return f'''
        SELECT
            fr.date_forecast AS forecast_date,
            strftime(fr.date_forecast, '%A') AS day_name,
            fr.store_no,
            nm.store_name,
            COUNT(DISTINCT fr.item_no) AS item_count,
            COUNT(*) AS line_count,
            
            -- Forecast quantities
            SUM(COALESCE(fr.forecast_qty_pre_store_pass, fr.forecast_quantity)) AS total_forecast_pre_store_pass,
            SUM(COALESCE(fr.store_level_adjustment_qty, 0)) AS total_store_level_adj,
            SUM(COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity)) AS total_forecast_pre_weather,
            SUM(fr.forecast_quantity) AS total_forecast_qty,
            SUM(COALESCE(fr.weather_adjustment_qty, 0)) AS total_weather_adj,
            
            -- Forecast Average (Expected Sales)
            SUM(fr.forecast_average) AS total_forecast_average,
            
            -- Shipped Trend (W4 > W3 > W2 > W1)
            SUM(fr.w4_shipped) AS w4_shipped_total,
            SUM(fr.w3_shipped) AS w3_shipped_total,
            SUM(fr.w2_shipped) AS w2_shipped_total,
            SUM(fr.w1_shipped) AS w1_shipped_total,
            
            -- Sold Qty Trend (W4 > W3 > W2 > W1)
            SUM(fr.w4_sold) AS w4_sold_total,
            SUM(fr.w3_sold) AS w3_sold_total,
            SUM(fr.w2_sold) AS w2_sold_total,
            SUM(fr.w1_sold) AS w1_sold_total,
            
            -- Growth Metrics (Expected Sales vs History)
            CASE 
                WHEN SUM(fr.w1_sold) > 0 
                THEN ROUND((SUM(fr.forecast_average) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_sold), 4)
                ELSE 0 
            END AS growth_vs_w1_pct,
            
            CASE 
                WHEN SUM(fr.w2_sold) > 0 
                THEN ROUND((SUM(fr.forecast_average) - SUM(fr.w2_sold))::DOUBLE / SUM(fr.w2_sold), 4)
                ELSE 0 
            END AS growth_vs_w2_pct,

            -- Expected Shrink calculations
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.forecast_average))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_avg,
            
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_lw,
            
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w2_sold))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_2w,
            
            -- Last week shrink (actual)
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.w1_shipped) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS lw_shrink_pct,
            
            -- Weather metrics (for indicator)
            MAX(COALESCE(fr.weather_severity_score, 0)) AS max_weather_severity,
            MAX(fr.weather_severity_category) AS max_severity_category,
            MAX(fr.weather_day_condition) AS weather_condition,
            ROUND(AVG(COALESCE(fr.weather_severity_score, 0)), 2) AS avg_weather_severity,
            SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_weather_adjusted,
            
            -- Change from last week
            SUM(fr.forecast_quantity) - SUM(fr.w1_shipped) AS delta_from_lw,
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS delta_from_lw_pct
            
        FROM forecast_results fr
        LEFT JOIN (
            SELECT DISTINCT s.store_no, s.store_name
            FROM main.shrink s
            JOIN (
                SELECT store_no, MAX(date_posting) AS latest_date
                FROM main.shrink
                WHERE region_code = '{region}'
                GROUP BY store_no
            ) AS latest
            ON s.store_no = latest.store_no AND s.date_posting = latest.latest_date
            WHERE s.region_code = '{region}'
        ) nm ON fr.store_no = nm.store_no
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        GROUP BY fr.date_forecast, fr.store_no, nm.store_name
        ORDER BY fr.date_forecast, fr.store_no
    '''


def get_item_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for item-level summary BY DATE with same metrics as daily summary."""
    # Build inactive stores filter
    inactive_stores_filter = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_filter = f"AND fr.store_no NOT IN ({inactive_stores_str})"
    
    return f'''
        SELECT
            fr.date_forecast AS forecast_date,
            strftime(fr.date_forecast, '%A') AS day_name,
            fr.item_no,
            fr.item_desc,
            COUNT(DISTINCT fr.store_no) AS store_count,
            COUNT(*) AS line_count,
            
            -- Forecast quantities
            SUM(COALESCE(fr.forecast_qty_pre_store_pass, fr.forecast_quantity)) AS total_forecast_pre_store_pass,
            SUM(COALESCE(fr.store_level_adjustment_qty, 0)) AS total_store_level_adj,
            SUM(COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity)) AS total_forecast_pre_weather,
            SUM(fr.forecast_quantity) AS total_forecast_qty,
            SUM(COALESCE(fr.weather_adjustment_qty, 0)) AS total_weather_adj,
            
            -- Forecast Average (Expected Sales)
            SUM(fr.forecast_average) AS total_forecast_average,
            
            -- Shipped Trend (W4 > W3 > W2 > W1)
            SUM(fr.w4_shipped) AS w4_shipped_total,
            SUM(fr.w3_shipped) AS w3_shipped_total,
            SUM(fr.w2_shipped) AS w2_shipped_total,
            SUM(fr.w1_shipped) AS w1_shipped_total,
            
            -- Sold Qty Trend (W4 > W3 > W2 > W1)
            SUM(fr.w4_sold) AS w4_sold_total,
            SUM(fr.w3_sold) AS w3_sold_total,
            SUM(fr.w2_sold) AS w2_sold_total,
            SUM(fr.w1_sold) AS w1_sold_total,
            
            -- Growth Metrics (Expected Sales vs History)
            CASE 
                WHEN SUM(fr.w1_sold) > 0 
                THEN ROUND((SUM(fr.forecast_average) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_sold), 4)
                ELSE 0 
            END AS growth_vs_w1_pct,
            
            CASE 
                WHEN SUM(fr.w2_sold) > 0 
                THEN ROUND((SUM(fr.forecast_average) - SUM(fr.w2_sold))::DOUBLE / SUM(fr.w2_sold), 4)
                ELSE 0 
            END AS growth_vs_w2_pct,

            -- Expected Shrink calculations
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.forecast_average))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_avg,
            
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_lw,
            
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w2_sold))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_from_2w,
            
            -- Last week shrink (actual)
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.w1_shipped) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS lw_shrink_pct,
            
            -- Weather metrics
            SUM(CASE WHEN fr.weather_severity_category = 'SEVERE' THEN 1 ELSE 0 END) AS severe_count,
            SUM(CASE WHEN fr.weather_severity_category = 'HIGH' THEN 1 ELSE 0 END) AS high_count,
            SUM(CASE WHEN fr.weather_severity_category = 'MODERATE' THEN 1 ELSE 0 END) AS moderate_count,
            SUM(CASE WHEN fr.weather_severity_category = 'LOW' THEN 1 ELSE 0 END) AS low_count,
            SUM(CASE WHEN fr.weather_severity_category = 'MINIMAL' OR fr.weather_severity_category IS NULL THEN 1 ELSE 0 END) AS minimal_count,
            ROUND(AVG(COALESCE(fr.weather_severity_score, 0)), 2) AS avg_weather_severity,
            SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_weather_adjusted,
            
            -- Change from last week
            SUM(fr.forecast_quantity) - SUM(fr.w1_shipped) AS delta_from_lw,
            
            -- Delta % change from LW
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS delta_from_lw_pct
            
        FROM forecast_results fr
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        GROUP BY fr.date_forecast, fr.item_no, fr.item_desc
        ORDER BY fr.date_forecast, fr.item_no
    '''


def get_item_detail_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for full item/store level detail with trends and weather indicators."""
    # Build inactive stores filter
    inactive_stores_filter = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_filter = f"AND fr.store_no NOT IN ({inactive_stores_str})"
    
    return f'''
        SELECT
            fr.date_forecast AS "Forecast Date",
            strftime(fr.date_forecast, '%A') AS "Day",
            fr.store_no AS "Store #",
            nm.store_name AS "Store Name",
            fr.item_no AS "Item #",
            fr.item_desc AS "Item Description",
            
            -- Case pack
            fr.case_pack_size AS "Case Pack",
            
            -- Forecast quantities (with store-level pass tracking)
            COALESCE(fr.forecast_qty_pre_store_pass, fr.forecast_quantity) AS "Fcst Pre-Store Adj",
            COALESCE(fr.store_level_adjustment_qty, 0) AS "Store Adj Qty",
            COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity) AS "Fcst Pre-Weather",
            fr.forecast_quantity AS "Fcst Final",
            fr.forecast_quantity / NULLIF(fr.case_pack_size, 0) AS "Fcst Cases",
            COALESCE(fr.weather_adjustment_qty, 0) AS "Weather Adj",
            
            -- Forecast Average (Expected Sales)
            fr.forecast_average AS "Fcst Avg (Exp Sales)",
            
            -- Shipped Trend (W4 > W3 > W2 > W1 oldest to newest)
            fr.w4_shipped AS "W4 Ship",
            fr.w3_shipped AS "W3 Ship",
            fr.w2_shipped AS "W2 Ship",
            fr.w1_shipped AS "W1 Ship",
            
            -- Sold Qty Trend (W4 > W3 > W2 > W1 oldest to newest)
            fr.w4_sold AS "W4 Sold",
            fr.w3_sold AS "W3 Sold",
            fr.w2_sold AS "W2 Sold",
            fr.w1_sold AS "W1 Sold",
            
            -- Growth Metrics (Expected Sales vs History)
            ROUND(CASE WHEN fr.w1_sold > 0 
                THEN (fr.forecast_average - fr.w1_sold)::DOUBLE / fr.w1_sold 
                ELSE 0 END * 100, 1) AS "Growth vs W1 %",
            
            ROUND(CASE WHEN fr.w2_sold > 0 
                THEN (fr.forecast_average - fr.w2_sold)::DOUBLE / fr.w2_sold 
                ELSE 0 END * 100, 1) AS "Growth vs W2 %",

            -- Expected Shrink metrics (with conditional formatting)
            ROUND(CASE WHEN fr.forecast_quantity > 0 
                THEN (fr.forecast_quantity - fr.forecast_average)::DOUBLE / fr.forecast_quantity 
                ELSE 0 END * 100, 1) AS "Exp Shrink (Avg) %",
            ROUND(CASE WHEN fr.forecast_quantity > 0 
                THEN (fr.forecast_quantity - fr.w1_sold)::DOUBLE / fr.forecast_quantity 
                ELSE 0 END * 100, 1) AS "Exp Shrink (LW) %",
            ROUND(CASE WHEN fr.forecast_quantity > 0 
                THEN (fr.forecast_quantity - fr.w2_sold)::DOUBLE / fr.forecast_quantity 
                ELSE 0 END * 100, 1) AS "Exp Shrink (2W) %",
            
            -- Historical shrink percentages
            ROUND(fr.w1_shrink_p * 100, 1) AS "W1 Shrink %",
            
            -- Weather metrics (for indicator)
            ROUND(COALESCE(fr.weather_severity_score, 0), 1) AS "Weather Severity",
            COALESCE(fr.weather_severity_category, 'MINIMAL') AS "Severity Category",
            COALESCE(fr.weather_day_condition, '-') AS "Weather Condition",
            COALESCE(fr.weather_status_indicator, '-') AS "Weather Indicator",
            
            -- Delta from LW with % change
            fr.delta_from_last_week AS "Delta from LW",
            ROUND(CASE WHEN fr.w1_shipped > 0 
                THEN (fr.forecast_quantity - fr.w1_shipped)::DOUBLE / fr.w1_shipped 
                ELSE 0 END * 100, 1) AS "Delta LW %",
            
            -- Cover parameters
            fr.base_cover_applied AS "Cover Applied"
            
        FROM forecast_results fr
        LEFT JOIN (
            SELECT DISTINCT s.store_no, s.store_name
            FROM main.shrink s
            JOIN (
                SELECT store_no, MAX(date_posting) AS latest_date
                FROM main.shrink
                WHERE region_code = '{region}'
                GROUP BY store_no
            ) AS latest
            ON s.store_no = latest.store_no AND s.date_posting = latest.latest_date
            WHERE s.region_code = '{region}'
        ) nm ON fr.store_no = nm.store_no
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        ORDER BY fr.date_forecast, fr.store_no, fr.item_no
    '''


def get_weather_impact_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for weather impact summary by store and date."""
    return f'''
        SELECT
            w.date AS "Date",
            strftime(w.date, '%A') AS "Day",
            w.store_no AS "Store #",
            nm.store_name AS "Store Name",
            
            -- Weather conditions
            w.day_condition AS "Conditions",
            w.day_description AS "Description",
            
            -- Temperature
            ROUND(w.temp_min, 1) AS "Temp Min (F)",
            ROUND(w.temp_max, 1) AS "Temp Max (F)",
            ROUND(w.feels_like_min, 1) AS "Feels Like Min",
            ROUND(w.feels_like_max, 1) AS "Feels Like Max",
            
            -- Precipitation
            ROUND(w.total_rain_expected, 3) AS "Precip (in)",
            ROUND(w.precip_probability, 0) AS "Precip Prob %",
            ROUND(w.precip_cover, 0) AS "Precip Cover %",
            w.precip_type AS "Precip Type",
            
            -- Snow
            ROUND(w.snow_amount, 1) AS "Snow (in)",
            ROUND(w.snow_depth, 1) AS "Snow Depth (in)",
            
            -- Wind
            ROUND(w.wind_speed, 1) AS "Wind (mph)",
            ROUND(w.wind_gust, 1) AS "Wind Gust (mph)",
            
            -- Visibility and clouds
            ROUND(w.visibility, 1) AS "Visibility (mi)",
            ROUND(w.cloud_cover, 0) AS "Cloud Cover %",
            
            -- Severe weather
            ROUND(w.severe_risk, 0) AS "Severe Risk",
            
            -- Individual severity scores
            ROUND(w.rain_severity, 2) AS "Rain Sev",
            ROUND(w.snow_severity, 2) AS "Snow Sev",
            ROUND(w.wind_severity, 2) AS "Wind Sev",
            ROUND(w.temp_severity, 2) AS "Temp Sev",
            ROUND(w.visibility_severity, 2) AS "Vis Sev",
            ROUND(w.condition_severity, 2) AS "Cond Sev",
            
            -- Composite severity
            ROUND(w.severity_score, 2) AS "Severity Score",
            w.severity_category AS "Severity Category",
            ROUND(w.sales_impact_factor, 3) AS "Sales Impact Factor",
            
            -- Forecast impact
            COALESCE(fr.total_weather_adj, 0) AS "Total Weather Adj",
            COALESCE(fr.items_adjusted, 0) AS "Items Adjusted"
            
        FROM weather w
        LEFT JOIN (
            SELECT DISTINCT s.store_no, s.store_name
            FROM main.shrink s
            JOIN (
                SELECT store_no, MAX(date_posting) AS latest_date
                FROM main.shrink
                WHERE region_code = '{region}'
                GROUP BY store_no
            ) AS latest
            ON s.store_no = latest.store_no AND s.date_posting = latest.latest_date
            WHERE s.region_code = '{region}'
        ) nm ON w.store_no = nm.store_no
        LEFT JOIN (
            SELECT 
                store_no, 
                date_forecast,
                SUM(COALESCE(weather_adjustment_qty, 0)) AS total_weather_adj,
                SUM(CASE WHEN weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_adjusted
            FROM forecast_results
            WHERE region_code = '{region}'
            AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY store_no, date_forecast
        ) fr ON w.store_no = fr.store_no AND w.date = fr.date_forecast
        WHERE w.store_no IN (
            SELECT DISTINCT store_no FROM forecast_results 
            WHERE region_code = '{region}'
        )
        AND w.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY w.severity_score DESC, w.date, w.store_no
    '''


def get_weather_summary_by_date_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for weather summary aggregated by date."""
    return f'''
        SELECT
            w.date AS "Date",
            strftime(w.date, '%A') AS "Day",
            COUNT(*) AS "Store Count",
            
            -- Severity distribution
            SUM(CASE WHEN w.severity_category = 'SEVERE' THEN 1 ELSE 0 END) AS "Severe",
            SUM(CASE WHEN w.severity_category = 'HIGH' THEN 1 ELSE 0 END) AS "High",
            SUM(CASE WHEN w.severity_category = 'MODERATE' THEN 1 ELSE 0 END) AS "Moderate",
            SUM(CASE WHEN w.severity_category = 'LOW' THEN 1 ELSE 0 END) AS "Low",
            SUM(CASE WHEN w.severity_category = 'MINIMAL' THEN 1 ELSE 0 END) AS "Minimal",
            
            -- Average severity metrics
            ROUND(AVG(w.severity_score), 2) AS "Avg Severity",
            ROUND(MAX(w.severity_score), 2) AS "Max Severity",
            ROUND(AVG(w.sales_impact_factor), 3) AS "Avg Impact Factor",
            ROUND(MIN(w.sales_impact_factor), 3) AS "Min Impact Factor",
            
            -- Weather conditions
            ROUND(AVG(w.temp_min), 1) AS "Avg Temp Min",
            ROUND(AVG(w.temp_max), 1) AS "Avg Temp Max",
            ROUND(MIN(w.temp_min), 1) AS "Coldest Temp",
            ROUND(MAX(w.temp_max), 1) AS "Warmest Temp",
            
            -- Precipitation summary
            SUM(CASE WHEN w.total_rain_expected > 0.1 THEN 1 ELSE 0 END) AS "Stores w/ Rain",
            SUM(CASE WHEN w.snow_amount > 0 THEN 1 ELSE 0 END) AS "Stores w/ Snow",
            ROUND(AVG(CASE WHEN w.snow_depth > 0 THEN w.snow_depth END), 1) AS "Avg Snow Depth",
            
            -- Forecast impact
            COALESCE(SUM(fr.total_weather_adj), 0) AS "Total Qty Adj",
            COALESCE(SUM(fr.items_adjusted), 0) AS "Total Items Adj"
            
        FROM weather w
        LEFT JOIN (
            SELECT 
                store_no, 
                date_forecast,
                SUM(COALESCE(weather_adjustment_qty, 0)) AS total_weather_adj,
                SUM(CASE WHEN weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_adjusted
            FROM forecast_results
            WHERE region_code = '{region}'
            AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY store_no, date_forecast
        ) fr ON w.store_no = fr.store_no AND w.date = fr.date_forecast
        WHERE w.store_no IN (
            SELECT DISTINCT store_no FROM forecast_results 
            WHERE region_code = '{region}'
        )
        AND w.date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY w.date
        ORDER BY w.date
    '''


# =============================================================================
# FORMATTING HELPER FUNCTIONS
# =============================================================================

def create_summary_formats(wb):
    """
    Create all custom formats for the summary workbook.
    
    Args:
        wb: xlsxwriter Workbook object
        
    Returns:
        Dictionary of format objects
    """
    formats = {}
    
    # Title format
    formats['title'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 16,
        'bold': True,
        'font_color': COLORS['header_font'],
        'bg_color': COLORS['header_bg'],
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })
    
    # Subtitle format
    formats['subtitle'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 12,
        'bold': True,
        'font_color': COLORS['subheader_font'],
        'bg_color': COLORS['subheader_bg'],
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })
    
    # Section header
    formats['section'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 11,
        'bold': True,
        'bg_color': COLORS['section_bg'],
        'border': 1
    })
    
    # Column headers
    formats['col_header'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 10,
        'bold': True,
        'font_color': COLORS['header_font'],
        'bg_color': COLORS['header_bg'],
        'align': 'center',
        'valign': 'vcenter',
        'text_wrap': True,
        'border': 1
    })
    
    # Number formats
    formats['number'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0',
        'align': 'right',
        'border': 1
    })
    
    formats['number_bold'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0',
        'align': 'right',
        'bold': True,
        'bg_color': COLORS['total_bg'],
        'border': 1
    })
    
    formats['decimal'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0.0',
        'align': 'right',
        'border': 1
    })
    
    formats['decimal2'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0.00',
        'align': 'right',
        'border': 1
    })
    
    formats['decimal3'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0.000',
        'align': 'right',
        'border': 1
    })
    
    formats['currency'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '$#,##0.00',
        'align': 'right',
        'border': 1
    })
    
    # Percentage formats
    formats['pct'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'border': 1
    })
    
    formats['pct_bold'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bold': True,
        'bg_color': COLORS['total_bg'],
        'border': 1
    })
    
    # Conditional percentage formats
    formats['pct_good'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bg_color': COLORS['good_bg'],
        'font_color': COLORS['good_font'],
        'border': 1
    })
    
    formats['pct_warning'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bg_color': COLORS['warning_bg'],
        'font_color': COLORS['warning_font'],
        'border': 1
    })
    
    formats['pct_bad'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bg_color': COLORS['bad_bg'],
        'font_color': COLORS['bad_font'],
        'border': 1
    })
    
    # Text formats
    formats['text'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'left',
        'border': 1
    })
    
    formats['text_center'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'border': 1
    })
    
    formats['text_bold'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'bold': True,
        'align': 'left',
        'bg_color': COLORS['total_bg'],
        'border': 1
    })
    
    # Date format
    formats['date'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': 'yyyy-mm-dd',
        'align': 'center',
        'border': 1
    })
    
    # Weather severity formats
    formats['severity_severe'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['severe_bg'],
        'font_color': 'white',
        'bold': True,
        'border': 1
    })
    
    formats['severity_high'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['high_bg'],
        'font_color': 'white',
        'border': 1
    })
    
    formats['severity_moderate'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['moderate_bg'],
        'border': 1
    })
    
    formats['severity_low'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['low_bg'],
        'border': 1
    })
    
    formats['severity_minimal'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['minimal_bg'],
        'border': 1
    })
    
    # Trend string format
    formats['trend'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'font_size': 10,
        'border': 1
    })
    
    return formats


def get_severity_format(formats: dict, severity_score: float, category: str = None):
    """
    Get appropriate format based on weather severity.
    
    Args:
        formats: Dictionary of format objects
        severity_score: Weather severity score (0-10)
        category: Severity category string
        
    Returns:
        Format object
    """
    if category:
        category = category.upper()
        if category == 'SEVERE':
            return formats['severity_severe']
        elif category == 'HIGH':
            return formats['severity_high']
        elif category == 'MODERATE':
            return formats['severity_moderate']
        elif category == 'LOW':
            return formats['severity_low']
        else:
            return formats['severity_minimal']
    
    if severity_score >= 8:
        return formats['severity_severe']
    elif severity_score >= 6:
        return formats['severity_high']
    elif severity_score >= 4:
        return formats['severity_moderate']
    elif severity_score >= 2:
        return formats['severity_low']
    else:
        return formats['severity_minimal']


def get_shrink_pct_format(formats: dict, shrink_pct: float):
    """
    Get appropriate format based on shrink percentage.
    
    Args:
        formats: Dictionary of format objects
        shrink_pct: Shrink percentage (as decimal, e.g., 0.15 = 15%)
        
    Returns:
        Format object
    """
    if shrink_pct is None:
        return formats['pct']
    if shrink_pct <= 0.05:  # 5% or less = good
        return formats['pct_good']
    elif shrink_pct <= 0.15:  # 5-15% = warning
        return formats['pct_warning']
    else:  # Over 15% = bad
        return formats['pct_bad']


def build_sales_trend_string(w4: int, w3: int, w2: int, w1: int) -> str:
    """
    Build sales trend string: W4 > W3 > W2 > W1.
    
    Args:
        w4, w3, w2, w1: Weekly sales values
        
    Returns:
        Formatted trend string
    """
    def fmt(val):
        if val is None:
            return "-"
        return f"{int(val):,}"
    
    return f"{fmt(w4)} > {fmt(w3)} > {fmt(w2)} > {fmt(w1)}"


# =============================================================================
# WORKSHEET CREATION FUNCTIONS
# =============================================================================

def write_daily_summary_sheet(wb, conn, region: str, 
                               start_date: str, end_date: str,
                               formats: dict):
    """
    Create the Daily Summary worksheet with trends, expected shrink, and weather severity columns.
    
    Improvements:
    - Fcst (pre Store Adj), Store Adj Qty columns
    - Shipped Trend (W4 > W3 > W2 > W1)
    - Sold Qty Trend (W4 > W3 > W2 > W1)  
    - Forecast Avg (Expected Sales)
    - Expected Shrink (based on avg, LW sales, 2W sales) with conditional formatting
    - Weather severity columns by type (color coded) with store counts
    - Delta from LW with % change
    """
    ws = wb.add_worksheet('Daily Summary')
    
    # Set column widths for expanded layout
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:E', 8)    # Stores, Items, Lines
    ws.set_column('F:K', 12)   # Forecast quantities
    ws.set_column('L:L', 14)   # Fcst Avg
    ws.set_column('M:M', 26)   # Shipped Trend
    ws.set_column('N:N', 26)   # Sold Trend
    ws.set_column('O:R', 11)   # Expected Shrink columns
    ws.set_column('S:X', 8)    # Weather severity counts (SEVERE, HIGH, MODERATE, LOW, MINIMAL)
    ws.set_column('Y:Y', 11)   # Avg Weather
    ws.set_column('Z:AA', 12)  # Delta, Delta %
    
    # Title
    ws.merge_range('A1:AA1', f'Daily Forecast Summary - Region {region}', formats['title'])
    ws.merge_range('A2:AA2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Column headers - reorganized per improvements
    headers = [
        'Forecast Date', 'Day', 'Stores', 'Items', 'Lines',
        'Fcst (Pre Store Adj)', 'Store Adj Qty', 'Fcst (Pre Weather)', 
        'Weather Adj', 'Fcst Final',
        'Fcst Avg (Exp Sales)',
        'Shipped Trend (W4>W3>W2>W1)', 'Sold Trend (W4>W3>W2>W1)',
        'Exp Shrink (Avg) %', 'Exp Shrink (LW) %', 'Exp Shrink (2W) %', 'LW Shrink %',
        '🔴 Severe', '🟠 High', '🟡 Moderate', '🟢 Low', '✅ Minimal',
        'Avg Weather', 'Weather Adj Items',
        'Delta from LW', 'Delta LW %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Get data
    query = get_daily_summary_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting daily summary: {e}")
        data = []
    
    # Write data rows
    row = 4
    totals = {
        'stores': 0, 'items': 0, 'lines': 0,
        'pre_store_pass': 0, 'store_adj': 0, 'pre_weather': 0,
        'weather_adj': 0, 'forecast': 0, 'forecast_avg': 0,
        'w4_shipped': 0, 'w3_shipped': 0, 'w2_shipped': 0, 'w1_shipped': 0,
        'w4_sold': 0, 'w3_sold': 0, 'w2_sold': 0, 'w1_sold': 0,
        'severe': 0, 'high': 0, 'moderate': 0, 'low': 0, 'minimal': 0,
        'weather_adjusted': 0, 'delta': 0
    }
    
    for d in data:
        col = 0
        # Basic info
        ws.write(row, col, d.get('forecast_date'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        ws.write(row, col, d.get('store_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('item_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('line_count'), formats['number'])
        col += 1
        
        # Forecast quantities
        ws.write(row, col, d.get('total_forecast_pre_store_pass'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_store_level_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_pre_weather'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_weather_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_qty'), formats['number'])
        col += 1
        
        # Forecast Average
        ws.write(row, col, d.get('total_forecast_average'), formats['number'])
        col += 1
        
        # Shipped Trend
        shipped_trend = build_sales_trend_string(
            d.get('w4_shipped_total'), d.get('w3_shipped_total'),
            d.get('w2_shipped_total'), d.get('w1_shipped_total')
        )
        ws.write(row, col, shipped_trend, formats['trend'])
        col += 1
        
        # Sold Trend
        sold_trend = build_sales_trend_string(
            d.get('w4_sold_total'), d.get('w3_sold_total'),
            d.get('w2_sold_total'), d.get('w1_sold_total')
        )
        ws.write(row, col, sold_trend, formats['trend'])
        col += 1
        
        # Expected Shrink with conditional formatting
        exp_shrink_avg = d.get('expected_shrink_from_avg') or 0
        exp_shrink_lw = d.get('expected_shrink_from_lw') or 0
        exp_shrink_2w = d.get('expected_shrink_from_2w') or 0
        lw_shrink = d.get('lw_shrink_pct') or 0
        
        ws.write(row, col, exp_shrink_avg, get_shrink_pct_format(formats, exp_shrink_avg))
        col += 1
        ws.write(row, col, exp_shrink_lw, get_shrink_pct_format(formats, exp_shrink_lw))
        col += 1
        ws.write(row, col, exp_shrink_2w, get_shrink_pct_format(formats, exp_shrink_2w))
        col += 1
        ws.write(row, col, lw_shrink, get_shrink_pct_format(formats, lw_shrink))
        col += 1
        
        # Weather severity counts with color coding
        severe = d.get('severe_count', 0) or 0
        high = d.get('high_count', 0) or 0
        moderate = d.get('moderate_count', 0) or 0
        low = d.get('low_count', 0) or 0
        minimal = d.get('minimal_count', 0) or 0
        
        ws.write(row, col, severe, formats['severity_severe'] if severe > 0 else formats['number'])
        col += 1
        ws.write(row, col, high, formats['severity_high'] if high > 0 else formats['number'])
        col += 1
        ws.write(row, col, moderate, formats['severity_moderate'] if moderate > 0 else formats['number'])
        col += 1
        ws.write(row, col, low, formats['severity_low'] if low > 0 else formats['number'])
        col += 1
        ws.write(row, col, minimal, formats['severity_minimal'] if minimal > 0 else formats['number'])
        col += 1
        
        # Avg weather
        avg_sev = d.get('avg_weather_severity') or 0
        ws.write(row, col, avg_sev, get_severity_format(formats, avg_sev))
        col += 1
        ws.write(row, col, d.get('items_weather_adjusted'), formats['number'])
        col += 1
        
        # Delta from LW with % change
        ws.write(row, col, d.get('delta_from_lw'), formats['number'])
        col += 1
        delta_pct = d.get('delta_from_lw_pct') or 0
        ws.write(row, col, delta_pct, formats['pct'])
        col += 1
        
        # Accumulate totals
        totals['stores'] = max(totals['stores'], d.get('store_count') or 0)
        totals['items'] = max(totals['items'], d.get('item_count') or 0)
        totals['lines'] += d.get('line_count') or 0
        totals['pre_store_pass'] += d.get('total_forecast_pre_store_pass') or 0
        totals['store_adj'] += d.get('total_store_level_adj') or 0
        totals['pre_weather'] += d.get('total_forecast_pre_weather') or 0
        totals['weather_adj'] += d.get('total_weather_adj') or 0
        totals['forecast'] += d.get('total_forecast_qty') or 0
        totals['forecast_avg'] += d.get('total_forecast_average') or 0
        totals['w4_shipped'] += d.get('w4_shipped_total') or 0
        totals['w3_shipped'] += d.get('w3_shipped_total') or 0
        totals['w2_shipped'] += d.get('w2_shipped_total') or 0
        totals['w1_shipped'] += d.get('w1_shipped_total') or 0
        totals['w4_sold'] += d.get('w4_sold_total') or 0
        totals['w3_sold'] += d.get('w3_sold_total') or 0
        totals['w2_sold'] += d.get('w2_sold_total') or 0
        totals['w1_sold'] += d.get('w1_sold_total') or 0
        totals['severe'] += severe
        totals['high'] += high
        totals['moderate'] += moderate
        totals['low'] += low
        totals['minimal'] += minimal
        totals['weather_adjusted'] += d.get('items_weather_adjusted') or 0
        totals['delta'] += d.get('delta_from_lw') or 0
        
        row += 1
    
    # Write totals row
    col = 0
    ws.write(row, col, 'TOTAL', formats['text_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['stores'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['items'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['lines'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_store_pass'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['store_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_weather'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['weather_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast_avg'], formats['number_bold'])
    col += 1
    
    # Totals for trends
    shipped_total_trend = build_sales_trend_string(
        totals['w4_shipped'], totals['w3_shipped'],
        totals['w2_shipped'], totals['w1_shipped']
    )
    ws.write(row, col, shipped_total_trend, formats['trend'])
    col += 1
    
    sold_total_trend = build_sales_trend_string(
        totals['w4_sold'], totals['w3_sold'],
        totals['w2_sold'], totals['w1_sold']
    )
    ws.write(row, col, sold_total_trend, formats['trend'])
    col += 1
    
    # Total shrink percentages
    total_exp_shrink_avg = (totals['forecast'] - totals['forecast_avg']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_lw = (totals['forecast'] - totals['w1_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_2w = (totals['forecast'] - totals['w2_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_lw_shrink = (totals['w1_shipped'] - totals['w1_sold']) / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    
    ws.write(row, col, total_exp_shrink_avg, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_lw, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_2w, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_lw_shrink, formats['pct_bold'])
    col += 1
    
    # Weather totals
    ws.write(row, col, totals['severe'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['high'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['moderate'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['low'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['minimal'], formats['number_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['weather_adjusted'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['delta'], formats['number_bold'])
    col += 1
    total_delta_pct = totals['delta'] / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    ws.write(row, col, total_delta_pct, formats['pct_bold'])
    col += 1
    
    # Freeze panes
    ws.freeze_panes(4, 2)
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(headers) - 1)


def write_store_summary_sheet(wb, conn, region: str,
                               start_date: str, end_date: str,
                               formats: dict):
    """
    Create the Store Summary worksheet BY DATE (repeating stores for each day).
    
    Improvements:
    - By date format (repeating stores for each day)
    - Include metrics like daily summary
    - Weather indicator with symbol (emoji)
    - Shipped/Sold trends
    - Expected shrink columns
    - Delta from LW with % change
    """
    ws = wb.add_worksheet('Store Summary')
    
    # Set column widths for expanded daily layout
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:C', 10)   # Store #
    ws.set_column('D:D', 22)   # Store name
    ws.set_column('E:E', 8)    # Weather Icon
    ws.set_column('F:G', 8)    # Items, Lines
    ws.set_column('H:L', 12)   # Forecast quantities
    ws.set_column('M:M', 14)   # Fcst Avg
    ws.set_column('N:N', 26)   # Shipped Trend
    ws.set_column('O:O', 26)   # Sold Trend
    ws.set_column('P:S', 11)   # Expected Shrink columns
    ws.set_column('T:U', 11)   # Weather severity
    ws.set_column('V:W', 12)   # Delta columns
    
    # Title
    ws.merge_range('A1:W1', f'Store Daily Summary - Region {region}', formats['title'])
    ws.merge_range('A2:W2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Column headers
    headers = [
        'Date', 'Day', 'Store #', 'Store Name', 'Weather',
        'Items', 'Lines',
        'Fcst (Pre Store Adj)', 'Store Adj Qty', 'Fcst (Pre Weather)',
        'Weather Adj', 'Fcst Final',
        'Fcst Avg (Exp Sales)',
        'Shipped Trend (W4>W3>W2>W1)', 'Sold Trend (W4>W3>W2>W1)',
        'Exp Shrink (Avg) %', 'Exp Shrink (LW) %', 'Exp Shrink (2W) %', 'LW Shrink %',
        'Weather Severity', 'Severity Category',
        'Delta from LW', 'Delta LW %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Get data (now by date/store from updated query)
    query = get_store_summary_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting store summary: {e}")
        data = []
    
    # Write data rows
    row = 4
    for d in data:
        col = 0
        
        # Date and Day
        ws.write(row, col, d.get('forecast_date'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        
        # Store info
        ws.write(row, col, d.get('store_no'), formats['number'])
        col += 1
        ws.write(row, col, d.get('store_name') or f"Store {d.get('store_no')}", formats['text'])
        col += 1
        
        # Weather indicator icon
        weather_condition = d.get('weather_condition') or ''
        severity_cat = d.get('max_severity_category') or 'MINIMAL'
        severity_score = d.get('max_weather_severity') or 0
        weather_icon = get_weather_indicator_icon(
            condition=weather_condition,
            severity_category=severity_cat,
            severity_score=severity_score
        )
        ws.write(row, col, f"{weather_icon} {severity_cat}", get_severity_format(formats, severity_score, severity_cat))
        col += 1
        
        # Counts
        ws.write(row, col, d.get('item_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('line_count'), formats['number'])
        col += 1
        
        # Forecast quantities
        ws.write(row, col, d.get('total_forecast_pre_store_pass'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_store_level_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_pre_weather'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_weather_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_qty'), formats['number'])
        col += 1
        
        # Forecast Average
        ws.write(row, col, d.get('total_forecast_average'), formats['number'])
        col += 1
        
        # Shipped Trend
        shipped_trend = build_sales_trend_string(
            d.get('w4_shipped_total'), d.get('w3_shipped_total'),
            d.get('w2_shipped_total'), d.get('w1_shipped_total')
        )
        ws.write(row, col, shipped_trend, formats['trend'])
        col += 1
        
        # Sold Trend
        sold_trend = build_sales_trend_string(
            d.get('w4_sold_total'), d.get('w3_sold_total'),
            d.get('w2_sold_total'), d.get('w1_sold_total')
        )
        ws.write(row, col, sold_trend, formats['trend'])
        col += 1
        
        # Expected Shrink with conditional formatting
        exp_shrink_avg = d.get('expected_shrink_from_avg') or 0
        exp_shrink_lw = d.get('expected_shrink_from_lw') or 0
        exp_shrink_2w = d.get('expected_shrink_from_2w') or 0
        lw_shrink = d.get('lw_shrink_pct') or 0
        
        ws.write(row, col, exp_shrink_avg, get_shrink_pct_format(formats, exp_shrink_avg))
        col += 1
        ws.write(row, col, exp_shrink_lw, get_shrink_pct_format(formats, exp_shrink_lw))
        col += 1
        ws.write(row, col, exp_shrink_2w, get_shrink_pct_format(formats, exp_shrink_2w))
        col += 1
        ws.write(row, col, lw_shrink, get_shrink_pct_format(formats, lw_shrink))
        col += 1
        
        # Weather severity with formatting
        ws.write(row, col, severity_score, get_severity_format(formats, severity_score))
        col += 1
        ws.write(row, col, severity_cat, get_severity_format(formats, severity_score, severity_cat))
        col += 1
        
        # Delta from LW with % change
        ws.write(row, col, d.get('delta_from_lw'), formats['number'])
        col += 1
        delta_pct = d.get('delta_from_lw_pct') or 0
        ws.write(row, col, delta_pct, formats['pct'])
        col += 1
        
        row += 1
    
    # Freeze panes
    ws.freeze_panes(4, 4)  # Freeze date, day, store #, store name
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(headers) - 1)


def write_item_summary_sheet(wb, conn, region: str,
                              start_date: str, end_date: str,
                              formats: dict):
    """
    Create the Item Summary worksheet BY DATE with same metrics as daily summary.
    
    Format: Each day shows all items in separate rows, then moves to next day.
    Same metrics as Daily Summary but aggregated at the item level.
    
    Args:
        wb: Workbook object
        conn: DuckDB connection
        region: Region code
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    ws = wb.add_worksheet('Item Summary')
    
    # Set column widths for expanded layout
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:C', 12)   # Item #
    ws.set_column('D:D', 35)   # Item Description
    ws.set_column('E:F', 8)    # Stores, Lines
    ws.set_column('G:K', 12)   # Forecast quantities
    ws.set_column('L:L', 14)   # Fcst Avg
    ws.set_column('M:M', 26)   # Shipped Trend
    ws.set_column('N:N', 26)   # Sold Trend
    ws.set_column('O:R', 11)   # Expected Shrink columns
    ws.set_column('S:W', 8)    # Weather severity counts (SEVERE, HIGH, MODERATE, LOW, MINIMAL)
    ws.set_column('X:X', 11)   # Avg Weather
    ws.set_column('Y:Z', 12)   # Delta, Delta %
    
    # Title
    ws.merge_range('A1:Z1', f'Item Daily Summary - Region {region}', formats['title'])
    ws.merge_range('A2:Z2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Column headers - same metrics as Daily Summary but by item
    headers = [
        'Forecast Date', 'Day', 'Item #', 'Item Description',
        'Stores', 'Lines',
        'Fcst (Pre Store Adj)', 'Store Adj Qty', 'Fcst (Pre Weather)', 
        'Weather Adj', 'Fcst Final',
        'Fcst Avg (Exp Sales)',
        'Shipped Trend (W4>W3>W2>W1)', 'Sold Trend (W4>W3>W2>W1)',
        'Exp Shrink (Avg) %', 'Exp Shrink (LW) %', 'Exp Shrink (2W) %', 'LW Shrink %',
        '🔴 Severe', '🟠 High', '🟡 Moderate', '🟢 Low', '✅ Minimal',
        'Avg Weather',
        'Delta from LW', 'Delta LW %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Get data
    query = get_item_summary_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting item summary: {e}")
        data = []
    
    # Write data rows
    row = 4
    current_date = None
    
    for d in data:
        col = 0
        
        # Check if we need a date separator (visual grouping)
        forecast_date = d.get('forecast_date')
        if current_date is not None and forecast_date != current_date:
            # Add a subtle separator row between dates (empty row with light formatting)
            row += 1
        current_date = forecast_date
        
        # Basic info
        ws.write(row, col, d.get('forecast_date'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        ws.write(row, col, d.get('item_no'), formats['number'])
        col += 1
        ws.write(row, col, d.get('item_desc') or f"Item {d.get('item_no')}", formats['text'])
        col += 1
        ws.write(row, col, d.get('store_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('line_count'), formats['number'])
        col += 1
        
        # Forecast quantities
        ws.write(row, col, d.get('total_forecast_pre_store_pass'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_store_level_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_pre_weather'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_weather_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_qty'), formats['number'])
        col += 1
        
        # Forecast Average
        ws.write(row, col, d.get('total_forecast_average'), formats['number'])
        col += 1
        
        # Shipped Trend
        shipped_trend = build_sales_trend_string(
            d.get('w4_shipped_total'), d.get('w3_shipped_total'),
            d.get('w2_shipped_total'), d.get('w1_shipped_total')
        )
        ws.write(row, col, shipped_trend, formats['trend'])
        col += 1
        
        # Sold Trend
        sold_trend = build_sales_trend_string(
            d.get('w4_sold_total'), d.get('w3_sold_total'),
            d.get('w2_sold_total'), d.get('w1_sold_total')
        )
        ws.write(row, col, sold_trend, formats['trend'])
        col += 1
        
        # Expected Shrink with conditional formatting
        exp_shrink_avg = d.get('expected_shrink_from_avg') or 0
        exp_shrink_lw = d.get('expected_shrink_from_lw') or 0
        exp_shrink_2w = d.get('expected_shrink_from_2w') or 0
        lw_shrink = d.get('lw_shrink_pct') or 0
        
        ws.write(row, col, exp_shrink_avg, get_shrink_pct_format(formats, exp_shrink_avg))
        col += 1
        ws.write(row, col, exp_shrink_lw, get_shrink_pct_format(formats, exp_shrink_lw))
        col += 1
        ws.write(row, col, exp_shrink_2w, get_shrink_pct_format(formats, exp_shrink_2w))
        col += 1
        ws.write(row, col, lw_shrink, get_shrink_pct_format(formats, lw_shrink))
        col += 1
        
        # Weather severity counts with color coding
        severe = d.get('severe_count', 0) or 0
        high = d.get('high_count', 0) or 0
        moderate = d.get('moderate_count', 0) or 0
        low = d.get('low_count', 0) or 0
        minimal = d.get('minimal_count', 0) or 0
        
        ws.write(row, col, severe, formats['severity_severe'] if severe > 0 else formats['number'])
        col += 1
        ws.write(row, col, high, formats['severity_high'] if high > 0 else formats['number'])
        col += 1
        ws.write(row, col, moderate, formats['severity_moderate'] if moderate > 0 else formats['number'])
        col += 1
        ws.write(row, col, low, formats['severity_low'] if low > 0 else formats['number'])
        col += 1
        ws.write(row, col, minimal, formats['severity_minimal'] if minimal > 0 else formats['number'])
        col += 1
        
        # Avg weather
        avg_sev = d.get('avg_weather_severity') or 0
        ws.write(row, col, avg_sev, get_severity_format(formats, avg_sev))
        col += 1
        ws.write(row, col, d.get('items_weather_adjusted'), formats['number'])
        col += 1
        
        # Delta from LW with % change
        ws.write(row, col, d.get('delta_from_lw'), formats['number'])
        col += 1
        delta_pct = d.get('delta_from_lw_pct') or 0
        ws.write(row, col, delta_pct, formats['pct'])
        col += 1
        
        # Accumulate totals
        totals['stores'] = max(totals['stores'], d.get('store_count') or 0)
        totals['items'] = max(totals['items'], d.get('item_count') or 0)
        totals['lines'] += d.get('line_count') or 0
        totals['pre_store_pass'] += d.get('total_forecast_pre_store_pass') or 0
        totals['store_adj'] += d.get('total_store_level_adj') or 0
        totals['pre_weather'] += d.get('total_forecast_pre_weather') or 0
        totals['weather_adj'] += d.get('total_weather_adj') or 0
        totals['forecast'] += d.get('total_forecast_qty') or 0
        totals['forecast_avg'] += d.get('total_forecast_average') or 0
        totals['w4_shipped'] += d.get('w4_shipped_total') or 0
        totals['w3_shipped'] += d.get('w3_shipped_total') or 0
        totals['w2_shipped'] += d.get('w2_shipped_total') or 0
        totals['w1_shipped'] += d.get('w1_shipped_total') or 0
        totals['w4_sold'] += d.get('w4_sold_total') or 0
        totals['w3_sold'] += d.get('w3_sold_total') or 0
        totals['w2_sold'] += d.get('w2_sold_total') or 0
        totals['w1_sold'] += d.get('w1_sold_total') or 0
        totals['severe'] += severe
        totals['high'] += high
        totals['moderate'] += moderate
        totals['low'] += low
        totals['minimal'] += minimal
        totals['weather_adjusted'] += d.get('items_weather_adjusted') or 0
        totals['delta'] += d.get('delta_from_lw') or 0
        
        row += 1
    
    # Write totals row
    col = 0
    ws.write(row, col, 'TOTAL', formats['text_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['stores'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['items'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['lines'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_store_pass'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['store_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_weather'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['weather_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast_avg'], formats['number_bold'])
    col += 1
    
    # Totals for trends
    shipped_total_trend = build_sales_trend_string(
        totals['w4_shipped'], totals['w3_shipped'],
        totals['w2_shipped'], totals['w1_shipped']
    )
    ws.write(row, col, shipped_total_trend, formats['trend'])
    col += 1
    
    sold_total_trend = build_sales_trend_string(
        totals['w4_sold'], totals['w3_sold'],
        totals['w2_sold'], totals['w1_sold']
    )
    ws.write(row, col, sold_total_trend, formats['trend'])
    col += 1
    
    # Total shrink percentages
    total_exp_shrink_avg = (totals['forecast'] - totals['forecast_avg']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_lw = (totals['forecast'] - totals['w1_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_2w = (totals['forecast'] - totals['w2_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_lw_shrink = (totals['w1_shipped'] - totals['w1_sold']) / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    
    ws.write(row, col, total_exp_shrink_avg, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_lw, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_2w, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_lw_shrink, formats['pct_bold'])
    col += 1
    
    # Weather totals
    ws.write(row, col, totals['severe'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['high'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['moderate'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['low'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['minimal'], formats['number_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['weather_adjusted'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['delta'], formats['number_bold'])
    col += 1
    total_delta_pct = totals['delta'] / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    ws.write(row, col, total_delta_pct, formats['pct_bold'])
    col += 1
    
    # Freeze panes
    ws.freeze_panes(4, 2)
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(headers) - 1)


def write_item_summary_sheet(wb, conn, region: str,
                              start_date: str, end_date: str,
                              formats: dict):
    """
    Create the Item Summary worksheet BY DATE with same metrics as daily summary.
    
    Format: Each day shows all items in separate rows, then moves to next day.
    Same metrics as Daily Summary but aggregated at the item level.
    
    Args:
        wb: Workbook object
        conn: DuckDB connection
        region: Region code
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    ws = wb.add_worksheet('Item Summary')
    
    # Set column widths for expanded layout
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:C', 12)   # Item #
    ws.set_column('D:D', 35)   # Item Description
    ws.set_column('E:F', 8)    # Stores, Lines
    ws.set_column('G:K', 12)   # Forecast quantities
    ws.set_column('L:L', 14)   # Fcst Avg
    ws.set_column('M:M', 26)   # Shipped Trend
    ws.set_column('N:N', 26)   # Sold Trend
    ws.set_column('O:R', 11)   # Expected Shrink columns
    ws.set_column('S:W', 8)    # Weather severity counts (SEVERE, HIGH, MODERATE, LOW, MINIMAL)
    ws.set_column('X:X', 11)   # Avg Weather
    ws.set_column('Y:Z', 12)   # Delta, Delta %
    
    # Title
    ws.merge_range('A1:Z1', f'Item Daily Summary - Region {region}', formats['title'])
    ws.merge_range('A2:Z2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Column headers - same metrics as Daily Summary but by item
    headers = [
        'Forecast Date', 'Day', 'Item #', 'Item Description',
        'Stores', 'Lines',
        'Fcst (Pre Store Adj)', 'Store Adj Qty', 'Fcst (Pre Weather)', 
        'Weather Adj', 'Fcst Final',
        'Fcst Avg (Exp Sales)',
        'Shipped Trend (W4>W3>W2>W1)', 'Sold Trend (W4>W3>W2>W1)',
        'Exp Shrink (Avg) %', 'Exp Shrink (LW) %', 'Exp Shrink (2W) %', 'LW Shrink %',
        '🔴 Severe', '🟠 High', '🟡 Moderate', '🟢 Low', '✅ Minimal',
        'Avg Weather',
        'Delta from LW', 'Delta LW %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Get data
    query = get_item_summary_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting item summary: {e}")
        data = []
    
    # Write data rows
    row = 4
    current_date = None
    
    for d in data:
        col = 0
        
        # Check if we need a date separator (visual grouping)
        forecast_date = d.get('forecast_date')
        if current_date is not None and forecast_date != current_date:
            # Add a subtle separator row between dates (empty row with light formatting)
            row += 1
        current_date = forecast_date
        
        # Basic info
        ws.write(row, col, d.get('forecast_date'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        ws.write(row, col, d.get('item_no'), formats['number'])
        col += 1
        ws.write(row, col, d.get('item_desc') or f"Item {d.get('item_no')}", formats['text'])
        col += 1
        ws.write(row, col, d.get('store_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('line_count'), formats['number'])
        col += 1
        
        # Forecast quantities
        ws.write(row, col, d.get('total_forecast_pre_store_pass'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_store_level_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_pre_weather'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_weather_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_qty'), formats['number'])
        col += 1
        
        # Forecast Average
        ws.write(row, col, d.get('total_forecast_average'), formats['number'])
        col += 1
        
        # Shipped Trend
        shipped_trend = build_sales_trend_string(
            d.get('w4_shipped_total'), d.get('w3_shipped_total'),
            d.get('w2_shipped_total'), d.get('w1_shipped_total')
        )
        ws.write(row, col, shipped_trend, formats['trend'])
        col += 1
        
        # Sold Trend
        sold_trend = build_sales_trend_string(
            d.get('w4_sold_total'), d.get('w3_sold_total'),
            d.get('w2_sold_total'), d.get('w1_sold_total')
        )
        ws.write(row, col, sold_trend, formats['trend'])
        col += 1
        
        # Expected Shrink with conditional formatting
        exp_shrink_avg = d.get('expected_shrink_from_avg') or 0
        exp_shrink_lw = d.get('expected_shrink_from_lw') or 0
        exp_shrink_2w = d.get('expected_shrink_from_2w') or 0
        lw_shrink = d.get('lw_shrink_pct') or 0
        
        ws.write(row, col, exp_shrink_avg, get_shrink_pct_format(formats, exp_shrink_avg))
        col += 1
        ws.write(row, col, exp_shrink_lw, get_shrink_pct_format(formats, exp_shrink_lw))
        col += 1
        ws.write(row, col, exp_shrink_2w, get_shrink_pct_format(formats, exp_shrink_2w))
        col += 1
        ws.write(row, col, lw_shrink, get_shrink_pct_format(formats, lw_shrink))
        col += 1
        
        # Weather severity counts with color coding
        severe = d.get('severe_count', 0) or 0
        high = d.get('high_count', 0) or 0
        moderate = d.get('moderate_count', 0) or 0
        low = d.get('low_count', 0) or 0
        minimal = d.get('minimal_count', 0) or 0
        
        ws.write(row, col, severe, formats['severity_severe'] if severe > 0 else formats['number'])
        col += 1
        ws.write(row, col, high, formats['severity_high'] if high > 0 else formats['number'])
        col += 1
        ws.write(row, col, moderate, formats['severity_moderate'] if moderate > 0 else formats['number'])
        col += 1
        ws.write(row, col, low, formats['severity_low'] if low > 0 else formats['number'])
        col += 1
        ws.write(row, col, minimal, formats['severity_minimal'] if minimal > 0 else formats['number'])
        col += 1
        
        # Avg weather
        avg_sev = d.get('avg_weather_severity') or 0
        ws.write(row, col, avg_sev, get_severity_format(formats, avg_sev))
        col += 1
        ws.write(row, col, d.get('items_weather_adjusted'), formats['number'])
        col += 1
        
        # Delta from LW with % change
        ws.write(row, col, d.get('delta_from_lw'), formats['number'])
        col += 1
        delta_pct = d.get('delta_from_lw_pct') or 0
        ws.write(row, col, delta_pct, formats['pct'])
        col += 1
        
        # Accumulate totals
        totals['stores'] = max(totals['stores'], d.get('store_count') or 0)
        totals['items'] = max(totals['items'], d.get('item_count') or 0)
        totals['lines'] += d.get('line_count') or 0
        totals['pre_store_pass'] += d.get('total_forecast_pre_store_pass') or 0
        totals['store_adj'] += d.get('total_store_level_adj') or 0
        totals['pre_weather'] += d.get('total_forecast_pre_weather') or 0
        totals['weather_adj'] += d.get('total_weather_adj') or 0
        totals['forecast'] += d.get('total_forecast_qty') or 0
        totals['forecast_avg'] += d.get('total_forecast_average') or 0
        totals['w4_shipped'] += d.get('w4_shipped_total') or 0
        totals['w3_shipped'] += d.get('w3_shipped_total') or 0
        totals['w2_shipped'] += d.get('w2_shipped_total') or 0
        totals['w1_shipped'] += d.get('w1_shipped_total') or 0
        totals['w4_sold'] += d.get('w4_sold_total') or 0
        totals['w3_sold'] += d.get('w3_sold_total') or 0
        totals['w2_sold'] += d.get('w2_sold_total') or 0
        totals['w1_sold'] += d.get('w1_sold_total') or 0
        totals['severe'] += severe
        totals['high'] += high
        totals['moderate'] += moderate
        totals['low'] += low
        totals['minimal'] += minimal
        totals['weather_adjusted'] += d.get('items_weather_adjusted') or 0
        totals['delta'] += d.get('delta_from_lw') or 0
        
        row += 1
    
    # Write totals row
    col = 0
    ws.write(row, col, 'TOTAL', formats['text_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['stores'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['items'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['lines'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_store_pass'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['store_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_weather'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['weather_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast_avg'], formats['number_bold'])
    col += 1
    
    # Totals for trends
    shipped_total_trend = build_sales_trend_string(
        totals['w4_shipped'], totals['w3_shipped'],
        totals['w2_shipped'], totals['w1_shipped']
    )
    ws.write(row, col, shipped_total_trend, formats['trend'])
    col += 1
    
    sold_total_trend = build_sales_trend_string(
        totals['w4_sold'], totals['w3_sold'],
        totals['w2_sold'], totals['w1_sold']
    )
    ws.write(row, col, sold_total_trend, formats['trend'])
    col += 1
    
    # Total shrink percentages
    total_exp_shrink_avg = (totals['forecast'] - totals['forecast_avg']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_lw = (totals['forecast'] - totals['w1_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_2w = (totals['forecast'] - totals['w2_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_lw_shrink = (totals['w1_shipped'] - totals['w1_sold']) / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    
    ws.write(row, col, total_exp_shrink_avg, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_lw, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_2w, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_lw_shrink, formats['pct_bold'])
    col += 1
    
    # Weather totals
    ws.write(row, col, totals['severe'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['high'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['moderate'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['low'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['minimal'], formats['number_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['weather_adjusted'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['delta'], formats['number_bold'])
    col += 1
    total_delta_pct = totals['delta'] / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    ws.write(row, col, total_delta_pct, formats['pct_bold'])
    col += 1
    
    # Freeze panes
    ws.freeze_panes(4, 2)
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(headers) - 1)


def write_item_detail_sheet(wb, conn, region: str,
                            start_date: str, end_date: str,
                            formats: dict):
    """
    Create the Item Details worksheet with weather indicator logos.
    
    Improvements:
    - Weather indicator logos column
    - Shipped/Sold trends
    - Expected shrink columns
    - Delta with % change
    """
    ws = wb.add_worksheet('Item Details')
    
    # Title
    ws.merge_range('A1:AB1', f'Item/Store Details - Region {region}', formats['title'])
    ws.merge_range('A2:AB2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Get data
    query = get_item_detail_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
    except Exception as e:
        print(f"Error getting item details: {e}")
        return
    
    if len(df) == 0:
        ws.write(4, 0, "No data available", formats['text'])
        return
    
    # Get column names
    columns = df.columns
    
    # Write headers
    for col, header in enumerate(columns):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Set column widths based on content type
    col_widths = {
        'Forecast Date': 12, 'Day': 10, 'Store #': 8, 'Store Name': 20,
        'Item #': 10, 'Item Description': 30, 'Case Pack': 8,
        'Fcst Pre-Store Adj': 14, 'Store Adj Qty': 12, 'Fcst Pre-Weather': 14,
        'Fcst Final': 12, 'Fcst Cases': 10, 'Weather Adj': 10,
        'Fcst Avg (Exp Sales)': 14,
        'W4 Ship': 10, 'W3 Ship': 10, 'W2 Ship': 10, 'W1 Ship': 10,
        'W4 Sold': 10, 'W3 Sold': 10, 'W2 Sold': 10, 'W1 Sold': 10,
        'Exp Shrink (Avg) %': 14, 'Exp Shrink (LW) %': 14, 'Exp Shrink (2W) %': 14,
        'W1 Shrink %': 12,
        'Weather Severity': 12, 'Severity Category': 14, 'Weather Condition': 18,
        'Weather Indicator': 30,
        'Delta from LW': 12, 'Delta LW %': 11, 'Cover Applied': 10
    }
    
    for col, header in enumerate(columns):
        width = col_widths.get(header, 12)
        ws.set_column(col, col, width)
    
    # Write data
    row = 4
    data = df.to_dicts()
    
    for d in data:
        for col, header in enumerate(columns):
            value = d.get(header)
            
            # Apply appropriate format based on column type
            if 'Date' in header:
                ws.write(row, col, value, formats['date'])
            elif 'Shrink' in header and '%' in header:
                # These are already in percentage form (e.g., 15.5 for 15.5%)
                pct_val = (value or 0) / 100
                ws.write(row, col, pct_val, get_shrink_pct_format(formats, pct_val))
            elif header == 'Severity Category':
                severity_score = d.get('Weather Severity') or 0
                # Add weather icon to severity category
                weather_condition = d.get('Weather Condition') or ''
                severity_cat = value or 'MINIMAL'
                icon = get_weather_indicator_icon(
                    condition=weather_condition,
                    severity_category=severity_cat,
                    severity_score=severity_score
                )
                ws.write(row, col, f"{icon} {severity_cat}", get_severity_format(formats, severity_score, value))
            elif header == 'Weather Indicator':
                # Weather indicator with full icon
                severity_score = d.get('Weather Severity') or 0
                severity_cat = d.get('Severity Category') or 'MINIMAL'
                weather_condition = d.get('Weather Condition') or ''
                icon = get_weather_indicator_icon(
                    condition=weather_condition,
                    severity_category=severity_cat,
                    severity_score=severity_score
                )
                ws.write(row, col, f"{icon} {weather_condition}", get_severity_format(formats, severity_score, severity_cat))
            elif header == 'Weather Severity':
                ws.write(row, col, value, get_severity_format(formats, value or 0))
            elif 'Cost' in header or 'Price' in header:
                ws.write(row, col, value, formats['currency'])
            elif header in ('Day', 'Store Name', 'Item Description', 'Weather Condition'):
                ws.write(row, col, value, formats['text'])
            elif 'Delta LW %' in header:
                pct_val = (value or 0) / 100
                ws.write(row, col, pct_val, formats['pct'])
            elif 'Velocity' in header or 'Volatility' in header or 'Cover' in header:
                ws.write(row, col, value, formats['decimal2'])
            elif 'Avg' in header or 'EMA' in header:
                ws.write(row, col, value, formats['decimal'])
            else:
                ws.write(row, col, value, formats['number'])
        
        row += 1
    
    # Freeze panes
    ws.freeze_panes(4, 6)  # Freeze up to Item #
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(columns) - 1)


def write_weather_impact_sheet(wb, conn, region: str,
                               start_date: str, end_date: str,
                               formats: dict):
    """
    Create the Weather Impact Summary worksheet.
    
    This sheet provides comprehensive weather impact analysis including:
    - Daily weather summary with severity distribution
    - Store-level weather details sorted by severity
    - Individual severity factor breakdown
    
    Note: This function connects to the weather.db separately since 
    weather data is stored in a different database.
    
    Args:
        wb: Workbook object
        conn: DuckDB connection (for forecast_results)
        region: Region code
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    import duckdb
    
    ws = wb.add_worksheet('Weather Impact')
    
    # Title
    ws.merge_range('A1:S1', f'Weather Impact Summary - Region {region}', formats['title'])
    ws.merge_range('A2:S2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Get weather database path
    weather_db_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'data_store', 'weather.db'
    )
    
    # Get store names from forecast database
    store_names_query = f'''
        SELECT DISTINCT s.store_no, s.store_name
        FROM main.shrink s
        JOIN (
            SELECT store_no, MAX(date_posting) AS latest_date
            FROM main.shrink
            WHERE region_code = '{region}'
            GROUP BY store_no
        ) AS latest
        ON s.store_no = latest.store_no AND s.date_posting = latest.latest_date
        WHERE s.region_code = '{region}'
    '''
    
    # Get forecast adjustments
    forecast_adj_query = f'''
        SELECT 
            store_no, 
            date_forecast,
            SUM(COALESCE(weather_adjustment_qty, 0)) AS total_weather_adj,
            SUM(CASE WHEN weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_adjusted
        FROM forecast_results
        WHERE region_code = '{region}'
        AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY store_no, date_forecast
    '''
    
    try:
        store_names_df = conn.sql(store_names_query).to_df()
        store_names_dict = dict(zip(
            store_names_df['store_no'].astype(str),
            store_names_df['store_name']
        ))
        
        forecast_adj_df = conn.sql(forecast_adj_query).to_df()
    except Exception as e:
        print(f"Error getting forecast data: {e}")
        store_names_dict = {}
        forecast_adj_df = None
    
    # Get store list for this region
    region_stores_query = f'''
        SELECT DISTINCT store_no FROM forecast_results WHERE region_code = '{region}'
    '''
    try:
        region_stores = [str(s[0]) for s in conn.sql(region_stores_query).fetchall()]
    except:
        region_stores = []
    
    # =========================================================================
    # SECTION 1: Daily Weather Summary
    # =========================================================================
    current_row = 3
    ws.merge_range(current_row, 0, current_row, 18, 
                   'Daily Weather Summary', formats['section'])
    current_row += 1
    
    # Get daily summary data from weather.db
    daily_df = None
    if os.path.exists(weather_db_path) and region_stores:
        try:
            weather_conn = duckdb.connect(weather_db_path, read_only=True)
            
            # Build store list for IN clause
            store_list = ','.join(f"'{s}'" for s in region_stores)
            
            daily_query = f'''
                SELECT
                    date AS "Date",
                    strftime(date, '%A') AS "Day",
                    COUNT(*) AS "Store Count",
                    
                    -- Severity distribution
                    SUM(CASE WHEN severity_category = 'SEVERE' THEN 1 ELSE 0 END) AS "Severe",
                    SUM(CASE WHEN severity_category = 'HIGH' THEN 1 ELSE 0 END) AS "High",
                    SUM(CASE WHEN severity_category = 'MODERATE' THEN 1 ELSE 0 END) AS "Moderate",
                    SUM(CASE WHEN severity_category = 'LOW' THEN 1 ELSE 0 END) AS "Low",
                    SUM(CASE WHEN severity_category = 'MINIMAL' THEN 1 ELSE 0 END) AS "Minimal",
                    
                    -- Average severity metrics
                    ROUND(AVG(severity_score), 2) AS "Avg Severity",
                    ROUND(MAX(severity_score), 2) AS "Max Severity",
                    ROUND(AVG(sales_impact_factor), 3) AS "Avg Impact Factor",
                    ROUND(MIN(sales_impact_factor), 3) AS "Min Impact Factor",
                    
                    -- Weather conditions
                    ROUND(AVG(temp_min), 1) AS "Avg Temp Min",
                    ROUND(AVG(temp_max), 1) AS "Avg Temp Max",
                    ROUND(MIN(temp_min), 1) AS "Coldest Temp",
                    ROUND(MAX(temp_max), 1) AS "Warmest Temp",
                    
                    -- Precipitation summary
                    SUM(CASE WHEN total_rain_expected > 0.01 THEN 1 ELSE 0 END) AS "Stores w/ Rain",
                    SUM(CASE WHEN snow_amount > 0 THEN 1 ELSE 0 END) AS "Stores w/ Snow",
                    ROUND(AVG(CASE WHEN snow_depth > 0 THEN snow_depth ELSE NULL END), 1) AS "Avg Snow Depth"
                    
                FROM weather
                WHERE store_no IN ({store_list})
                AND date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY date
                ORDER BY date
            '''
            daily_df = pl.from_pandas(weather_conn.sql(daily_query).to_df())
            weather_conn.close()
        except Exception as e:
            print(f"Error getting weather daily summary: {e}")
            daily_df = None
    
    if daily_df is not None and len(daily_df) > 0:
        # Headers
        daily_headers = [
            'Date', 'Day', 'Stores', 'Severe', 'High', 'Moderate', 'Low', 'Minimal',
            'Avg Severity', 'Max Severity', 'Avg Impact', 'Min Impact',
            'Avg Low °F', 'Avg High °F', 'Coldest', 'Warmest',
            'Rain Stores', 'Snow Stores', 'Avg Snow Depth'
        ]
        
        for col, header in enumerate(daily_headers):
            ws.write(current_row, col, header, formats['col_header'])
        ws.set_row(current_row, 30)
        current_row += 1
        
        # Set column widths for daily section
        ws.set_column('A:A', 12)  # Date
        ws.set_column('B:B', 10)  # Day
        ws.set_column('C:H', 8)   # Store counts and severity counts
        ws.set_column('I:L', 10)  # Severity and impact metrics
        ws.set_column('M:P', 10)  # Temperature
        ws.set_column('Q:S', 10)  # Precipitation
        
        # Write daily data
        for d in daily_df.to_dicts():
            col = 0
            # Date
            ws.write(current_row, col, d.get('Date'), formats['date']); col += 1
            # Day
            ws.write(current_row, col, d.get('Day'), formats['text']); col += 1
            # Store Count
            ws.write(current_row, col, d.get('Store Count'), formats['number']); col += 1
            
            # Severity counts with color coding
            severe_count = d.get('Severe', 0) or 0
            high_count = d.get('High', 0) or 0
            moderate_count = d.get('Moderate', 0) or 0
            low_count = d.get('Low', 0) or 0
            minimal_count = d.get('Minimal', 0) or 0
            
            ws.write(current_row, col, severe_count, 
                    formats['severity_severe'] if severe_count > 0 else formats['number']); col += 1
            ws.write(current_row, col, high_count,
                    formats['severity_high'] if high_count > 0 else formats['number']); col += 1
            ws.write(current_row, col, moderate_count,
                    formats['severity_moderate'] if moderate_count > 0 else formats['number']); col += 1
            ws.write(current_row, col, low_count,
                    formats['severity_low'] if low_count > 0 else formats['number']); col += 1
            ws.write(current_row, col, minimal_count,
                    formats['severity_minimal'] if minimal_count > 0 else formats['number']); col += 1
            
            # Severity metrics
            avg_sev = d.get('Avg Severity', 0) or 0
            ws.write(current_row, col, avg_sev, get_severity_format(formats, avg_sev)); col += 1
            max_sev = d.get('Max Severity', 0) or 0
            ws.write(current_row, col, max_sev, get_severity_format(formats, max_sev)); col += 1
            
            # Impact factors
            ws.write(current_row, col, d.get('Avg Impact Factor'), formats['decimal3']); col += 1
            ws.write(current_row, col, d.get('Min Impact Factor'), formats['decimal3']); col += 1
            
            # Temperature
            ws.write(current_row, col, d.get('Avg Temp Min'), formats['decimal']); col += 1
            ws.write(current_row, col, d.get('Avg Temp Max'), formats['decimal']); col += 1
            ws.write(current_row, col, d.get('Coldest Temp'), formats['decimal']); col += 1
            ws.write(current_row, col, d.get('Warmest Temp'), formats['decimal']); col += 1
            
            # Precipitation
            ws.write(current_row, col, d.get('Stores w/ Rain'), formats['number']); col += 1
            ws.write(current_row, col, d.get('Stores w/ Snow'), formats['number']); col += 1
            ws.write(current_row, col, d.get('Avg Snow Depth'), formats['decimal']); col += 1
            
            current_row += 1
    else:
        ws.write(current_row, 0, "No daily weather summary available", formats['text'])
        current_row += 1
    
    # Add spacing
    current_row += 2
    
    # =========================================================================
    # SECTION 2: Store Weather Details (sorted by severity)
    # =========================================================================
    ws.merge_range(current_row, 0, current_row, 18,
                   'Store Weather Details (Sorted by Severity)', formats['section'])
    current_row += 1
    
    # Get store-level weather data from weather.db
    detail_df = None
    if os.path.exists(weather_db_path) and region_stores:
        try:
            weather_conn = duckdb.connect(weather_db_path, read_only=True)
            
            # Build store list for IN clause
            store_list = ','.join(f"'{s}'" for s in region_stores)
            
            detail_query = f'''
                SELECT
                    date AS "Date",
                    strftime(date, '%A') AS "Day",
                    store_no AS "Store #",
                    
                    -- Weather conditions
                    day_condition AS "Conditions",
                    
                    -- Temperature
                    ROUND(temp_min, 1) AS "Temp Min (F)",
                    ROUND(temp_max, 1) AS "Temp Max (F)",
                    
                    -- Precipitation
                    ROUND(total_rain_expected, 3) AS "Precip (in)",
                    ROUND(precip_probability, 0) AS "Precip Prob %",
                    
                    -- Snow
                    ROUND(snow_amount, 1) AS "Snow (in)",
                    ROUND(snow_depth, 1) AS "Snow Depth (in)",
                    
                    -- Wind
                    ROUND(wind_speed, 1) AS "Wind (mph)",
                    
                    -- Visibility
                    ROUND(visibility, 1) AS "Visibility (mi)",
                    
                    -- Severe weather
                    ROUND(severe_risk, 0) AS "Severe Risk",
                    
                    -- Composite severity
                    ROUND(severity_score, 2) AS "Severity Score",
                    severity_category AS "Severity Category",
                    ROUND(sales_impact_factor, 3) AS "Sales Impact Factor"
                    
                FROM weather
                WHERE store_no IN ({store_list})
                AND date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY severity_score DESC, date, store_no
            '''
            
            detail_df = pl.from_pandas(weather_conn.sql(detail_query).to_df())
            weather_conn.close()
            
            # Add store names and forecast adjustment data
            if detail_df is not None and len(detail_df) > 0:
                detail_dicts = detail_df.to_dicts()
                for row_dict in detail_dicts:
                    store_no = str(row_dict.get('Store #', ''))
                    row_dict['Store Name'] = store_names_dict.get(store_no, '')
                    
                    # Get forecast adjustment for this store/date
                    if forecast_adj_df is not None:
                        date_val = row_dict.get('Date')
                        adj_row = forecast_adj_df[
                            (forecast_adj_df['store_no'].astype(str) == store_no) &
                            (forecast_adj_df['date_forecast'].astype(str) == str(date_val))
                        ]
                        if len(adj_row) > 0:
                            row_dict['Total Weather Adj'] = adj_row.iloc[0]['total_weather_adj']
                            row_dict['Items Adjusted'] = adj_row.iloc[0]['items_adjusted']
                        else:
                            row_dict['Total Weather Adj'] = 0
                            row_dict['Items Adjusted'] = 0
                    else:
                        row_dict['Total Weather Adj'] = 0
                        row_dict['Items Adjusted'] = 0
                
                detail_df = pl.from_dicts(detail_dicts)
                
        except Exception as e:
            print(f"Error getting weather details: {e}")
            detail_df = None
    
    if detail_df is not None and len(detail_df) > 0:
        # Filter to show only stores with meaningful weather impact
        # Show all stores but sort by severity
        
        # Headers for detail section - added Weather Icon column
        detail_headers = [
            'Weather', 'Date', 'Day', 'Store #', 'Store Name', 'Conditions',
            'Temp Min', 'Temp Max', 'Precip (in)', 'Precip %', 
            'Snow (in)', 'Snow Depth', 'Wind (mph)', 'Visibility',
            'Severe Risk', 'Severity Score', 'Category', 'Impact Factor',
            'Qty Adjusted', 'Items Adj'
        ]
        
        for col, header in enumerate(detail_headers):
            ws.write(current_row, col, header, formats['col_header'])
        ws.set_row(current_row, 30)
        current_row += 1
        
        # Map from query columns to display - updated with Weather Icon first
        col_mapping = [
            ('_weather_icon', 'weather_icon'),  # Special handling for icon
            ('Date', 'date'),
            ('Day', 'text'),
            ('Store #', 'number'),
            ('Store Name', 'text'),
            ('Conditions', 'text'),
            ('Temp Min (F)', 'decimal'),
            ('Temp Max (F)', 'decimal'),
            ('Precip (in)', 'decimal3'),
            ('Precip Prob %', 'number'),
            ('Snow (in)', 'decimal'),
            ('Snow Depth (in)', 'decimal'),
            ('Wind (mph)', 'decimal'),
            ('Visibility (mi)', 'decimal'),
            ('Severe Risk', 'number'),
            ('Severity Score', 'severity'),
            ('Severity Category', 'severity_cat'),
            ('Sales Impact Factor', 'decimal3'),
            ('Total Weather Adj', 'number'),
            ('Items Adjusted', 'number'),
        ]
        
        # Limit to top 200 rows for performance (already sorted by severity DESC)
        data_rows = detail_df.to_dicts()[:200]
        
        for d in data_rows:
            for col, (key, fmt_type) in enumerate(col_mapping):
                if fmt_type == 'weather_icon':
                    # Generate weather icon from conditions
                    condition = d.get('Conditions') or ''
                    severity_cat = d.get('Severity Category') or 'MINIMAL'
                    severity_score = d.get('Severity Score') or 0
                    snow_amount = d.get('Snow (in)') or 0
                    rain_amount = d.get('Precip (in)') or 0
                    temp_min = d.get('Temp Min (F)')
                    temp_max = d.get('Temp Max (F)')
                    wind_speed = d.get('Wind (mph)') or 0
                    
                    icon = get_weather_indicator_icon(
                        condition=condition,
                        severity_category=severity_cat,
                        snow_amount=snow_amount,
                        rain_amount=rain_amount,
                        temp_min=temp_min,
                        temp_max=temp_max,
                        wind_speed=wind_speed,
                        severity_score=severity_score
                    )
                    ws.write(current_row, col, icon, get_severity_format(formats, severity_score, severity_cat))
                else:
                    value = d.get(key)
                    
                    if fmt_type == 'date':
                        ws.write(current_row, col, value, formats['date'])
                    elif fmt_type == 'text':
                        ws.write(current_row, col, value or '', formats['text'])
                    elif fmt_type == 'decimal':
                        ws.write(current_row, col, value, formats['decimal'])
                    elif fmt_type == 'decimal3':
                        ws.write(current_row, col, value, formats['decimal3'])
                    elif fmt_type == 'severity':
                        ws.write(current_row, col, value, get_severity_format(formats, value or 0))
                    elif fmt_type == 'severity_cat':
                        sev_score = d.get('Severity Score') or 0
                        ws.write(current_row, col, value or 'MINIMAL', 
                                get_severity_format(formats, sev_score, value))
                    else:
                        ws.write(current_row, col, value, formats['number'])
            
            current_row += 1
        
        if len(detail_df) > 200:
            ws.write(current_row, 0, f"... and {len(detail_df) - 200} more rows (showing top 200 by severity)", 
                    formats['text'])
    else:
        ws.write(current_row, 0, "No store weather details available", formats['text'])
    
    # Add legend at the bottom with weather icons
    current_row += 3
    ws.merge_range(current_row, 0, current_row, 5, 'Severity Category Legend:', formats['section'])
    current_row += 1
    
    legend_items = [
        (f"{SEVERITY_ICONS['SEVERE']} SEVERE (7-10)", 'severity_severe', 'Dangerous conditions - significant travel hazard'),
        (f"{SEVERITY_ICONS['HIGH']} HIGH (5-7)", 'severity_high', 'Poor conditions - notable impact on foot traffic'),
        (f"{SEVERITY_ICONS['MODERATE']} MODERATE (3-5)", 'severity_moderate', 'Fair conditions - some impact expected'),
        (f"{SEVERITY_ICONS['LOW']} LOW (1.5-3)", 'severity_low', 'Minor conditions - minimal impact'),
        (f"{SEVERITY_ICONS['MINIMAL']} MINIMAL (0-1.5)", 'severity_minimal', 'Good conditions - no weather impact'),
    ]
    
    for label, fmt_key, desc in legend_items:
        ws.write(current_row, 0, label, formats[fmt_key])
        ws.write(current_row, 1, desc, formats['text'])
        current_row += 1
    
    # Add weather icon legend
    current_row += 2
    ws.merge_range(current_row, 0, current_row, 5, 'Weather Condition Icons:', formats['section'])
    current_row += 1
    
    icon_legend = [
        (WEATHER_ICONS['severe'], 'Severe/Dangerous'),
        (WEATHER_ICONS['thunderstorm'], 'Thunderstorm'),
        (WEATHER_ICONS['snow_heavy'], 'Heavy Snow'),
        (WEATHER_ICONS['snow'], 'Snow'),
        (WEATHER_ICONS['rain_heavy'], 'Heavy Rain'),
        (WEATHER_ICONS['rain'], 'Rain'),
        (WEATHER_ICONS['fog'], 'Fog/Mist'),
        (WEATHER_ICONS['wind'], 'High Wind'),
        (WEATHER_ICONS['extreme_cold'], 'Extreme Cold'),
        (WEATHER_ICONS['clear'], 'Clear/Sunny'),
    ]
    
    for icon, desc in icon_legend:
        ws.write(current_row, 0, icon, formats['text_center'])
        ws.write(current_row, 1, desc, formats['text'])
        current_row += 1
    
    # Freeze panes
    ws.freeze_panes(5, 3)


# =============================================================================
# MAIN EXPORT FUNCTION
# =============================================================================

def export_regional_summary(conn, region: str,
                            start_date: datetime, end_date: datetime,
                            output_dir: str = None):
    """
    Export comprehensive regional summary to Excel.
    
    Creates a multi-sheet workbook with:
    - Daily Summary: Aggregated metrics by forecast date
    - Store Summary: Aggregated metrics by store with weather
    - Item Summary: Aggregated metrics by item per date
    - Weather Impact: Comprehensive weather impact analysis
    - Item Details: Full item/store level detail
    
    Args:
        conn: DuckDB connection
        region: Region code
        start_date: Start date
        end_date: End date
        output_dir: Output directory (default: settings.EXCEL_OUTPUT_DIR/excel_summary)
    """
    # Set up output directory
    if output_dir is None:
        output_dir = os.path.join(
            os.path.dirname(settings.EXCEL_OUTPUT_DIR),
            'excel_summary'
        )
    os.makedirs(output_dir, exist_ok=True)
    
    # Format dates
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Create filename
    filename = f'{region}_{start_str}_{end_str}_Summary.xlsx'
    filepath = os.path.join(output_dir, filename)
    
    print(f"Creating regional summary for {region}: {filepath}")
    
    # Create workbook
    wb = xlsxwriter.Workbook(filepath, {'strings_to_numbers': True})
    
    # Create formats
    formats = create_summary_formats(wb)
    
    # Create worksheets
    write_daily_summary_sheet(wb, conn, region, start_str, end_str, formats)
    write_store_summary_sheet(wb, conn, region, start_str, end_str, formats)
    write_item_summary_sheet(wb, conn, region, start_str, end_str, formats)
    write_weather_impact_sheet(wb, conn, region, start_str, end_str, formats)
    write_item_detail_sheet(wb, conn, region, start_str, end_str, formats)
    
    # Close workbook
    wb.close()
    
    print(f"Regional summary exported: {filepath}")
    return filepath


def export_all_regional_summaries(conn, regions: list,
                                  start_date: datetime, end_date: datetime,
                                  output_dir: str = None):
    """
    Export regional summaries for all regions.
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date
        end_date: End date
        output_dir: Output directory
        
    Returns:
        List of created file paths
    """
    filepaths = []
    for region in regions:
        try:
            filepath = export_regional_summary(conn, region, start_date, end_date, output_dir)
            filepaths.append(filepath)
        except Exception as e:
            print(f"Error exporting summary for region {region}: {e}")
    
    return filepaths
