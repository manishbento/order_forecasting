"""
Data Preparation Module
=======================
Handles data cleaning, transformation, and preparation for forecasting.

This module is responsible for:
- Creating forecast results table schema
- Preparing historical week date calculations
- Handling item substitutions
- Data validation and cleaning
- Platter inclusion based on date/region configuration
"""

import duckdb
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

from config import settings


def get_platter_inclusion_config(region_code: str, forecast_date: str) -> Optional[dict]:
    """
    Check if platters should be included for the given region and date.
    
    Args:
        region_code: Region code (e.g., 'BA', 'LA')
        forecast_date: Forecast date string (YYYY-MM-DD)
        
    Returns:
        Platter inclusion config dict if platters should be included, None otherwise.
        The dict contains 'items' key which is None (all platters) or a list of item numbers.
    """
    for inclusion in settings.PLATTER_INCLUSIONS:
        if inclusion['region'] == region_code:
            if inclusion['start_date'] <= forecast_date <= inclusion['end_date']:
                return inclusion
    return None


def get_historical_week_dates(forecast_date: datetime, 
                              num_weeks: int = 4,
                              region_code: str = None,
                              exceptional_days: List[str] = None) -> List[str]:
    """
    Calculate historical week dates for forecasting, excluding exceptional days.
    
    For each forecast date, we need data from the same day of previous weeks.
    Any date falling on an exceptional day is skipped.
    
    Args:
        forecast_date: The date being forecasted
        num_weeks: Number of historical weeks needed (default 4)
        region_code: Region code to filter exceptional days (e.g., 'BA', 'LA')
        exceptional_days: List of dates to exclude (YYYY-MM-DD format).
                         If None, will use get_exceptional_days_for_region(region_code)
        
    Returns:
        List of date strings for W1, W2, W3, W4 (most recent first)
    """
    if exceptional_days is None:
        exceptional_days = settings.get_exceptional_days_for_region(region_code)
    
    w_dates = []
    week_offset = 1
    
    while len(w_dates) < num_weeks and week_offset <= num_weeks + 5:
        w_date = forecast_date - timedelta(weeks=week_offset)
        date_str = w_date.strftime('%Y-%m-%d')
        
        if date_str not in exceptional_days:
            w_dates.append(date_str)
        
        week_offset += 1
    
    return w_dates


def create_forecast_results_table(conn: duckdb.DuckDBPyConnection, force: bool = True):
    """
    Create the forecast_results table in DuckDB.
    
    This table stores all forecast outputs and calculated metrics.
    
    Args:
        conn: DuckDB connection object
        force: If True, drops existing table first
    """
    if force:
        conn.execute('DROP TABLE IF EXISTS forecast_results;')
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS forecast_results (
        -- Identifiers from the base query
        item_no BIGINT,
        item_desc VARCHAR,
        store_no BIGINT,
        region_code VARCHAR,
        date_forecast DATE,
        date_base_w1 DATE,
        date_base_w2 DATE,
        date_base_w3 DATE,
        date_base_w4 DATE,

        -- Parameters (save to database too)
        base_cover DOUBLE,
        base_cover_sold_out DOUBLE,
        base_cover_applied DOUBLE,
        k_factor DOUBLE,

        -- Base data from the query
        case_pack_size INTEGER,
        w1_cost_unit DOUBLE,
        w1_price_unit DOUBLE,
        avg_four_week_sales DOUBLE,
        w1_shipped BIGINT,
        w2_shipped BIGINT,
        w3_shipped BIGINT,
        w4_shipped BIGINT,
        w1_sold BIGINT,
        w2_sold BIGINT,
        w3_sold BIGINT,
        w4_sold BIGINT,
        w1_shrink_p DOUBLE,
        w2_shrink_p DOUBLE,
        w3_shrink_p DOUBLE,
        w4_shrink_p DOUBLE,
        
        -- Store-level aggregates
        store_w1_received BIGINT,
        store_w1_sold BIGINT,
        store_w1_shrink BIGINT,
        store_w1_shrink_p DOUBLE,
        store_w2_received BIGINT,
        store_w2_sold BIGINT,
        store_w2_shrink BIGINT,
        store_w2_shrink_p DOUBLE,
        store_w3_received BIGINT,
        store_w3_sold BIGINT,
        store_w3_shrink BIGINT,
        store_w3_shrink_p DOUBLE,
        store_w4_received BIGINT,
        store_w4_sold BIGINT,
        store_w4_shrink BIGINT,
        store_w4_shrink_p DOUBLE,

        -- Actuals (if available)
        result_shipped BIGINT,
        result_sold BIGINT,
        result_shrink_p DOUBLE,
        result_store_received BIGINT,
        result_store_sold BIGINT,
        result_store_shrink BIGINT,
        result_store_shrink_p DOUBLE,
        result_price_unit DOUBLE,

        -- Sold-out flag (last week sold == last week shipped, exact match only)
        sold_out_lw INTEGER,

        -- Calculated forecast fields
        sales_velocity DOUBLE,
        sales_volatility DOUBLE,
        average_sold DOUBLE,
        ema DOUBLE,
        forecast_average DOUBLE,
        forecast_average_w_cover DOUBLE,
        round_up_quantity DOUBLE,
        round_up_final DOUBLE,
        forecast_quantity DOUBLE,
        delta_from_last_week DOUBLE,
        impact_of_rounding DOUBLE,
        forecast_safety_stock DOUBLE,
        forecast_safety_stock_applied BIGINT,

        -- Forecasting metrics
        forecast_shrink_last_week_sales DOUBLE,
        forecast_shrink_average DOUBLE,

        -- Parameters recorded
        forecast_w1_weight DOUBLE,
        forecast_w2_weight DOUBLE,
        forecast_w3_weight DOUBLE,
        forecast_w4_weight DOUBLE,
        forecast_high_shrink_threshold DOUBLE,
        forecast_round_down_shrink_threshold DOUBLE,

        -- Weather Data from VisualCrossing
        weather_day_condition VARCHAR,
        weather_day_low_rain INTEGER,
        weather_day_medium_rain INTEGER,
        weather_day_high_rain INTEGER,
        weather_total_rain_expected REAL,
        weather_latitude REAL,
        weather_longitude REAL,
        weather_resolved_address VARCHAR,
        weather_timezone VARCHAR,

        -- Weather Data from AccuWeather
        accuweather_day_condition VARCHAR,
        accuweather_day_low_rain INTEGER,
        accuweather_day_medium_rain INTEGER,
        accuweather_day_high_rain INTEGER,
        accuweather_total_rain_expected REAL,
        accuweather_temp_max REAL,
        accuweather_temp_min REAL,
        accuweather_realfeel_temp_max REAL,
        accuweather_realfeel_temp_min REAL,
        accuweather_hours_of_sun REAL,
        accuweather_hours_of_rain REAL,
        accuweather_day_short_phrase VARCHAR,
        accuweather_day_long_phrase VARCHAR,

        -- Weather Severity Metrics (from enhanced VisualCrossing processing)
        weather_severity_score REAL,
        weather_severity_category VARCHAR,
        weather_sales_impact_factor REAL,
        weather_rain_severity REAL,
        weather_snow_severity REAL,
        weather_wind_severity REAL,
        weather_visibility_severity REAL,
        weather_temp_severity REAL,
        weather_snow_amount REAL,
        weather_snow_depth REAL,
        weather_wind_speed REAL,
        weather_wind_gust REAL,
        weather_temp_max REAL,
        weather_temp_min REAL,
        weather_visibility REAL,
        weather_severe_risk REAL,
        weather_precip_probability REAL,
        weather_precip_cover REAL,
        weather_humidity REAL,
        weather_cloud_cover REAL,

        -- Baseline Adjustment Tracking (for waterfall analysis)
        baseline_source VARCHAR,              -- 'lw_sales', 'ema', 'average', 'minimum_case'
        baseline_qty DOUBLE,                  -- The baseline value used
        baseline_adj_qty DOUBLE,              -- forecast_average - w1_sold
        ema_uplift_applied INTEGER,           -- 1 if EMA > LW sales and EMA was used
        ema_uplift_qty DOUBLE,                -- EMA - LW sales (positive when EMA > LW)
        baseline_uplift_qty DOUBLE,           -- Uplift from w1_sold to baseline (for ALL baseline sources)
        
        -- Decline Adjustment Tracking (for items with significant WoW decline)
        decline_adj_applied INTEGER,          -- 1 if decline adjustment was applied
        decline_adj_qty DOUBLE,               -- Additional qty added due to decline pattern
        
        -- High Shrink Adjustment Tracking (for items with consecutive high shrink)
        high_shrink_adj_applied INTEGER,      -- 1 if high shrink adjustment was applied
        high_shrink_adj_qty DOUBLE,           -- Qty reduction due to high shrink (negative)
        
        -- Base Cover Tracking
        base_cover_qty DOUBLE,                -- The actual cover quantity added
        base_cover_type VARCHAR,              -- 'default' or 'sold_out'
        
        -- Rounding Adjustment Tracking (separated into up/down)
        rounding_direction VARCHAR,           -- 'up', 'down', or 'none'
        rounding_up_qty DOUBLE,               -- Positive quantity added due to rounding up
        rounding_down_qty DOUBLE,             -- Positive quantity removed due to rounding down
        rounding_net_qty DOUBLE,              -- Net impact of rounding (up - down)
        
        -- Guardrail Adjustment Fields
        guardrail_adj_qty DOUBLE,             -- Quantity reduced by guardrail (negative value)
        guardrail_applied INTEGER,            -- 1 if guardrail was applied, 0 otherwise

        -- Store-Level Pass Adjustment Fields (separated into growth/decline)
        forecast_qty_pre_store_pass DOUBLE,
        store_level_adjustment_qty DOUBLE,
        store_level_growth_qty DOUBLE,        -- Positive qty added (when coverage too low)
        store_level_decline_qty DOUBLE,       -- Negative qty removed (shrink control)
        store_level_adjustment_reason VARCHAR,
        store_level_adjusted INTEGER,
        store_level_shrink_pct DOUBLE,
        store_level_coverage_pct DOUBLE,

        -- Weather Adjustment Fields
        forecast_qty_pre_weather DOUBLE,
        weather_adjustment_qty DOUBLE,
        weather_adjustment_reason VARCHAR,
        weather_adjusted INTEGER,
        weather_status_indicator VARCHAR,
        sales_trend_4wk VARCHAR,
        
        -- ===== Adjustment Type Tracking Fields =====
        -- Promotional Adjustments
        promo_adj_applied INTEGER,
        promo_adj_qty DOUBLE,
        promo_adj_name VARCHAR,
        promo_adj_multiplier DOUBLE,
        
        -- Holiday Increase Adjustments
        holiday_increase_adj_applied INTEGER,
        holiday_increase_adj_qty DOUBLE,
        holiday_increase_adj_name VARCHAR,
        holiday_increase_adj_multiplier DOUBLE,
        
        -- Cannibalism Adjustments
        cannibalism_adj_applied INTEGER,
        cannibalism_adj_qty DOUBLE,
        cannibalism_adj_name VARCHAR,
        cannibalism_adj_multiplier DOUBLE,
        
        -- Adhoc Increase Adjustments
        adhoc_increase_adj_applied INTEGER,
        adhoc_increase_adj_qty DOUBLE,
        adhoc_increase_adj_name VARCHAR,
        adhoc_increase_adj_multiplier DOUBLE,
        
        -- Adhoc Decrease Adjustments
        adhoc_decrease_adj_applied INTEGER,
        adhoc_decrease_adj_qty DOUBLE,
        adhoc_decrease_adj_name VARCHAR,
        adhoc_decrease_adj_multiplier DOUBLE,
        
        -- Store Specific Adjustments
        store_specific_adj_applied INTEGER,
        store_specific_adj_qty DOUBLE,
        store_specific_adj_name VARCHAR,
        store_specific_adj_multiplier DOUBLE,
        
        -- Item Specific Adjustments
        item_specific_adj_applied INTEGER,
        item_specific_adj_qty DOUBLE,
        item_specific_adj_name VARCHAR,
        item_specific_adj_multiplier DOUBLE,
        
        -- Regional Adjustments
        regional_adj_applied INTEGER,
        regional_adj_qty DOUBLE,
        regional_adj_name VARCHAR,
        regional_adj_multiplier DOUBLE,
        
        -- Legacy promotional tracking (for backward compatibility)
        promo_uplift_applied INTEGER,
        promo_uplift_qty DOUBLE,
        promo_uplift_name VARCHAR,
        promo_uplift_multiplier DOUBLE,
        holiday_adj_applied INTEGER,
        holiday_adj_qty DOUBLE,
        holiday_adj_name VARCHAR,
        holiday_adj_multiplier DOUBLE,

        -- AI Forecasting Fields (placeholder for future)
        ai_forecast DOUBLE,
        ai_difference DOUBLE,
        ai_reasoning VARCHAR,

        -- Result metrics
        result_forecast_case_pack_size BIGINT,
        result_forecast_shrink BIGINT,
        result_forecast_shrink_p DOUBLE,
        result_forecast_sold_out INTEGER,
        result_sales_amount DOUBLE,
        result_forecast_shrink_cost DOUBLE,
        result_forecast_lost_sales DOUBLE,
        result_forecast_margin_amount DOUBLE
    );
    """
    
    try:
        conn.execute(create_table_query)
        print("Table 'forecast_results' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")


def get_forecast_base_query(include_platters: bool = False, platter_items: Optional[List[int]] = None) -> str:
    """
    Return the base SQL query for forecast data extraction.
    
    This query joins shrink data with configuration tables and calculates
    weekly aggregates needed for forecasting.
    
    The query has placeholders for:
    - {region}: Region code
    - {forecast_date}: Date being forecasted
    - {w1}, {w2}, {w3}, {w4}: Historical week dates
    
    Args:
        include_platters: Whether to include platter items in the forecast
        platter_items: Optional list of specific platter item numbers to include.
                      If None and include_platters is True, all platters are included.
    
    Returns:
        SQL query string with placeholders
    """
    # Get case sizes from settings
    platter_case_size = settings.PLATTER_CASE_SIZE
    regular_case_size = settings.REGULAR_CASE_SIZE
    
    # Build the platter exclusion SQL - exclude platters from config_active unless platters should be included
    platter_exclusion_sql = "" if include_platters else "AND ca.item_desc NOT LIKE '%PLATTER%'"
    
    # Build the platter inclusion SQL if needed
    platter_union_sql = ""
    if include_platters:
        if platter_items:
            # Include specific platter items
            items_list = ", ".join(str(item) for item in platter_items)
            platter_filter = f"AND s.item_no IN ({items_list})"
        else:
            # Include all platters (items with PLATTER in description)
            platter_filter = "AND s.item_desc LIKE '%PLATTER%'"
        
        platter_union_sql = f"""
        UNION
        -- Include platter items for this date/region
        SELECT DISTINCT
            s.item_no,
            s.item_desc,
            s.store_no
        FROM clean_shrink s
        CROSS JOIN params p
        WHERE s.region_code = p.region_code
        {platter_filter}
        AND s.date_posting IN (p.date_w1, p.date_w2, p.date_w3, p.date_w4)
        """
    
    # Build the case pack size SQL with the configured values
    case_pack_sql = f"CASE WHEN u.item_desc LIKE '%PLATTER%' THEN {platter_case_size} ELSE {regular_case_size} END AS case_pack_size"
    
    # Use standard string with .format() placeholders for region, dates etc.
    # Only platter_union_sql and case_pack_sql are pre-interpolated
    return '''
    WITH params AS (
        SELECT
            '{region}' AS region_code,
            CAST('{forecast_date}' AS DATE) AS date_forecast,
            CAST('{w1}' AS DATE) AS date_w1,
            CAST('{w2}' AS DATE) AS date_w2,
            CAST('{w3}' AS DATE) AS date_w3,
            CAST('{w4}' AS DATE) AS date_w4
    ),
    -- Create a clean sales history, replacing old SKUs with new ones from the substitute table
    clean_shrink AS (
        SELECT
            COALESCE(CAST(sub.sub_item_no AS BIGINT), s.item_no) AS item_no,
            COALESCE(CAST(sub.sub_item_desc AS VARCHAR), s.item_desc) AS item_desc,
            s.* EXCLUDE (item_no, item_desc)
        FROM main.shrink s
        CROSS JOIN params p
        LEFT JOIN main.config_substitute sub 
            ON s.item_no = sub.item_no 
            AND s.region_code = sub.region_code 
            AND sub.effective_date <= p.date_forecast 
            AND sub.effective_end_date > p.date_forecast
    ),
    -- Create the set of all item/store combinations to forecast
    item_store_universe AS (
        SELECT DISTINCT
            ca.item_no,
            ca.item_desc,
            s.store_no
        FROM config_active ca
        JOIN clean_shrink s ON ca.item_no = s.item_no
        CROSS JOIN params p
        WHERE ca.region_code = p.region_code
        AND s.region_code = p.region_code
        AND ca.active_date <= p.date_forecast
        AND ca.active_end_date >= p.date_forecast
        ''' + platter_exclusion_sql + '''
        ''' + platter_union_sql + '''
    ),
    -- Calculate store-level aggregates for each week
    store_weekly_aggs AS (
        SELECT
            s.store_no,
            -- Result day (forecast date) aggregates
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_received END) AS result_store_received,
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_sold END) AS result_store_sold,
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_shrink END) AS result_store_shrink,
            -- W1 aggregates
            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_received END) AS store_w1_received,
            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_sold END) AS store_w1_sold,
            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_shrink END) AS store_w1_shrink,
            -- W2 aggregates
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_received END) AS store_w2_received,
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_sold END) AS store_w2_sold,
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_shrink END) AS store_w2_shrink,
            -- W3 aggregates
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_received END) AS store_w3_received,
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_sold END) AS store_w3_sold,
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_shrink END) AS store_w3_shrink,
            -- W4 aggregates
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_received END) AS store_w4_received,
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_sold END) AS store_w4_sold,
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_shrink END) AS store_w4_shrink,
            -- Shrink percentages
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_shrink END) / 
                NULLIF(SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_received END), 0) AS result_store_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_shrink END) / 
                NULLIF(SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_received END), 0) AS store_w1_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_shrink END) / 
                NULLIF(SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_received END), 0) AS store_w2_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_shrink END) / 
                NULLIF(SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_received END), 0) AS store_w3_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_shrink END) / 
                NULLIF(SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_received END), 0) AS store_w4_shrink_p
        FROM clean_shrink s
        CROSS JOIN params p
        WHERE s.date_posting IN (p.date_w1, p.date_w2, p.date_w3, p.date_w4, p.date_forecast)
            AND s.region_code = p.region_code
        GROUP BY s.store_no
    )
    -- Final SELECT
    SELECT
        u.item_no, u.item_desc, u.store_no,
        p.region_code, p.date_forecast, 
        p.date_w1 AS date_base_w1, p.date_w2 AS date_base_w2, 
        p.date_w3 AS date_base_w3, p.date_w4 AS date_base_w4,

        -- Dynamic Case Pack Size (platters use smaller case size)
        ''' + case_pack_sql + ''',

        -- Cost and price from most recent week
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.cost_unit END) AS w1_cost_unit,
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.price_unit END) AS w1_price_unit,

        -- Four-week average
        (COALESCE(MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_sold END), 0) + 
         COALESCE(MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_sold END), 0) + 
         COALESCE(MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_sold END), 0) + 
         COALESCE(MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_sold END), 0)) / 4.0 AS avg_four_week_sales,

        -- Result day data (actuals if available)
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.price_unit END) AS result_price_unit,
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_received END) AS result_shipped,
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_sold END) AS result_sold,
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.shrink_percentage END) AS result_shrink_p,

        -- Item-level weekly data
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_received END) AS w1_shipped, 
        MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_received END) AS w2_shipped, 
        MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_received END) AS w3_shipped, 
        MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_received END) AS w4_shipped,
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_sold END) AS w1_sold, 
        MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_sold END) AS w2_sold, 
        MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_sold END) AS w3_sold, 
        MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_sold END) AS w4_sold,
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.shrink_percentage END) AS w1_shrink_p, 
        MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.shrink_percentage END) AS w2_shrink_p, 
        MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.shrink_percentage END) AS w3_shrink_p, 
        MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.shrink_percentage END) AS w4_shrink_p,

        -- Store-level weekly data
        sa.store_w1_received, sa.store_w1_sold, sa.store_w1_shrink, sa.store_w1_shrink_p,
        sa.store_w2_received, sa.store_w2_sold, sa.store_w2_shrink, sa.store_w2_shrink_p,
        sa.store_w3_received, sa.store_w3_sold, sa.store_w3_shrink, sa.store_w3_shrink_p,
        sa.store_w4_received, sa.store_w4_sold, sa.store_w4_shrink, sa.store_w4_shrink_p,
        sa.result_store_received, sa.result_store_sold, sa.result_store_shrink, sa.result_store_shrink_p

    FROM item_store_universe u
    CROSS JOIN params p
    LEFT JOIN clean_shrink s 
        ON u.item_no = s.item_no 
        AND u.store_no = s.store_no 
        AND s.date_posting IN (p.date_w1, p.date_w2, p.date_w3, p.date_w4, p.date_forecast)
    LEFT JOIN store_weekly_aggs sa ON u.store_no = sa.store_no

    GROUP BY ALL
    ORDER BY u.store_no, u.item_no;
    '''


def get_forecast_data(conn: duckdb.DuckDBPyConnection, 
                      region_code: str, 
                      forecast_date: str, 
                      w_dates: List[str]) -> List[dict]:
    """
    Execute forecast base query and return results as list of dictionaries.
    
    Args:
        conn: DuckDB connection
        region_code: Region to forecast (e.g., 'BA', 'LA')
        forecast_date: Date being forecasted (YYYY-MM-DD)
        w_dates: List of historical week dates [w1, w2, w3, w4]
        
    Returns:
        List of dictionaries, each representing a row of forecast base data
    """
    # Check if platters should be included for this region/date
    platter_config = get_platter_inclusion_config(region_code, forecast_date)
    include_platters = platter_config is not None
    platter_items = platter_config.get('items') if platter_config else None
    
    if include_platters:
        if platter_items:
            print(f"  Including specific platter items: {platter_items}")
        else:
            print(f"  Including all platters for {region_code} on {forecast_date}")
    
    query = get_forecast_base_query(
        include_platters=include_platters, 
        platter_items=platter_items
    ).format(
        region=region_code,
        forecast_date=forecast_date,
        w1=w_dates[0],
        w2=w_dates[1],
        w3=w_dates[2],
        w4=w_dates[3]
    )
    
    result = conn.execute(query).fetchall()
    column_names = [desc[0] for desc in conn.description]
    
    # Convert to list of dictionaries
    return [dict(zip(column_names, row)) for row in result]
