"""
Aggregate Tables Module
=======================
Creates and persists aggregate tables in DuckDB for:
1. Executive reporting (waterfall analysis, regional summaries)
2. External consumption (Power BI, Fabric ingestion)
3. AI agent analysis

All calculations are done once during forecast processing and stored,
avoiding duplicate calculations in export functions.
"""

from datetime import datetime
from typing import Optional


def create_waterfall_aggregate_table(conn, force: bool = False) -> None:
    """
    Create the waterfall_aggregate table in DuckDB.
    
    This table pre-calculates all waterfall components at region/date level
    directly from forecast_results tracking columns.
    
    Args:
        conn: DuckDB connection object
        force: If True, drops existing table first
    """
    if force:
        conn.execute('DROP TABLE IF EXISTS waterfall_aggregate;')
    
    create_query = """
    CREATE TABLE IF NOT EXISTS waterfall_aggregate (
        region_code VARCHAR,
        date_forecast DATE,
        day_name VARCHAR,
        
        -- Counts
        store_count INTEGER,
        item_count INTEGER,
        line_count INTEGER,
        
        -- Starting Point (Last Week)
        lw_shipped BIGINT,
        lw_sold BIGINT,
        
        -- Baseline Adjustment Components
        baseline_lw_sales_qty BIGINT,      -- Items using LW sales as baseline
        baseline_lw_sales_count INTEGER,   -- Count of items using LW sales
        baseline_ema_qty BIGINT,           -- Items using EMA as baseline
        baseline_ema_count INTEGER,        -- Count of items using EMA
        baseline_avg_qty BIGINT,           -- Items using average as baseline  
        baseline_avg_count INTEGER,        -- Count of items using average
        baseline_min_case_qty BIGINT,      -- Items using minimum case
        baseline_min_case_count INTEGER,   -- Count of items using minimum case
        
        -- Baseline Uplift (total change from w1_sold to baseline, for ALL sources)
        baseline_uplift_qty DOUBLE,        -- Total baseline uplift quantity
        baseline_uplift_count INTEGER,     -- Count of items with baseline uplift
        
        -- EMA Uplift (items where LW < EMA) - legacy, for EMA-specific tracking
        ema_uplift_qty DOUBLE,             -- Total uplift quantity
        ema_uplift_count INTEGER,          -- Count of items with EMA uplift
        
        -- Decline Adjustment (items with significant WoW decline pattern)
        decline_adj_qty DOUBLE,            -- Total decline adjustment quantity
        decline_adj_count INTEGER,         -- Count of items with decline adjustment
        
        -- High Shrink Adjustment (items with consecutive high shrink - reduces forecast)
        high_shrink_adj_qty DOUBLE,        -- Total high shrink reduction (negative)
        high_shrink_adj_count INTEGER,     -- Count of items with high shrink adjustment
        
        -- Base Cover Components
        base_cover_total_qty DOUBLE,       -- Total base cover added
        base_cover_default_qty DOUBLE,     -- Default cover quantity
        base_cover_default_count INTEGER,  -- Items with default cover
        base_cover_soldout_qty DOUBLE,     -- Sold-out cover quantity
        base_cover_soldout_count INTEGER,  -- Items with sold-out cover
        
        -- Rounding Components (separated)
        rounding_up_qty DOUBLE,            -- Total rounded UP (positive)
        rounding_up_count INTEGER,         -- Items rounded up
        rounding_down_qty DOUBLE,          -- Total rounded DOWN (negative value = reduction)
        rounding_down_count INTEGER,       -- Items rounded down
        rounding_net_qty DOUBLE,           -- Net rounding impact (up - down)
        
        -- Guardrail Component
        guardrail_adj_qty DOUBLE,          -- Total guardrail reduction (negative)
        guardrail_count INTEGER,           -- Items with guardrail applied
        
        -- Safety Stock Component
        safety_stock_qty DOUBLE,           -- Total safety stock added
        safety_stock_count INTEGER,        -- Items with safety stock applied
        
        -- Store Pass Components (separated)
        store_pass_decline_qty DOUBLE,     -- Total decline (shrink control)
        store_pass_decline_count INTEGER,  -- Items/stores with decline
        store_pass_growth_qty DOUBLE,      -- Total growth (coverage add)
        store_pass_growth_count INTEGER,   -- Items/stores with growth
        store_pass_net_qty DOUBLE,         -- Net store pass impact
        store_pass_stores_adjusted INTEGER,-- Number of stores adjusted
        
        -- Weather Adjustment
        weather_adj_qty DOUBLE,            -- Total weather adjustment (typically negative)
        weather_adj_count INTEGER,         -- Items weather adjusted
        
        -- ===== NEW: Adjustment Type Tracking =====
        -- Promotional Adjustments
        promo_adj_qty DOUBLE,              -- Total promotional uplift
        promo_adj_count INTEGER,           -- Items with promo adjustment
        
        -- Holiday Increase Adjustments
        holiday_increase_adj_qty DOUBLE,   -- Total holiday increase
        holiday_increase_adj_count INTEGER,-- Items with holiday increase
        
        -- Cannibalism Adjustments
        cannibalism_adj_qty DOUBLE,        -- Total cannibalism reduction (negative)
        cannibalism_adj_count INTEGER,     -- Items with cannibalism
        
        -- Adhoc Increase Adjustments
        adhoc_increase_adj_qty DOUBLE,     -- Total adhoc increase
        adhoc_increase_adj_count INTEGER,  -- Items with adhoc increase
        
        -- Adhoc Decrease Adjustments
        adhoc_decrease_adj_qty DOUBLE,     -- Total adhoc decrease (negative)
        adhoc_decrease_adj_count INTEGER,  -- Items with adhoc decrease
        
        -- Store Specific Adjustments
        store_specific_adj_qty DOUBLE,     -- Total store-specific adjustments
        store_specific_adj_count INTEGER,  -- Items with store-specific adj
        
        -- Item Specific Adjustments
        item_specific_adj_qty DOUBLE,      -- Total item-specific adjustments
        item_specific_adj_count INTEGER,   -- Items with item-specific adj
        
        -- Regional Adjustments
        regional_adj_qty DOUBLE,           -- Total regional adjustments
        regional_adj_count INTEGER,        -- Items with regional adj
        
        -- Intermediate Totals (for verification)
        total_forecast_avg DOUBLE,         -- Sum of forecast_average
        total_with_cover DOUBLE,           -- Sum of forecast_average_w_cover
        pre_store_pass_qty DOUBLE,         -- Sum before store pass
        pre_weather_qty DOUBLE,            -- Sum before weather
        
        -- Final Forecast
        final_forecast_qty BIGINT,
        
        -- Delta Metrics
        delta_from_lw_sales BIGINT,        -- final - lw_sold
        delta_from_lw_sales_pct DOUBLE,    -- % change from LW sales
        delta_from_lw_shipped BIGINT,      -- final - lw_shipped
        delta_from_lw_shipped_pct DOUBLE,  -- % change from LW shipped
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        conn.execute(create_query)
        print("Table 'waterfall_aggregate' created successfully.")
    except Exception as e:
        print(f"Error creating waterfall_aggregate table: {e}")


def populate_waterfall_aggregate(conn, regions: list, start_date: str, end_date: str) -> None:
    """
    Populate the waterfall_aggregate table from forecast_results.
    
    Uses the new tracking columns in forecast_results to build the waterfall
    without re-computing metrics.
    
    Args:
        conn: DuckDB connection object
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
    """
    regions_str = "', '".join(regions)
    
    # First clear existing data for this date range
    conn.execute(f"""
        DELETE FROM waterfall_aggregate
        WHERE region_code IN ('{regions_str}')
        AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    """)
    
    insert_query = f"""
    INSERT INTO waterfall_aggregate
    SELECT
        fr.region_code,
        fr.date_forecast,
        strftime(fr.date_forecast, '%A') AS day_name,
        
        -- Counts
        COUNT(DISTINCT fr.store_no) AS store_count,
        COUNT(DISTINCT fr.item_no) AS item_count,
        COUNT(*) AS line_count,
        
        -- Starting Point
        SUM(fr.w1_shipped) AS lw_shipped,
        SUM(fr.w1_sold) AS lw_sold,
        
        -- Baseline by Source
        SUM(CASE WHEN fr.baseline_source = 'lw_sales' THEN fr.baseline_qty ELSE 0 END) AS baseline_lw_sales_qty,
        SUM(CASE WHEN fr.baseline_source = 'lw_sales' THEN 1 ELSE 0 END) AS baseline_lw_sales_count,
        SUM(CASE WHEN fr.baseline_source = 'ema' THEN fr.baseline_qty ELSE 0 END) AS baseline_ema_qty,
        SUM(CASE WHEN fr.baseline_source = 'ema' THEN 1 ELSE 0 END) AS baseline_ema_count,
        SUM(CASE WHEN fr.baseline_source = 'average' THEN fr.baseline_qty ELSE 0 END) AS baseline_avg_qty,
        SUM(CASE WHEN fr.baseline_source = 'average' THEN 1 ELSE 0 END) AS baseline_avg_count,
        SUM(CASE WHEN fr.baseline_source = 'minimum_case' THEN fr.baseline_qty ELSE 0 END) AS baseline_min_case_qty,
        SUM(CASE WHEN fr.baseline_source = 'minimum_case' THEN 1 ELSE 0 END) AS baseline_min_case_count,
        
        -- Baseline Uplift (total change from w1_sold to baseline, for ALL sources)
        SUM(COALESCE(fr.baseline_uplift_qty, 0)) AS baseline_uplift_qty,
        SUM(CASE WHEN fr.baseline_uplift_qty > 0 THEN 1 ELSE 0 END) AS baseline_uplift_count,
        
        -- EMA Uplift (legacy, for EMA-specific tracking)
        SUM(COALESCE(fr.ema_uplift_qty, 0)) AS ema_uplift_qty,
        SUM(CASE WHEN fr.ema_uplift_applied = 1 THEN 1 ELSE 0 END) AS ema_uplift_count,
        
        -- Decline Adjustment
        SUM(COALESCE(fr.decline_adj_qty, 0)) AS decline_adj_qty,
        SUM(CASE WHEN fr.decline_adj_applied = 1 THEN 1 ELSE 0 END) AS decline_adj_count,
        
        -- High Shrink Adjustment
        SUM(COALESCE(fr.high_shrink_adj_qty, 0)) AS high_shrink_adj_qty,
        SUM(CASE WHEN fr.high_shrink_adj_applied = 1 THEN 1 ELSE 0 END) AS high_shrink_adj_count,
        
        -- Base Cover
        SUM(COALESCE(fr.base_cover_qty, 0)) AS base_cover_total_qty,
        SUM(CASE WHEN fr.base_cover_type = 'standard' THEN COALESCE(fr.base_cover_qty, 0) ELSE 0 END) AS base_cover_default_qty,
        SUM(CASE WHEN fr.base_cover_type = 'standard' THEN 1 ELSE 0 END) AS base_cover_default_count,
        SUM(CASE WHEN fr.base_cover_type = 'sold_out' THEN COALESCE(fr.base_cover_qty, 0) ELSE 0 END) AS base_cover_soldout_qty,
        SUM(CASE WHEN fr.base_cover_type = 'sold_out' THEN 1 ELSE 0 END) AS base_cover_soldout_count,
        
        -- Rounding (from row-level tracking)
        SUM(COALESCE(fr.rounding_up_qty, 0)) AS rounding_up_qty,
        SUM(CASE WHEN fr.rounding_direction = 'up' THEN 1 ELSE 0 END) AS rounding_up_count,
        SUM(COALESCE(fr.rounding_down_qty, 0)) AS rounding_down_qty,
        SUM(CASE WHEN fr.rounding_direction = 'down' THEN 1 ELSE 0 END) AS rounding_down_count,
        SUM(COALESCE(fr.rounding_net_qty, 0)) AS rounding_net_qty,
        
        -- Guardrail Adjustment
        SUM(COALESCE(fr.guardrail_adj_qty, 0)) AS guardrail_adj_qty,
        SUM(CASE WHEN fr.guardrail_applied = 1 THEN 1 ELSE 0 END) AS guardrail_count,
        
        -- Safety Stock
        SUM(COALESCE(fr.forecast_safety_stock_applied, 0)) AS safety_stock_qty,
        SUM(CASE WHEN fr.forecast_safety_stock_applied > 0 THEN 1 ELSE 0 END) AS safety_stock_count,
        
        -- Store Pass (from row-level tracking - decline is negative, growth is positive)
        SUM(COALESCE(fr.store_level_decline_qty, 0)) AS store_pass_decline_qty,
        SUM(CASE WHEN fr.store_level_decline_qty < 0 THEN 1 ELSE 0 END) AS store_pass_decline_count,
        SUM(COALESCE(fr.store_level_growth_qty, 0)) AS store_pass_growth_qty,
        SUM(CASE WHEN fr.store_level_growth_qty > 0 THEN 1 ELSE 0 END) AS store_pass_growth_count,
        SUM(COALESCE(fr.store_level_decline_qty, 0)) + SUM(COALESCE(fr.store_level_growth_qty, 0)) AS store_pass_net_qty,
        COUNT(DISTINCT CASE WHEN fr.store_level_adjusted = 1 THEN fr.store_no END) AS store_pass_stores_adjusted,
        
        -- Weather (now stored as negative = reduction, so use directly)
        SUM(COALESCE(fr.weather_adjustment_qty, 0)) AS weather_adj_qty,
        SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END) AS weather_adj_count,
        
        -- ===== Adjustment Type Tracking =====
        -- Promotional Adjustments
        SUM(COALESCE(fr.promo_adj_qty, 0)) AS promo_adj_qty,
        SUM(CASE WHEN fr.promo_adj_applied = 1 THEN 1 ELSE 0 END) AS promo_adj_count,
        
        -- Holiday Increase Adjustments
        SUM(COALESCE(fr.holiday_increase_adj_qty, 0)) AS holiday_increase_adj_qty,
        SUM(CASE WHEN fr.holiday_increase_adj_applied = 1 THEN 1 ELSE 0 END) AS holiday_increase_adj_count,
        
        -- Cannibalism Adjustments
        SUM(COALESCE(fr.cannibalism_adj_qty, 0)) AS cannibalism_adj_qty,
        SUM(CASE WHEN fr.cannibalism_adj_applied = 1 THEN 1 ELSE 0 END) AS cannibalism_adj_count,
        
        -- Adhoc Increase Adjustments
        SUM(COALESCE(fr.adhoc_increase_adj_qty, 0)) AS adhoc_increase_adj_qty,
        SUM(CASE WHEN fr.adhoc_increase_adj_applied = 1 THEN 1 ELSE 0 END) AS adhoc_increase_adj_count,
        
        -- Adhoc Decrease Adjustments
        SUM(COALESCE(fr.adhoc_decrease_adj_qty, 0)) AS adhoc_decrease_adj_qty,
        SUM(CASE WHEN fr.adhoc_decrease_adj_applied = 1 THEN 1 ELSE 0 END) AS adhoc_decrease_adj_count,
        
        -- Store Specific Adjustments
        SUM(COALESCE(fr.store_specific_adj_qty, 0)) AS store_specific_adj_qty,
        SUM(CASE WHEN fr.store_specific_adj_applied = 1 THEN 1 ELSE 0 END) AS store_specific_adj_count,
        
        -- Item Specific Adjustments
        SUM(COALESCE(fr.item_specific_adj_qty, 0)) AS item_specific_adj_qty,
        SUM(CASE WHEN fr.item_specific_adj_applied = 1 THEN 1 ELSE 0 END) AS item_specific_adj_count,
        
        -- Regional Adjustments
        SUM(COALESCE(fr.regional_adj_qty, 0)) AS regional_adj_qty,
        SUM(CASE WHEN fr.regional_adj_applied = 1 THEN 1 ELSE 0 END) AS regional_adj_count,
        
        -- Intermediate Totals
        
        -- Intermediate Totals
        SUM(fr.forecast_average) AS total_forecast_avg,
        SUM(fr.forecast_average_w_cover) AS total_with_cover,
        SUM(COALESCE(fr.forecast_qty_pre_store_pass, fr.forecast_quantity)) AS pre_store_pass_qty,
        SUM(COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity)) AS pre_weather_qty,
        
        -- Final
        SUM(fr.forecast_quantity) AS final_forecast_qty,
        
        -- Deltas
        SUM(fr.forecast_quantity) - SUM(fr.w1_sold) AS delta_from_lw_sales,
        CASE WHEN SUM(fr.w1_sold) > 0 
            THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_sold), 4)
            ELSE 0 END AS delta_from_lw_sales_pct,
        SUM(fr.forecast_quantity) - SUM(fr.w1_shipped) AS delta_from_lw_shipped,
        CASE WHEN SUM(fr.w1_shipped) > 0 
            THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
            ELSE 0 END AS delta_from_lw_shipped_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP AS created_at
        
    FROM forecast_results fr
    WHERE fr.region_code IN ('{regions_str}')
    AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY fr.region_code, fr.date_forecast
    ORDER BY fr.region_code, fr.date_forecast
    """
    
    try:
        conn.execute(insert_query)
        count = conn.execute(f"""
            SELECT COUNT(*) FROM waterfall_aggregate
            WHERE region_code IN ('{regions_str}')
            AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
        """).fetchone()[0]
        print(f"  Waterfall aggregate: {count} region-date rows populated")
    except Exception as e:
        print(f"Error populating waterfall_aggregate: {e}")


def create_daily_summary_aggregate_table(conn, force: bool = False) -> None:
    """
    Create the daily_summary_aggregate table in DuckDB.
    
    This table provides daily totals by region for executive dashboards.
    
    Args:
        conn: DuckDB connection object
        force: If True, drops existing table first
    """
    if force:
        conn.execute('DROP TABLE IF EXISTS daily_summary_aggregate;')
    
    create_query = """
    CREATE TABLE IF NOT EXISTS daily_summary_aggregate (
        region_code VARCHAR,
        date_forecast DATE,
        day_name VARCHAR,
        
        -- Counts
        store_count INTEGER,
        item_count INTEGER,
        line_count INTEGER,
        
        -- Forecast Quantities
        forecast_shipped BIGINT,           -- Final forecast (what we'll ship)
        forecast_sales BIGINT,             -- Expected sales (forecast_average)
        expected_shrink_qty BIGINT,        -- forecast_shipped - forecast_sales
        expected_shrink_pct DOUBLE,        -- expected_shrink_qty / forecast_shipped
        
        -- Last Week Actuals
        lw_shipped BIGINT,
        lw_sold BIGINT,
        lw_shrink_qty BIGINT,
        lw_shrink_pct DOUBLE,
        
        -- Week over Week Changes
        delta_shipped_qty BIGINT,
        delta_shipped_pct DOUBLE,
        delta_sales_qty BIGINT,
        delta_sales_pct DOUBLE,
        
        -- Historical Trend
        w4_shipped BIGINT,
        w3_shipped BIGINT,
        w2_shipped BIGINT,
        w1_shipped BIGINT,
        w4_sold BIGINT,
        w3_sold BIGINT,
        w2_sold BIGINT,
        w1_sold BIGINT,
        
        -- Adjustment Totals
        total_ema_uplift DOUBLE,
        total_base_cover DOUBLE,
        total_rounding_net DOUBLE,
        total_store_pass_net DOUBLE,
        total_weather_adj DOUBLE,
        
        -- Weather Metrics
        avg_weather_severity DOUBLE,
        items_weather_adjusted INTEGER,
        stores_with_weather_impact INTEGER,
        
        -- Store Pass Metrics
        stores_with_store_pass INTEGER,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        conn.execute(create_query)
        print("Table 'daily_summary_aggregate' created successfully.")
    except Exception as e:
        print(f"Error creating daily_summary_aggregate table: {e}")


def populate_daily_summary_aggregate(conn, regions: list, start_date: str, end_date: str) -> None:
    """
    Populate the daily_summary_aggregate table from forecast_results.
    
    Args:
        conn: DuckDB connection object
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
    """
    regions_str = "', '".join(regions)
    
    # First clear existing data for this date range
    conn.execute(f"""
        DELETE FROM daily_summary_aggregate
        WHERE region_code IN ('{regions_str}')
        AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    """)
    
    insert_query = f"""
    INSERT INTO daily_summary_aggregate
    SELECT
        fr.region_code,
        fr.date_forecast,
        strftime(fr.date_forecast, '%A') AS day_name,
        
        -- Counts
        COUNT(DISTINCT fr.store_no) AS store_count,
        COUNT(DISTINCT fr.item_no) AS item_count,
        COUNT(*) AS line_count,
        
        -- Forecast Quantities
        SUM(fr.forecast_quantity) AS forecast_shipped,
        SUM(fr.forecast_average) AS forecast_sales,
        SUM(fr.forecast_quantity) - SUM(fr.forecast_average) AS expected_shrink_qty,
        CASE WHEN SUM(fr.forecast_quantity) > 0 
            THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.forecast_average))::DOUBLE / SUM(fr.forecast_quantity), 4)
            ELSE 0 END AS expected_shrink_pct,
        
        -- Last Week
        SUM(fr.w1_shipped) AS lw_shipped,
        SUM(fr.w1_sold) AS lw_sold,
        SUM(fr.w1_shipped) - SUM(fr.w1_sold) AS lw_shrink_qty,
        CASE WHEN SUM(fr.w1_shipped) > 0 
            THEN ROUND((SUM(fr.w1_shipped) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_shipped), 4)
            ELSE 0 END AS lw_shrink_pct,
        
        -- Deltas
        SUM(fr.forecast_quantity) - SUM(fr.w1_shipped) AS delta_shipped_qty,
        CASE WHEN SUM(fr.w1_shipped) > 0 
            THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
            ELSE 0 END AS delta_shipped_pct,
        SUM(fr.forecast_average) - SUM(fr.w1_sold) AS delta_sales_qty,
        CASE WHEN SUM(fr.w1_sold) > 0 
            THEN ROUND((SUM(fr.forecast_average) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_sold), 4)
            ELSE 0 END AS delta_sales_pct,
        
        -- Historical Trend
        SUM(fr.w4_shipped) AS w4_shipped,
        SUM(fr.w3_shipped) AS w3_shipped,
        SUM(fr.w2_shipped) AS w2_shipped,
        SUM(fr.w1_shipped) AS w1_shipped,
        SUM(fr.w4_sold) AS w4_sold,
        SUM(fr.w3_sold) AS w3_sold,
        SUM(fr.w2_sold) AS w2_sold,
        SUM(fr.w1_sold) AS w1_sold,
        
        -- Adjustment Totals
        SUM(COALESCE(fr.ema_uplift_qty, 0)) AS total_ema_uplift,
        SUM(COALESCE(fr.base_cover_qty, 0)) AS total_base_cover,
        SUM(COALESCE(fr.rounding_net_qty, 0)) AS total_rounding_net,
        SUM(COALESCE(fr.store_level_growth_qty, 0)) + SUM(COALESCE(fr.store_level_decline_qty, 0)) AS total_store_pass_net,
        SUM(COALESCE(fr.weather_adjustment_qty, 0)) AS total_weather_adj,
        
        -- Weather
        ROUND(AVG(COALESCE(fr.weather_severity_score, 0)), 2) AS avg_weather_severity,
        SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_weather_adjusted,
        COUNT(DISTINCT CASE WHEN fr.weather_adjusted = 1 THEN fr.store_no END) AS stores_with_weather_impact,
        
        -- Store Pass
        COUNT(DISTINCT CASE WHEN fr.store_level_adjusted = 1 THEN fr.store_no END) AS stores_with_store_pass,
        
        -- Metadata
        CURRENT_TIMESTAMP AS created_at
        
    FROM forecast_results fr
    WHERE fr.region_code IN ('{regions_str}')
    AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY fr.region_code, fr.date_forecast
    ORDER BY fr.region_code, fr.date_forecast
    """
    
    try:
        conn.execute(insert_query)
        count = conn.execute(f"""
            SELECT COUNT(*) FROM daily_summary_aggregate
            WHERE region_code IN ('{regions_str}')
            AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
        """).fetchone()[0]
        print(f"  Daily summary aggregate: {count} region-date rows populated")
    except Exception as e:
        print(f"Error populating daily_summary_aggregate: {e}")


def create_all_aggregate_tables(conn, force: bool = False) -> None:
    """
    Create all aggregate tables in DuckDB.
    
    Args:
        conn: DuckDB connection object
        force: If True, drops existing tables first
    """
    print("\nCreating aggregate tables...")
    create_waterfall_aggregate_table(conn, force)
    create_daily_summary_aggregate_table(conn, force)


def populate_all_aggregates(conn, regions: list, start_date: str, end_date: str) -> None:
    """
    Populate all aggregate tables from forecast_results.
    
    This should be called after all forecast processing is complete
    and before any export functions.
    
    Args:
        conn: DuckDB connection object
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
    """
    print("\nPopulating aggregate tables...")
    populate_waterfall_aggregate(conn, regions, start_date, end_date)
    populate_daily_summary_aggregate(conn, regions, start_date, end_date)
    print("Aggregate tables populated successfully.")
