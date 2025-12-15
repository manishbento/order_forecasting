"""
Executive Summary SQL Queries
=============================
SQL queries for generating executive summary data.

This module contains all queries for:
- Regional summary metrics by date
- Waterfall analysis components
- Weather adjustment summaries
"""

from config import settings


def _get_inactive_stores_filter(alias: str = "fr") -> str:
    """Build inactive stores filter clause."""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        return f"AND {alias}.store_no NOT IN ({inactive_stores_str})"
    return ""


def _get_inactive_store_items_filter(alias: str = "fr") -> str:
    """Build inactive store-item combinations filter clause."""
    if settings.INACTIVE_STORE_ITEMS:
        conditions = [
            f"({alias}.store_no = {store_no} AND {alias}.item_no = {item_no})"
            for store_no, item_no in settings.INACTIVE_STORE_ITEMS
        ]
        return f"AND NOT ({' OR '.join(conditions)})"
    return ""


def get_regional_summary_query(regions: list, start_date: str, end_date: str) -> str:
    """
    Generate query for regional summary by date.
    
    Provides compact overview with:
    - Forecast shipped (final quantity)
    - Forecast sales (expected based on average)
    - Last week shipped and sold
    - Delta percentages
    - Customer (store) count
    
    Args:
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        
    Returns:
        SQL query string
    """
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    regions_str = "', '".join(regions)
    
    return f'''
        WITH current_forecast AS (
            SELECT
                fr.region_code,
                fr.date_forecast,
                strftime(fr.date_forecast, '%A') AS day_name,
                COUNT(DISTINCT fr.store_no) AS active_stores,
                COUNT(DISTINCT fr.item_no) AS item_count,
                SUM(fr.forecast_quantity) AS forecast_shipped,
                SUM(fr.forecast_average) AS forecast_sales,
                SUM(fr.w1_shipped) AS lw_shipped,
                SUM(fr.w1_sold) AS lw_sold
            FROM forecast_results fr
            WHERE fr.region_code IN ('{regions_str}')
            AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
            {inactive_stores_filter}
            {inactive_store_items_filter}
            GROUP BY fr.region_code, fr.date_forecast
        ),
        -- Get last week's total stores that shipped (even if not in current forecast)
        last_week_stores AS (
            SELECT
                s.region_code,
                (s.date_posting + INTERVAL '7 DAY')::DATE AS forecast_date,
                COUNT(DISTINCT s.store_no) AS lw_total_stores
            FROM main.shrink s
            WHERE s.region_code IN ('{regions_str}')
            AND s.date_posting BETWEEN ('{start_date}'::DATE - INTERVAL '14 DAY') AND ('{end_date}'::DATE - INTERVAL '7 DAY')
            AND s.quantity_received > 0
            GROUP BY s.region_code, s.date_posting
        )
        SELECT
            cf.region_code,
            cf.date_forecast,
            cf.day_name,
            cf.active_stores,
            COALESCE(lws.lw_total_stores, cf.active_stores) AS lw_stores,
            cf.item_count,
            cf.forecast_shipped,
            cf.forecast_sales,
            cf.lw_shipped,
            cf.lw_sold,
            -- Delta calculations
            CASE 
                WHEN cf.lw_shipped > 0 
                THEN ROUND((cf.forecast_shipped - cf.lw_shipped)::DOUBLE / cf.lw_shipped, 4)
                ELSE 0 
            END AS delta_shipped_pct,
            CASE 
                WHEN cf.lw_sold > 0 
                THEN ROUND((cf.forecast_sales - cf.lw_sold)::DOUBLE / cf.lw_sold, 4)
                ELSE 0 
            END AS delta_sales_pct,
            -- Expected shrink
            CASE 
                WHEN cf.forecast_shipped > 0 
                THEN ROUND((cf.forecast_shipped - cf.forecast_sales)::DOUBLE / cf.forecast_shipped, 4)
                ELSE 0 
            END AS expected_shrink_pct,
            -- Last week shrink
            CASE 
                WHEN cf.lw_shipped > 0 
                THEN ROUND((cf.lw_shipped - cf.lw_sold)::DOUBLE / cf.lw_shipped, 4)
                ELSE 0 
            END AS lw_shrink_pct
        FROM current_forecast cf
        LEFT JOIN last_week_stores lws 
            ON cf.region_code = lws.region_code 
            AND cf.date_forecast = lws.forecast_date
        ORDER BY cf.region_code, cf.date_forecast
    '''


def get_waterfall_components_query(regions: list, start_date: str, end_date: str) -> str:
    """
    Generate query for waterfall analysis components from the waterfall_aggregate table.
    
    The waterfall_aggregate table contains pre-calculated metrics from forecast_results,
    eliminating duplicate calculations and ensuring consistency.
    
    Shows the adjustments from last week SALES (sold) to final forecast:
    - Starting point: Last week sold (LW Sales)
    - EMA Uplift (items where LW < trend)
    - Base cover (default + sold-out)
    - Rounding (up and down separately)
    - Store pass (growth and decline separately)
    - Weather adjustment
    - Final forecast
    
    Args:
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        
    Returns:
        SQL query string
    """
    regions_str = "', '".join(regions)
    
    return f'''
        SELECT
            wa.region_code,
            wa.date_forecast,
            wa.day_name,
            
            -- Counts
            wa.store_count,
            wa.item_count,
            wa.line_count,
            
            -- Starting Point
            wa.lw_shipped,
            wa.lw_sold,
            
            -- Baseline Source Breakdown
            wa.baseline_lw_sales_qty,
            wa.baseline_lw_sales_count,
            wa.baseline_ema_qty,
            wa.baseline_ema_count,
            wa.baseline_avg_qty,
            wa.baseline_avg_count,
            wa.baseline_min_case_qty,
            wa.baseline_min_case_count,
            
            -- EMA Uplift
            wa.ema_uplift_qty,
            wa.ema_uplift_count,
            
            -- Decline Adjustment (WoW decline pattern)
            wa.decline_adj_qty,
            wa.decline_adj_count,
            
            -- High Shrink Adjustment (consecutive high shrink)
            wa.high_shrink_adj_qty,
            wa.high_shrink_adj_count,
            
            -- Base Cover
            wa.base_cover_total_qty,
            wa.base_cover_default_qty,
            wa.base_cover_default_count,
            wa.base_cover_soldout_qty,
            wa.base_cover_soldout_count,
            
            -- Rounding (separated)
            wa.rounding_up_qty,
            wa.rounding_up_count,
            wa.rounding_down_qty,
            wa.rounding_down_count,
            wa.rounding_net_qty,
            
            -- Safety Stock
            wa.safety_stock_qty,
            wa.safety_stock_count,
            
            -- Store Pass (separated)
            wa.store_pass_decline_qty,
            wa.store_pass_decline_count,
            wa.store_pass_growth_qty,
            wa.store_pass_growth_count,
            wa.store_pass_net_qty,
            wa.store_pass_stores_adjusted,
            
            -- Weather
            wa.weather_adj_qty,
            wa.weather_adj_count,
            
            -- Intermediate totals for verification
            wa.total_forecast_avg,
            wa.total_with_cover,
            wa.pre_store_pass_qty,
            wa.pre_weather_qty,
            
            -- Final
            wa.final_forecast_qty,
            
            -- Delta metrics
            wa.delta_from_lw_sales,
            wa.delta_from_lw_sales_pct,
            wa.delta_from_lw_shipped,
            wa.delta_from_lw_shipped_pct
            
        FROM waterfall_aggregate wa
        WHERE wa.region_code IN ('{regions_str}')
        AND wa.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY wa.region_code, wa.date_forecast
    '''


def get_weather_summary_query(regions: list, start_date: str, end_date: str) -> str:
    """
    Generate query for weather adjustment summary.
    
    Shows by region:
    - Store counts by severity category
    - Total and average weather severity
    - Quantity adjusted due to weather
    
    Args:
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        
    Returns:
        SQL query string
    """
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    regions_str = "', '".join(regions)
    
    return f'''
        SELECT
            fr.region_code,
            fr.date_forecast,
            strftime(fr.date_forecast, '%A') AS day_name,
            
            -- Store counts by severity
            COUNT(DISTINCT CASE 
                WHEN fr.weather_severity_category = 'SEVERE' THEN fr.store_no 
            END) AS severe_stores,
            COUNT(DISTINCT CASE 
                WHEN fr.weather_severity_category = 'HIGH' THEN fr.store_no 
            END) AS high_stores,
            COUNT(DISTINCT CASE 
                WHEN fr.weather_severity_category = 'MODERATE' THEN fr.store_no 
            END) AS moderate_stores,
            COUNT(DISTINCT CASE 
                WHEN fr.weather_severity_category = 'LOW' THEN fr.store_no 
            END) AS low_stores,
            COUNT(DISTINCT CASE 
                WHEN fr.weather_severity_category = 'MINIMAL' OR fr.weather_severity_category IS NULL THEN fr.store_no 
            END) AS minimal_stores,
            
            -- Total stores
            COUNT(DISTINCT fr.store_no) AS total_stores,
            
            -- Severity metrics
            ROUND(AVG(COALESCE(fr.weather_severity_score, 0)), 2) AS avg_severity_score,
            MAX(fr.weather_severity_score) AS max_severity_score,
            
            -- Item level weather adjustments
            SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END) AS items_adjusted,
            COUNT(*) AS total_items,
            
            -- Quantity adjustments
            SUM(COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity)) AS pre_weather_qty,
            SUM(fr.forecast_quantity) AS post_weather_qty,
            SUM(COALESCE(fr.weather_adjustment_qty, 0)) AS weather_adj_qty,
            
            -- Percentage reduction
            CASE 
                WHEN SUM(COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity)) > 0 
                THEN ROUND(
                    ABS(SUM(COALESCE(fr.weather_adjustment_qty, 0)))::DOUBLE / 
                    SUM(COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity)), 4
                )
                ELSE 0 
            END AS weather_reduction_pct,
            
            -- Common weather conditions
            MODE(fr.weather_day_condition) AS primary_condition
            
        FROM forecast_results fr
        WHERE fr.region_code IN ('{regions_str}')
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        GROUP BY fr.region_code, fr.date_forecast
        ORDER BY fr.region_code, fr.date_forecast
    '''


def get_inactive_stores_summary_query(regions: list, start_date: str, end_date: str) -> str:
    """
    Generate query for inactive store summary.
    
    Identifies stores that were active last week but are not in current forecast.
    
    Args:
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        
    Returns:
        SQL query string
    """
    regions_str = "', '".join(regions)
    
    # Build inactive stores exclusion list
    inactive_stores_list = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_list = f"AND store_no IN ({inactive_stores_str})"
    
    return f'''
        WITH last_week_active AS (
            SELECT DISTINCT
                s.region_code,
                s.store_no,
                s.store_name,
                (s.date_posting + INTERVAL '7 DAY')::DATE AS forecast_date,
                SUM(s.quantity_received) AS lw_shipped,
                SUM(s.quantity_sold) AS lw_sold
            FROM main.shrink s
            WHERE s.region_code IN ('{regions_str}')
            AND s.date_posting BETWEEN ('{start_date}'::DATE - INTERVAL '14 DAY') AND ('{end_date}'::DATE - INTERVAL '7 DAY')
            {inactive_stores_list}
            GROUP BY s.region_code, s.store_no, s.store_name, s.date_posting
        )
        SELECT
            lwa.region_code,
            lwa.forecast_date,
            lwa.store_no,
            lwa.store_name,
            lwa.lw_shipped,
            lwa.lw_sold
        FROM last_week_active lwa
        WHERE lwa.lw_shipped > 0
        ORDER BY lwa.region_code, lwa.forecast_date, lwa.store_no
    '''


def get_all_regions_total_query(regions: list, start_date: str, end_date: str) -> str:
    """
    Generate query for all regions total summary.
    
    Provides company-wide totals across all regions.
    
    Args:
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        
    Returns:
        SQL query string
    """
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    regions_str = "', '".join(regions)
    
    return f'''
        SELECT
            fr.date_forecast,
            strftime(fr.date_forecast, '%A') AS day_name,
            COUNT(DISTINCT fr.region_code) AS region_count,
            COUNT(DISTINCT fr.store_no) AS total_stores,
            COUNT(DISTINCT fr.item_no) AS total_items,
            SUM(fr.forecast_quantity) AS total_forecast_shipped,
            SUM(fr.forecast_average) AS total_forecast_sales,
            SUM(fr.w1_shipped) AS total_lw_shipped,
            SUM(fr.w1_sold) AS total_lw_sold,
            SUM(COALESCE(fr.weather_adjustment_qty, 0)) AS total_weather_adj,
            SUM(COALESCE(fr.store_level_adjustment_qty, 0)) AS total_store_adj,
            -- Percentages
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS delta_from_lw_pct,
            CASE 
                WHEN SUM(fr.forecast_quantity) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.forecast_average))::DOUBLE / SUM(fr.forecast_quantity), 4)
                ELSE 0 
            END AS expected_shrink_pct
        FROM forecast_results fr
        WHERE fr.region_code IN ('{regions_str}')
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        GROUP BY fr.date_forecast
        ORDER BY fr.date_forecast
    '''
