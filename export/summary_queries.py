"""
Summary Queries Module
======================
SQL queries for regional summary reports.

This module contains all SQL queries used for generating
regional summary Excel reports.
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
    inactive_store_items = getattr(settings, 'INACTIVE_STORE_ITEMS', None)
    if inactive_store_items:
        conditions = [
            f"({alias}.store_no = {store_no} AND {alias}.item_no = {item_no})"
            for store_no, item_no in inactive_store_items
        ]
        return f"AND NOT ({' OR '.join(conditions)})"
    return ""


def _get_store_name_subquery(region: str) -> str:
    """Build store name lookup subquery."""
    return f'''
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


def get_daily_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for daily summary metrics with trends and expected shrink."""
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    
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
            
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS delta_from_lw_pct
            
        FROM forecast_results fr
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        GROUP BY fr.date_forecast
        ORDER BY fr.date_forecast
    '''


def get_store_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for store-level summary BY DATE with weather indicators."""
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    store_name_subquery = _get_store_name_subquery(region)
    
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
            
            -- Shipped Trend
            SUM(fr.w4_shipped) AS w4_shipped_total,
            SUM(fr.w3_shipped) AS w3_shipped_total,
            SUM(fr.w2_shipped) AS w2_shipped_total,
            SUM(fr.w1_shipped) AS w1_shipped_total,
            
            -- Sold Qty Trend
            SUM(fr.w4_sold) AS w4_sold_total,
            SUM(fr.w3_sold) AS w3_sold_total,
            SUM(fr.w2_sold) AS w2_sold_total,
            SUM(fr.w1_sold) AS w1_sold_total,
            
            -- Growth Metrics
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

            -- Expected Shrink
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
            
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.w1_shipped) - SUM(fr.w1_sold))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS lw_shrink_pct,
            
            -- Weather metrics
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
        LEFT JOIN ({store_name_subquery}) nm ON fr.store_no = nm.store_no
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        GROUP BY fr.date_forecast, fr.store_no, nm.store_name
        ORDER BY fr.date_forecast, fr.store_no
    '''


def get_item_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for item-level summary BY DATE."""
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    
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
            
            -- Shipped Trend
            SUM(fr.w4_shipped) AS w4_shipped_total,
            SUM(fr.w3_shipped) AS w3_shipped_total,
            SUM(fr.w2_shipped) AS w2_shipped_total,
            SUM(fr.w1_shipped) AS w1_shipped_total,
            
            -- Sold Qty Trend
            SUM(fr.w4_sold) AS w4_sold_total,
            SUM(fr.w3_sold) AS w3_sold_total,
            SUM(fr.w2_sold) AS w2_sold_total,
            SUM(fr.w1_sold) AS w1_sold_total,
            
            -- Growth Metrics
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

            -- Expected Shrink
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
            
            CASE 
                WHEN SUM(fr.w1_shipped) > 0 
                THEN ROUND((SUM(fr.forecast_quantity) - SUM(fr.w1_shipped))::DOUBLE / SUM(fr.w1_shipped), 4)
                ELSE 0 
            END AS delta_from_lw_pct
            
        FROM forecast_results fr
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        GROUP BY fr.date_forecast, fr.item_no, fr.item_desc
        ORDER BY fr.date_forecast, fr.item_no
    '''


def get_item_detail_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for full item/store level detail."""
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    store_name_subquery = _get_store_name_subquery(region)
    
    return f'''
        SELECT
            fr.date_forecast AS "Forecast Date",
            strftime(fr.date_forecast, '%A') AS "Day",
            fr.store_no AS "Store #",
            nm.store_name AS "Store Name",
            fr.item_no AS "Item #",
            fr.item_desc AS "Item Description",
            
            fr.case_pack_size AS "Case Pack",
            
            -- Forecast quantities
            COALESCE(fr.forecast_qty_pre_store_pass, fr.forecast_quantity) AS "Fcst Pre-Store Adj",
            COALESCE(fr.store_level_adjustment_qty, 0) AS "Store Adj Qty",
            COALESCE(fr.forecast_qty_pre_weather, fr.forecast_quantity) AS "Fcst Pre-Weather",
            fr.forecast_quantity AS "Fcst Final",
            fr.forecast_quantity / NULLIF(fr.case_pack_size, 0) AS "Fcst Cases",
            COALESCE(fr.weather_adjustment_qty, 0) AS "Weather Adj",
            
            fr.forecast_average AS "Fcst Avg (Exp Sales)",
            
            -- Shipped Trend
            fr.w4_shipped AS "W4 Ship",
            fr.w3_shipped AS "W3 Ship",
            fr.w2_shipped AS "W2 Ship",
            fr.w1_shipped AS "W1 Ship",
            
            -- Sold Qty Trend
            fr.w4_sold AS "W4 Sold",
            fr.w3_sold AS "W3 Sold",
            fr.w2_sold AS "W2 Sold",
            fr.w1_sold AS "W1 Sold",
            
            -- Growth Metrics
            ROUND(CASE WHEN fr.w1_sold > 0 
                THEN (fr.forecast_average - fr.w1_sold)::DOUBLE / fr.w1_sold 
                ELSE 0 END * 100, 1) AS "Growth vs W1 %",
            
            ROUND(CASE WHEN fr.w2_sold > 0 
                THEN (fr.forecast_average - fr.w2_sold)::DOUBLE / fr.w2_sold 
                ELSE 0 END * 100, 1) AS "Growth vs W2 %",

            -- Expected Shrink
            ROUND(CASE WHEN fr.forecast_quantity > 0 
                THEN (fr.forecast_quantity - fr.forecast_average)::DOUBLE / fr.forecast_quantity 
                ELSE 0 END * 100, 1) AS "Exp Shrink (Avg) %",
            ROUND(CASE WHEN fr.forecast_quantity > 0 
                THEN (fr.forecast_quantity - fr.w1_sold)::DOUBLE / fr.forecast_quantity 
                ELSE 0 END * 100, 1) AS "Exp Shrink (LW) %",
            ROUND(CASE WHEN fr.forecast_quantity > 0 
                THEN (fr.forecast_quantity - fr.w2_sold)::DOUBLE / fr.forecast_quantity 
                ELSE 0 END * 100, 1) AS "Exp Shrink (2W) %",
            
            ROUND(fr.w1_shrink_p * 100, 1) AS "W1 Shrink %",
            
            -- Weather metrics
            ROUND(COALESCE(fr.weather_severity_score, 0), 1) AS "Weather Severity",
            COALESCE(fr.weather_severity_category, 'MINIMAL') AS "Severity Category",
            COALESCE(fr.weather_day_condition, '-') AS "Weather Condition",
            COALESCE(fr.weather_status_indicator, '-') AS "Weather Indicator",
            
            fr.delta_from_last_week AS "Delta from LW",
            ROUND(CASE WHEN fr.w1_shipped > 0 
                THEN (fr.forecast_quantity - fr.w1_shipped)::DOUBLE / fr.w1_shipped 
                ELSE 0 END * 100, 1) AS "Delta LW %",
            
            fr.base_cover_applied AS "Cover Applied"
            
        FROM forecast_results fr
        LEFT JOIN ({store_name_subquery}) nm ON fr.store_no = nm.store_no
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        ORDER BY fr.date_forecast, fr.store_no, fr.item_no
    '''


def get_weather_impact_summary_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for weather impact summary by store and date."""
    store_name_subquery = _get_store_name_subquery(region)
    
    return f'''
        SELECT
            w.date AS "Date",
            strftime(w.date, '%A') AS "Day",
            w.store_no AS "Store #",
            nm.store_name AS "Store Name",
            
            w.day_condition AS "Conditions",
            w.day_description AS "Description",
            
            ROUND(w.temp_min, 1) AS "Temp Min (F)",
            ROUND(w.temp_max, 1) AS "Temp Max (F)",
            ROUND(w.feels_like_min, 1) AS "Feels Like Min",
            ROUND(w.feels_like_max, 1) AS "Feels Like Max",
            
            ROUND(w.total_rain_expected, 3) AS "Precip (in)",
            ROUND(w.precip_probability, 0) AS "Precip Prob %",
            ROUND(w.precip_cover, 0) AS "Precip Cover %",
            w.precip_type AS "Precip Type",
            
            ROUND(w.snow_amount, 1) AS "Snow (in)",
            ROUND(w.snow_depth, 1) AS "Snow Depth (in)",
            
            ROUND(w.wind_speed, 1) AS "Wind (mph)",
            ROUND(w.wind_gust, 1) AS "Wind Gust (mph)",
            
            ROUND(w.visibility, 1) AS "Visibility (mi)",
            ROUND(w.cloud_cover, 0) AS "Cloud Cover %",
            
            ROUND(w.severe_risk, 0) AS "Severe Risk",
            
            ROUND(w.rain_severity, 2) AS "Rain Sev",
            ROUND(w.snow_severity, 2) AS "Snow Sev",
            ROUND(w.wind_severity, 2) AS "Wind Sev",
            ROUND(w.temp_severity, 2) AS "Temp Sev",
            ROUND(w.visibility_severity, 2) AS "Vis Sev",
            ROUND(w.condition_severity, 2) AS "Cond Sev",
            
            ROUND(w.severity_score, 2) AS "Severity Score",
            w.severity_category AS "Severity Category",
            ROUND(w.sales_impact_factor, 3) AS "Sales Impact Factor",
            
            COALESCE(fr.total_weather_adj, 0) AS "Total Weather Adj",
            COALESCE(fr.items_adjusted, 0) AS "Items Adjusted"
            
        FROM weather w
        LEFT JOIN ({store_name_subquery}) nm ON w.store_no = nm.store_no
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
    """Generate query for weather summary aggregated by date from forecast_results."""
    inactive_stores_filter = _get_inactive_stores_filter("forecast_results")
    inactive_store_items_filter = _get_inactive_store_items_filter("forecast_results")
    
    return f'''
        SELECT
            date_forecast AS "Date",
            strftime(date_forecast, '%A') AS "Day",
            COUNT(DISTINCT store_no) AS "Store Count",
            
            -- Severity category distribution
            COUNT(DISTINCT CASE WHEN weather_severity_category = 'SEVERE' THEN store_no END) AS "Severe",
            COUNT(DISTINCT CASE WHEN weather_severity_category = 'HIGH' THEN store_no END) AS "High",
            COUNT(DISTINCT CASE WHEN weather_severity_category = 'MODERATE' THEN store_no END) AS "Moderate",
            COUNT(DISTINCT CASE WHEN weather_severity_category = 'LOW' THEN store_no END) AS "Low",
            COUNT(DISTINCT CASE WHEN weather_severity_category = 'MINIMAL' OR weather_severity_category IS NULL THEN store_no END) AS "Minimal",
            
            -- Severity scores
            ROUND(AVG(COALESCE(weather_severity_score, 0)), 2) AS "Avg Severity",
            ROUND(MAX(COALESCE(weather_severity_score, 0)), 2) AS "Max Severity",
            ROUND(AVG(COALESCE(weather_sales_impact_factor, 1.0)), 3) AS "Avg Impact Factor",
            ROUND(MIN(COALESCE(weather_sales_impact_factor, 1.0)), 3) AS "Min Impact Factor",
            
            -- Temperatures
            ROUND(AVG(weather_temp_min), 1) AS "Avg Temp Min",
            ROUND(AVG(weather_temp_max), 1) AS "Avg Temp Max",
            ROUND(MIN(weather_temp_min), 1) AS "Coldest Temp",
            ROUND(MAX(weather_temp_max), 1) AS "Warmest Temp",
            
            -- Precipitation counts with actual precip probability
            COUNT(DISTINCT CASE WHEN COALESCE(weather_precip_probability, 0) > 50 THEN store_no END) AS "Stores w/ Rain Likely",
            COUNT(DISTINCT CASE WHEN COALESCE(weather_total_rain_expected, 0) > 0.1 THEN store_no END) AS "Stores w/ Rain",
            COUNT(DISTINCT CASE WHEN COALESCE(weather_snow_amount, 0) > 0 THEN store_no END) AS "Stores w/ Snow",
            COUNT(DISTINCT CASE WHEN COALESCE(weather_snow_depth, 0) > 2 THEN store_no END) AS "Stores w/ Snow Depth > 2in",
            
            -- Average metrics for stores with precipitation
            ROUND(AVG(CASE WHEN COALESCE(weather_total_rain_expected, 0) > 0 THEN weather_total_rain_expected END), 2) AS "Avg Rain",
            ROUND(AVG(CASE WHEN COALESCE(weather_snow_amount, 0) > 0 THEN weather_snow_amount END), 1) AS "Avg Snow",
            ROUND(AVG(CASE WHEN COALESCE(weather_snow_depth, 0) > 0 THEN weather_snow_depth END), 1) AS "Avg Snow Depth",
            ROUND(AVG(COALESCE(weather_wind_speed, 0)), 1) AS "Avg Wind",
            ROUND(MAX(COALESCE(weather_wind_gust, 0)), 1) AS "Max Wind Gust",
            
            -- Adjustments
            COALESCE(SUM(COALESCE(weather_adjustment_qty, 0)), 0) AS "Total Qty Adj",
            COALESCE(SUM(CASE WHEN weather_adjusted = 1 THEN 1 ELSE 0 END), 0) AS "Total Items Adj"
            
        FROM forecast_results
        WHERE region_code = '{region}'
        AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        GROUP BY date_forecast
        ORDER BY date_forecast
    '''


def get_weather_store_detail_query(region: str, start_date: str, end_date: str) -> str:
    """Generate query for detailed store-level weather data, ranked by severity."""
    inactive_stores_filter = _get_inactive_stores_filter("fr")
    inactive_store_items_filter = _get_inactive_store_items_filter("fr")
    store_name_subquery = _get_store_name_subquery(region)
    
    return f'''
        SELECT
            fr.date_forecast AS "Date",
            strftime(fr.date_forecast, '%A') AS "Day",
            fr.store_no AS "Store #",
            COALESCE(nm.store_name, 'Store ' || fr.store_no) AS "Store Name",
            fr.weather_day_condition AS "Conditions",
            ROUND(fr.weather_temp_min, 1) AS "Temp Min",
            ROUND(fr.weather_temp_max, 1) AS "Temp Max",
            -- Precipitation details
            ROUND(COALESCE(fr.weather_total_rain_expected, 0), 2) AS "Precip (in)",
            ROUND(COALESCE(fr.weather_precip_probability, 0), 0) AS "Precip %",
            ROUND(COALESCE(fr.weather_precip_cover, 0), 0) AS "Precip Cover %",
            -- Snow details
            ROUND(COALESCE(fr.weather_snow_amount, 0), 1) AS "Snow (in)",
            ROUND(COALESCE(fr.weather_snow_depth, 0), 1) AS "Snow Depth",
            -- Wind details
            ROUND(COALESCE(fr.weather_wind_speed, 0), 1) AS "Wind (mph)",
            ROUND(COALESCE(fr.weather_wind_gust, 0), 1) AS "Wind Gust",
            -- Visibility and atmosphere
            ROUND(COALESCE(fr.weather_visibility, 10), 1) AS "Visibility",
            ROUND(COALESCE(fr.weather_humidity, 0), 0) AS "Humidity %",
            ROUND(COALESCE(fr.weather_cloud_cover, 0), 0) AS "Cloud Cover %",
            ROUND(COALESCE(fr.weather_severe_risk, 0), 0) AS "Severe Risk",
            -- Component severity scores
            ROUND(COALESCE(fr.weather_rain_severity, 0), 2) AS "Rain Sev",
            ROUND(COALESCE(fr.weather_snow_severity, 0), 2) AS "Snow Sev",
            ROUND(COALESCE(fr.weather_wind_severity, 0), 2) AS "Wind Sev",
            ROUND(COALESCE(fr.weather_visibility_severity, 0), 2) AS "Vis Sev",
            ROUND(COALESCE(fr.weather_temp_severity, 0), 2) AS "Temp Sev",
            -- Final severity metrics
            ROUND(COALESCE(fr.weather_severity_score, 0), 2) AS "Severity Score",
            COALESCE(fr.weather_severity_category, 'MINIMAL') AS "Category",
            ROUND(COALESCE(fr.weather_sales_impact_factor, 1.0), 3) AS "Impact Factor",
            -- Adjustments
            COALESCE(SUM(fr.weather_adjustment_qty), 0) AS "Qty Adjusted",
            COALESCE(SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END), 0) AS "Items Adj"
        FROM forecast_results fr
        LEFT JOIN ({store_name_subquery}) nm ON fr.store_no = nm.store_no
        WHERE fr.region_code = '{region}'
        AND fr.date_forecast BETWEEN '{start_date}' AND '{end_date}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        GROUP BY 
            fr.date_forecast, fr.store_no, nm.store_name,
            fr.weather_day_condition, fr.weather_temp_min, fr.weather_temp_max,
            fr.weather_total_rain_expected, fr.weather_precip_probability, fr.weather_precip_cover,
            fr.weather_snow_amount, fr.weather_snow_depth,
            fr.weather_wind_speed, fr.weather_wind_gust, fr.weather_visibility,
            fr.weather_humidity, fr.weather_cloud_cover, fr.weather_severe_risk,
            fr.weather_rain_severity, fr.weather_snow_severity, fr.weather_wind_severity,
            fr.weather_visibility_severity, fr.weather_temp_severity,
            fr.weather_severity_score, fr.weather_severity_category,
            fr.weather_sales_impact_factor
        ORDER BY 
            COALESCE(fr.weather_severity_score, 0) DESC,
            fr.date_forecast,
            fr.store_no
    '''
