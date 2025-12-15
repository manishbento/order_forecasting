"""
Regional Level AI Export
========================
Region-level forecast analysis for AI consumption.

Provides per-region:
- Daily breakdown with waterfall components
- Store-level aggregates (which stores driving changes)
- Item-level aggregates (which items driving changes)
- Key drivers and anomalies

Uses compact format with IDs only (no names).
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any

from config import settings


def get_regional_summary_data(conn, region: str,
                               start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Query and build regional summary data for AI analysis.
    
    Args:
        conn: DuckDB connection
        region: Region code
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        Dictionary with regional analysis data
    """
    # Daily breakdown for this region
    daily_query = f"""
    SELECT
        date_forecast as dt,
        strftime(date_forecast, '%a') as dow,
        store_count as stores,
        item_count as items,
        line_count as lines,
        
        -- Base metrics
        lw_shipped as lw_ship,
        lw_sold as lw_sold,
        final_forecast_qty as fcst,
        delta_from_lw_shipped as delta_ship,
        ROUND(delta_from_lw_shipped_pct * 100, 2) as delta_ship_pct,
        
        -- Waterfall components (waterfall starts from lw_sold)
        baseline_uplift_qty as baseline_up,
        baseline_uplift_count as baseline_up_n,
        ema_uplift_qty as ema_up,
        ema_uplift_count as ema_up_n,
        decline_adj_qty as decline,
        decline_adj_count as decline_n,
        high_shrink_adj_qty as shrink_adj,
        high_shrink_adj_count as shrink_adj_n,
        base_cover_total_qty as cover,
        base_cover_soldout_qty as cover_so,
        base_cover_soldout_count as cover_so_n,
        rounding_net_qty as round_net,
        rounding_up_qty as round_up,
        rounding_down_qty as round_dn,
        safety_stock_qty as safety,
        store_pass_net_qty as store_pass,
        store_pass_growth_qty as store_grow,
        store_pass_decline_qty as store_decline,
        store_pass_stores_adjusted as stores_adj,
        weather_adj_qty as weather,
        weather_adj_count as weather_n
        
    FROM waterfall_aggregate
    WHERE region_code = '{region}'
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date_forecast
    """
    
    daily = conn.execute(daily_query).fetchdf()
    daily['dt'] = daily['dt'].astype(str)
    daily = daily.to_dict('records')
    
    # Top stores by forecast volume (aggregate across dates)
    top_stores_query = f"""
    SELECT
        store_no as sid,
        COUNT(*) as n_items,
        SUM(w1_shipped) as lw_ship,
        SUM(w1_sold) as lw_sold,
        SUM(forecast_quantity) as fcst,
        SUM(forecast_quantity) - SUM(w1_shipped) as delta,
        ROUND(100.0 * (SUM(forecast_quantity) - SUM(w1_shipped)) / NULLIF(SUM(w1_shipped), 0), 1) as delta_pct,
        
        -- Key drivers for this store
        SUM(COALESCE(ema_uplift_qty, 0)) as ema_up,
        SUM(COALESCE(base_cover_qty, 0)) as cover,
        SUM(COALESCE(rounding_net_qty, 0)) as round_net,
        SUM(COALESCE(store_level_growth_qty, 0) + COALESCE(store_level_decline_qty, 0)) as store_adj,
        SUM(COALESCE(weather_adjustment_qty, 0)) as weather,
        
        -- Store-level shrink context
        ROUND(AVG(store_w1_shrink_p) * 100, 1) as avg_shrink_pct
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_no
    ORDER BY SUM(forecast_quantity) DESC
    LIMIT 20
    """
    
    top_stores = conn.execute(top_stores_query).fetchdf().to_dict('records')
    
    # Top items by forecast volume
    top_items_query = f"""
    SELECT
        item_no as iid,
        COUNT(DISTINCT store_no) as n_stores,
        SUM(w1_shipped) as lw_ship,
        SUM(w1_sold) as lw_sold,
        SUM(forecast_quantity) as fcst,
        SUM(forecast_quantity) - SUM(w1_shipped) as delta,
        ROUND(100.0 * (SUM(forecast_quantity) - SUM(w1_shipped)) / NULLIF(SUM(w1_shipped), 0), 1) as delta_pct,
        
        -- Baseline source distribution
        SUM(CASE WHEN baseline_source = 'lw_sales' THEN 1 ELSE 0 END) as bl_lw_n,
        SUM(CASE WHEN baseline_source = 'ema' THEN 1 ELSE 0 END) as bl_ema_n,
        
        -- Key drivers
        SUM(COALESCE(ema_uplift_qty, 0)) as ema_up,
        SUM(COALESCE(base_cover_qty, 0)) as cover,
        SUM(COALESCE(rounding_net_qty, 0)) as round_net
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY item_no
    ORDER BY SUM(forecast_quantity) DESC
    LIMIT 15
    """
    
    top_items = conn.execute(top_items_query).fetchdf().to_dict('records')
    
    # Stores with high adjustments (potential anomalies)
    anomaly_stores_query = f"""
    SELECT
        store_no as sid,
        date_forecast as dt,
        COUNT(*) as n_items,
        SUM(w1_shipped) as lw_ship,
        SUM(forecast_quantity) as fcst,
        ROUND(100.0 * (SUM(forecast_quantity) - SUM(w1_shipped)) / NULLIF(SUM(w1_shipped), 0), 1) as delta_pct,
        SUM(COALESCE(store_level_growth_qty, 0) + COALESCE(store_level_decline_qty, 0)) as store_adj,
        SUM(COALESCE(weather_adjustment_qty, 0)) as weather
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_no, date_forecast
    HAVING ABS(ROUND(100.0 * (SUM(forecast_quantity) - SUM(w1_shipped)) / NULLIF(SUM(w1_shipped), 0), 1)) > 20
    ORDER BY ABS(SUM(forecast_quantity) - SUM(w1_shipped)) DESC
    LIMIT 15
    """
    
    anomaly_stores = conn.execute(anomaly_stores_query).fetchdf()
    if not anomaly_stores.empty:
        anomaly_stores['dt'] = anomaly_stores['dt'].astype(str)
    anomaly_stores = anomaly_stores.to_dict('records')
    
    # Weather impact summary
    weather_query = f"""
    SELECT
        date_forecast as dt,
        COUNT(DISTINCT store_no) as stores_impacted,
        SUM(CASE WHEN weather_adjusted = 1 THEN 1 ELSE 0 END) as items_adj,
        SUM(COALESCE(weather_adjustment_qty, 0)) as weather_total,
        ROUND(AVG(CASE WHEN weather_severity_score > 0 THEN weather_severity_score END), 2) as avg_severity
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    AND weather_adjusted = 1
    GROUP BY date_forecast
    ORDER BY date_forecast
    """
    
    weather = conn.execute(weather_query).fetchdf()
    if not weather.empty:
        weather['dt'] = weather['dt'].astype(str)
    weather = weather.to_dict('records')
    
    # Overall region summary
    summary_query = f"""
    SELECT
        '{region}' as rgn,
        COUNT(DISTINCT date_forecast) as n_days,
        SUM(store_count) as store_days,
        SUM(item_count) as item_days,
        SUM(line_count) as lines,
        SUM(lw_shipped) as lw_ship,
        SUM(lw_sold) as lw_sold,
        SUM(final_forecast_qty) as fcst,
        SUM(final_forecast_qty) - SUM(lw_shipped) as delta,
        ROUND(100.0 * (SUM(final_forecast_qty) - SUM(lw_shipped)) / NULLIF(SUM(lw_shipped), 0), 2) as delta_pct
        
    FROM waterfall_aggregate
    WHERE region_code = '{region}'
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    """
    
    summary = conn.execute(summary_query).fetchdf().to_dict('records')[0]
    
    return {
        "meta": {
            "type": "regional",
            "region": region,
            "period": {"start": start_date, "end": end_date},
            "generated": datetime.now().isoformat()
        },
        "summary": summary,
        "daily": daily,
        "top_stores": top_stores,
        "top_items": top_items,
        "anomalies": anomaly_stores,
        "weather": weather
    }


def export_regional_ai(conn, regions: List[str],
                       start_date: str, end_date: str,
                       output_dir: str = None) -> List[str]:
    """
    Export regional-level AI analysis data for each region.
    
    Creates one compact JSON file per region with:
    - Daily waterfall breakdown
    - Top stores and items
    - Anomaly detection
    - Weather impact summary
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Output directory
        
    Returns:
        List of paths to exported files
    """
    if output_dir is None:
        output_dir = os.path.join(settings.OUTPUT_DIR, 'ai_analysis')
    os.makedirs(output_dir, exist_ok=True)
    
    filepaths = []
    for region in regions:
        data = get_regional_summary_data(conn, region, start_date, end_date)
        
        filename = f"ai_regional_{region}_{start_date}_{end_date}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, separators=(',', ':'), default=str)
        
        print(f"AI Regional export ({region}): {filepath}")
        filepaths.append(filepath)
    
    return filepaths
