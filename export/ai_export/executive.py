"""
Executive Level AI Export
=========================
High-level forecast summary for executive-level AI analysis.

Provides:
- Overall forecast metrics across all regions
- Waterfall component analysis (what's driving forecast changes)
- Trend analysis (WoW changes)
- Key insights for AI to interpret

Uses compact JSON format with abbreviated keys to minimize tokens.
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any

from config import settings


def get_executive_summary_data(conn, regions: List[str], 
                                start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Query and build executive summary data structure.
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        Dictionary with executive summary data
    """
    regions_str = "', '".join(regions)
    
    # Overall summary across all regions/dates
    overall_query = f"""
    SELECT
        COUNT(DISTINCT region_code) as n_regions,
        COUNT(DISTINCT date_forecast) as n_days,
        SUM(store_count) as total_store_days,
        SUM(item_count) as total_item_days,
        SUM(line_count) as total_lines,
        
        -- Totals
        SUM(lw_shipped) as lw_ship,
        SUM(lw_sold) as lw_sold,
        SUM(final_forecast_qty) as fcst,
        
        -- Delta from LW
        SUM(final_forecast_qty) - SUM(lw_shipped) as delta_ship,
        SUM(final_forecast_qty) - SUM(lw_sold) as delta_sold,
        ROUND(100.0 * (SUM(final_forecast_qty) - SUM(lw_shipped)) / NULLIF(SUM(lw_shipped), 0), 2) as delta_ship_pct,
        ROUND(100.0 * (SUM(final_forecast_qty) - SUM(lw_sold)) / NULLIF(SUM(lw_sold), 0), 2) as delta_sold_pct,
        
        -- Waterfall Components (aggregated)
        SUM(ema_uplift_qty) as ema_up,
        SUM(ema_uplift_count) as ema_up_n,
        SUM(decline_adj_qty) as decline_adj,
        SUM(decline_adj_count) as decline_adj_n,
        SUM(high_shrink_adj_qty) as shrink_adj,
        SUM(high_shrink_adj_count) as shrink_adj_n,
        SUM(base_cover_total_qty) as cover,
        SUM(base_cover_soldout_qty) as cover_so,
        SUM(rounding_net_qty) as round_net,
        SUM(rounding_up_qty) as round_up,
        SUM(rounding_down_qty) as round_dn,
        SUM(safety_stock_qty) as safety,
        SUM(store_pass_net_qty) as store_pass,
        SUM(store_pass_growth_qty) as store_grow,
        SUM(store_pass_decline_qty) as store_decline,
        SUM(weather_adj_qty) as weather
        
    FROM waterfall_aggregate
    WHERE region_code IN ('{regions_str}')
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    """
    
    overall = conn.execute(overall_query).fetchdf().to_dict('records')[0]
    
    # By region summary
    by_region_query = f"""
    SELECT
        region_code as rgn,
        SUM(store_count) as stores,
        SUM(item_count) as items,
        SUM(line_count) as lines,
        SUM(lw_shipped) as lw_ship,
        SUM(lw_sold) as lw_sold,
        SUM(final_forecast_qty) as fcst,
        ROUND(100.0 * (SUM(final_forecast_qty) - SUM(lw_shipped)) / NULLIF(SUM(lw_shipped), 0), 2) as delta_pct,
        
        -- Key drivers
        SUM(ema_uplift_qty) as ema_up,
        SUM(base_cover_total_qty) as cover,
        SUM(rounding_net_qty) as round_net,
        SUM(store_pass_net_qty) as store_pass,
        SUM(weather_adj_qty) as weather
        
    FROM waterfall_aggregate
    WHERE region_code IN ('{regions_str}')
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY region_code
    ORDER BY SUM(final_forecast_qty) DESC
    """
    
    by_region = conn.execute(by_region_query).fetchdf().to_dict('records')
    
    # By date summary (for trend analysis)
    by_date_query = f"""
    SELECT
        date_forecast as dt,
        strftime(date_forecast, '%a') as dow,
        SUM(store_count) as stores,
        SUM(lw_shipped) as lw_ship,
        SUM(lw_sold) as lw_sold,
        SUM(final_forecast_qty) as fcst,
        ROUND(100.0 * (SUM(final_forecast_qty) - SUM(lw_shipped)) / NULLIF(SUM(lw_shipped), 0), 2) as delta_pct,
        SUM(weather_adj_qty) as weather,
        ROUND(AVG(CASE WHEN weather_adj_count > 0 THEN weather_adj_qty::DOUBLE / weather_adj_count ELSE 0 END), 1) as avg_weather_per_item
        
    FROM waterfall_aggregate
    WHERE region_code IN ('{regions_str}')
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY date_forecast
    ORDER BY date_forecast
    """
    
    by_date = conn.execute(by_date_query).fetchdf()
    # Convert date to string for JSON serialization
    by_date['dt'] = by_date['dt'].astype(str)
    by_date = by_date.to_dict('records')
    
    # Waterfall breakdown (what components are driving change)
    waterfall_query = f"""
    SELECT
        -- Starting point
        SUM(lw_sold) as base_lw_sold,
        
        -- Step 1: Baseline uplift (total change from lw_sold to baseline)
        SUM(baseline_uplift_qty) as baseline_up,
        
        -- Step 1b: Baseline source breakdown (for reference)
        SUM(baseline_lw_sales_qty) as bl_lw,
        SUM(baseline_ema_qty) as bl_ema,
        SUM(baseline_avg_qty) as bl_avg,
        SUM(baseline_min_case_qty) as bl_min,
        
        -- Step 2: EMA uplift (legacy - subset of baseline_uplift)
        SUM(ema_uplift_qty) as ema_up,
        
        -- Step 3: Decline adjustment
        SUM(decline_adj_qty) as decline,
        
        -- Step 4: High shrink reduction
        SUM(high_shrink_adj_qty) as shrink_red,
        
        -- Step 5: Base cover
        SUM(base_cover_total_qty) as cover,
        
        -- Step 6: Rounding
        SUM(rounding_net_qty) as round_net,
        
        -- Step 7: Safety stock
        SUM(safety_stock_qty) as safety,
        
        -- Step 8: Store pass adjustment
        SUM(store_pass_net_qty) as store_pass,
        
        -- Step 9: Weather adjustment
        SUM(weather_adj_qty) as weather,
        
        -- Final
        SUM(final_forecast_qty) as final
        
    FROM waterfall_aggregate
    WHERE region_code IN ('{regions_str}')
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    """
    
    waterfall = conn.execute(waterfall_query).fetchdf().to_dict('records')[0]
    
    # Clean up None/NaN values
    for key in overall:
        if overall[key] is None or (isinstance(overall[key], float) and overall[key] != overall[key]):
            overall[key] = 0
    for key in waterfall:
        if waterfall[key] is None or (isinstance(waterfall[key], float) and waterfall[key] != waterfall[key]):
            waterfall[key] = 0
    
    return {
        "meta": {
            "type": "executive",
            "regions": regions,
            "period": {"start": start_date, "end": end_date},
            "generated": datetime.now().isoformat()
        },
        "overall": overall,
        "by_region": by_region,
        "by_date": by_date,
        "waterfall": waterfall
    }


def export_executive_ai(conn, regions: List[str], 
                        start_date: str, end_date: str,
                        output_dir: str = None) -> str:
    """
    Export executive-level AI analysis data.
    
    Creates a compact JSON file with:
    - Overall forecast summary
    - Regional breakdown
    - Daily trends
    - Waterfall component analysis
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Output directory
        
    Returns:
        Path to exported file
    """
    if output_dir is None:
        output_dir = os.path.join(settings.OUTPUT_DIR, 'ai_analysis')
    os.makedirs(output_dir, exist_ok=True)
    
    data = get_executive_summary_data(conn, regions, start_date, end_date)
    
    # Export as compact JSON (no pretty-print to save tokens)
    filename = f"ai_executive_{start_date}_{end_date}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, separators=(',', ':'), default=str)
    
    print(f"AI Executive export: {filepath}")
    return filepath
