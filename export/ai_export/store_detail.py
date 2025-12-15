"""
Store Detail AI Export
======================
Store/item level detail for AI validation and reasoning.

Provides per-store:
- Item-level forecast with waterfall components
- Historical data for validation
- Flagged items needing review

This is the most granular export for AI to validate individual forecasts.
Uses compact format with IDs only, abbreviated keys.
"""

import json
import os
from typing import List, Dict, Any

from config import settings


def get_store_detail_data(conn, region: str, store_no: int,
                          start_date: str, end_date: str,
                          top_n_items: int = 50) -> Dict[str, Any]:
    """
    Get detailed store-level data for AI validation.
    
    Args:
        conn: DuckDB connection
        region: Region code
        store_no: Store number
        start_date: Start date
        end_date: End date
        top_n_items: Limit number of items (top by forecast)
        
    Returns:
        Dictionary with store detail data
    """
    # Store summary across dates
    store_summary_query = f"""
    SELECT
        store_no as sid,
        COUNT(DISTINCT date_forecast) as n_days,
        COUNT(DISTINCT item_no) as n_items,
        COUNT(*) as lines,
        
        -- Store-level history
        AVG(store_w1_shrink_p) as avg_shrink_w1,
        AVG(store_w2_shrink_p) as avg_shrink_w2,
        AVG(store_w3_shrink_p) as avg_shrink_w3,
        AVG(store_w4_shrink_p) as avg_shrink_w4,
        
        -- Totals
        SUM(w1_shipped) as lw_ship,
        SUM(w1_sold) as lw_sold,
        SUM(forecast_quantity) as fcst,
        SUM(forecast_quantity) - SUM(w1_shipped) as delta,
        ROUND(100.0 * (SUM(forecast_quantity) - SUM(w1_shipped)) / NULLIF(SUM(w1_shipped), 0), 1) as delta_pct,
        
        -- Adjustment totals
        SUM(COALESCE(ema_uplift_qty, 0)) as ema_up,
        SUM(COALESCE(base_cover_qty, 0)) as cover,
        SUM(COALESCE(rounding_net_qty, 0)) as round_net,
        SUM(COALESCE(store_level_growth_qty, 0) + COALESCE(store_level_decline_qty, 0)) as store_adj,
        SUM(COALESCE(weather_adjustment_qty, 0)) as weather
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND store_no = {store_no}
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_no
    """
    
    store_summary = conn.execute(store_summary_query).fetchdf().to_dict('records')
    store_summary = store_summary[0] if store_summary else {}
    
    # Item-level detail with waterfall components
    # Using compact column aliases
    items_query = f"""
    SELECT
        item_no as iid,
        date_forecast as dt,
        
        -- Historical (4 weeks)
        w4_sold as w4s,
        w3_sold as w3s,
        w2_sold as w2s,
        w1_sold as w1s,
        w4_shipped as w4r,
        w3_shipped as w3r,
        w2_shipped as w2r,
        w1_shipped as w1r,
        
        -- Calculated metrics
        ROUND(sales_velocity, 2) as vel,
        ROUND(ema, 1) as ema,
        ROUND(average_sold, 1) as avg,
        
        -- Baseline info
        baseline_source as bl_src,
        ROUND(baseline_qty, 1) as bl_qty,
        
        -- Waterfall components
        COALESCE(ema_uplift_applied, 0) as ema_up_f,
        ROUND(COALESCE(ema_uplift_qty, 0), 1) as ema_up,
        COALESCE(decline_adj_applied, 0) as dec_f,
        ROUND(COALESCE(decline_adj_qty, 0), 1) as dec_adj,
        COALESCE(high_shrink_adj_applied, 0) as shrk_f,
        ROUND(COALESCE(high_shrink_adj_qty, 0), 1) as shrk_adj,
        base_cover_type as cv_type,
        ROUND(COALESCE(base_cover_qty, 0), 1) as cover,
        rounding_direction as rnd_dir,
        ROUND(COALESCE(rounding_net_qty, 0), 1) as rnd_net,
        COALESCE(forecast_safety_stock_applied, 0) as safety,
        COALESCE(store_level_adjusted, 0) as st_adj_f,
        ROUND(COALESCE(store_level_growth_qty, 0) + COALESCE(store_level_decline_qty, 0), 1) as st_adj,
        store_level_adjustment_reason as st_adj_rsn,
        COALESCE(weather_adjusted, 0) as wx_f,
        ROUND(COALESCE(weather_adjustment_qty, 0), 1) as wx_adj,
        weather_adjustment_reason as wx_rsn,
        ROUND(COALESCE(weather_severity_score, 0), 2) as wx_sev,
        
        -- Forecast stages
        ROUND(forecast_average, 1) as f_avg,
        ROUND(forecast_average_w_cover, 1) as f_cov,
        forecast_quantity as fcst,
        
        -- Delta
        forecast_quantity - w1_shipped as delta,
        
        -- Shrink context
        ROUND(w1_shrink_p, 3) as shrk_w1,
        ROUND(w2_shrink_p, 3) as shrk_w2,
        
        -- Sold out flag
        sold_out_lw as so_lw
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND store_no = {store_no}
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY forecast_quantity DESC
    LIMIT {top_n_items}
    """
    
    items = conn.execute(items_query).fetchdf()
    if not items.empty:
        items['dt'] = items['dt'].astype(str)
    items = items.to_dict('records')
    
    # Items with large adjustments (for review)
    review_items_query = f"""
    SELECT
        item_no as iid,
        date_forecast as dt,
        w1_sold as w1s,
        w1_shipped as w1r,
        forecast_quantity as fcst,
        forecast_quantity - w1_shipped as delta,
        ROUND(100.0 * (forecast_quantity - w1_shipped) / NULLIF(w1_shipped, 0), 1) as delta_pct,
        baseline_source as bl_src,
        store_level_adjustment_reason as reason,
        weather_adjustment_reason as wx_rsn
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND store_no = {store_no}
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    AND (
        ABS(forecast_quantity - w1_shipped) > 5
        OR ABS(100.0 * (forecast_quantity - w1_shipped) / NULLIF(w1_shipped, 0)) > 30
    )
    ORDER BY ABS(forecast_quantity - w1_shipped) DESC
    LIMIT 20
    """
    
    review_items = conn.execute(review_items_query).fetchdf()
    if not review_items.empty:
        review_items['dt'] = review_items['dt'].astype(str)
    review_items = review_items.to_dict('records')
    
    return {
        "meta": {
            "type": "store_detail",
            "region": region,
            "store": store_no,
            "period": {"start": start_date, "end": end_date}
        },
        "summary": store_summary,
        "items": items,
        "review": review_items
    }


def get_all_stores_summary(conn, region: str,
                           start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    Get summary for all stores in a region (for AI to pick which to analyze).
    
    Returns:
        List of store summaries with key metrics
    """
    query = f"""
    SELECT
        store_no as sid,
        COUNT(DISTINCT date_forecast) as n_days,
        COUNT(DISTINCT item_no) as n_items,
        SUM(w1_shipped) as lw_ship,
        SUM(w1_sold) as lw_sold,
        SUM(forecast_quantity) as fcst,
        ROUND(100.0 * (SUM(forecast_quantity) - SUM(w1_shipped)) / NULLIF(SUM(w1_shipped), 0), 1) as delta_pct,
        ROUND(AVG(store_w1_shrink_p) * 100, 1) as avg_shrink,
        SUM(CASE WHEN store_level_adjusted = 1 THEN 1 ELSE 0 END) as n_store_adj,
        SUM(CASE WHEN weather_adjusted = 1 THEN 1 ELSE 0 END) as n_wx_adj
        
    FROM forecast_results
    WHERE region_code = '{region}'
    AND date_forecast BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY store_no
    ORDER BY SUM(forecast_quantity) DESC
    """
    
    return conn.execute(query).fetchdf().to_dict('records')


def export_store_detail_ai(conn, regions: List[str],
                           start_date: str, end_date: str,
                           output_dir: str = None,
                           stores_per_region: int = 10,
                           include_store_index: bool = True) -> List[str]:
    """
    Export store-level detail for AI validation.
    
    Creates:
    - One index file per region listing all stores with summary metrics
    - Detail files for top N stores by volume
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date
        end_date: End date
        output_dir: Output directory
        stores_per_region: Number of store detail files per region
        include_store_index: Whether to export store index file
        
    Returns:
        List of paths to exported files
    """
    if output_dir is None:
        output_dir = os.path.join(settings.OUTPUT_DIR, 'ai_analysis', 'store_detail')
    os.makedirs(output_dir, exist_ok=True)
    
    filepaths = []
    
    for region in regions:
        # Get all stores for this region
        stores = get_all_stores_summary(conn, region, start_date, end_date)
        
        if include_store_index:
            # Export store index
            index_data = {
                "meta": {
                    "type": "store_index",
                    "region": region,
                    "period": {"start": start_date, "end": end_date},
                    "n_stores": len(stores)
                },
                "stores": stores
            }
            
            index_file = f"ai_stores_{region}_{start_date}_{end_date}.json"
            index_path = os.path.join(output_dir, index_file)
            
            with open(index_path, 'w') as f:
                json.dump(index_data, f, separators=(',', ':'), default=str)
            
            print(f"AI Store index ({region}): {index_path}")
            filepaths.append(index_path)
        
        # Export detail for top stores
        top_stores = [s['sid'] for s in stores[:stores_per_region]]
        
        for store_no in top_stores:
            store_data = get_store_detail_data(
                conn, region, store_no, start_date, end_date
            )
            
            store_file = f"ai_store_{region}_{store_no}_{start_date}_{end_date}.json"
            store_path = os.path.join(output_dir, store_file)
            
            with open(store_path, 'w') as f:
                json.dump(store_data, f, separators=(',', ':'), default=str)
            
            filepaths.append(store_path)
        
        print(f"AI Store details ({region}): {len(top_stores)} stores exported")
    
    return filepaths


def export_store_detail_single(conn, region: str, store_no: int,
                                start_date: str, end_date: str,
                                output_dir: str = None) -> str:
    """
    Export detail for a single store (on-demand).
    
    Args:
        conn: DuckDB connection
        region: Region code
        store_no: Store number
        start_date: Start date
        end_date: End date
        output_dir: Output directory
        
    Returns:
        Path to exported file
    """
    if output_dir is None:
        output_dir = os.path.join(settings.OUTPUT_DIR, 'ai_analysis', 'store_detail')
    os.makedirs(output_dir, exist_ok=True)
    
    store_data = get_store_detail_data(conn, region, store_no, start_date, end_date)
    
    filename = f"ai_store_{region}_{store_no}_{start_date}_{end_date}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w') as f:
        json.dump(store_data, f, separators=(',', ':'), default=str)
    
    print(f"AI Store detail: {filepath}")
    return filepath
