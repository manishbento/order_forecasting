"""
JF Export Module
================
Exports forecast results in JF format for SD region stores.

Output Format (no headers):
- customer_number (jf#)
- date (MM/DD/YYYY)
- blank column
- upc_code
- quantity (forecast packs)
- unit (always "EA")
- date (MM/DD/YYYY) - repeated

Each day gets its own file named: YYYY-MM-DD.csv
Only stores defined in JF_STORE_MAPPING are included.
Only items with UPC mappings are exported.
"""

import os
import csv
from datetime import datetime, timedelta
from typing import Optional

from config import settings
from config.jf_mappings import (
    JF_EXPORT_REGION,
    JF_UNIT_OF_MEASURE,
    JF_CSV_DATE_FORMAT,
    get_jf_customer_number,
    get_upc_code,
    get_mapped_stores,
    get_mapped_items
)


def get_jf_export_query(forecast_date: str) -> str:
    """
    Generate the SQL query to get forecast data for JF export.
    
    Args:
        forecast_date: Forecast date (YYYY-MM-DD)
        
    Returns:
        SQL query string
    """
    # Get only the stores that have JF mappings
    mapped_stores = get_mapped_stores()
    if not mapped_stores:
        return ""
    
    stores_str = ','.join(str(s) for s in mapped_stores)
    
    # Get only the items that have UPC mappings
    mapped_items = get_mapped_items()
    if not mapped_items:
        return ""
    
    items_str = ','.join(str(i) for i in mapped_items)
    
    # Build inactive stores filter (exclude any inactive stores)
    inactive_stores_filter = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_filter = f"AND store_no NOT IN ({inactive_stores_str})"
    
    # Build inactive store-item combinations filter
    inactive_store_items_filter = ""
    if settings.INACTIVE_STORE_ITEMS:
        conditions = [
            f"(store_no = {store_no} AND item_no = {item_no})"
            for store_no, item_no in settings.INACTIVE_STORE_ITEMS
        ]
        inactive_store_items_filter = f"AND NOT ({' OR '.join(conditions)})"
    
    query = f'''
        SELECT
            store_no,
            item_no,
            date_forecast,
            forecast_quantity
        FROM forecast_results
        WHERE region_code = '{JF_EXPORT_REGION}'
        AND date_forecast = '{forecast_date}'
        AND store_no IN ({stores_str})
        AND item_no IN ({items_str})
        AND forecast_quantity > 0
        {inactive_stores_filter}
        {inactive_store_items_filter}
        ORDER BY store_no, item_no
    '''
    
    return query


def export_jf_for_date(conn, forecast_date: str,
                       output_dir: str = None) -> Optional[str]:
    """
    Export forecast results for a single date in JF format.
    
    Args:
        conn: DuckDB connection
        forecast_date: Forecast date (YYYY-MM-DD)
        output_dir: Output directory path (defaults to settings.JF_OUTPUT_DIR)
        
    Returns:
        Path to the exported file, or None if no data was exported
    """
    # Use default output directory if not specified
    if output_dir is None:
        output_dir = getattr(settings, 'JF_OUTPUT_DIR', 
                             os.path.join(settings.OUTPUT_DIR, 'jf'))
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Get the query
    query = get_jf_export_query(forecast_date)
    if not query:
        print(f"No stores or items mapped for JF export, skipping {forecast_date}...")
        return None
    
    # Execute the query
    try:
        results = conn.execute(query).fetchall()
    except Exception as e:
        print(f"Error querying forecast data for {forecast_date}: {e}")
        return None
    
    if not results:
        print(f"No data for {JF_EXPORT_REGION} on {forecast_date}, skipping JF export...")
        return None
    
    # Parse the forecast date for formatting
    forecast_dt = datetime.strptime(forecast_date, '%Y-%m-%d')
    date_formatted = forecast_dt.strftime(JF_CSV_DATE_FORMAT)
    
    # Build output rows
    output_rows = []
    
    for row in results:
        store_no = row[0]
        item_no = row[1]
        forecast_qty = int(row[3])  # forecast_quantity
        
        # Get JF customer number
        jf_customer = get_jf_customer_number(store_no)
        if jf_customer is None:
            continue
        
        # Get UPC code
        upc_code = get_upc_code(item_no)
        if upc_code is None:
            continue
        
        # Build the output row:
        # customer_number, date, blank, upc, quantity, unit, date
        output_row = [
            jf_customer,           # Customer number (JF#)
            date_formatted,        # Date (MM/DD/YYYY)
            '',                    # Blank column
            upc_code,              # UPC code
            forecast_qty,          # Quantity (packs)
            JF_UNIT_OF_MEASURE,    # Unit (EA)
            date_formatted         # Date (MM/DD/YYYY) repeated
        ]
        output_rows.append(output_row)
    
    if not output_rows:
        print(f"No valid mappings found for {forecast_date}, skipping...")
        return None
    
    # Generate output filename
    output_file = os.path.join(
        output_dir,
        f'{forecast_date}.csv'
    )
    
    # Write to CSV (no headers)
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(output_rows)
    
    print(f"JF Export: {output_file} ({len(output_rows)} rows)")
    return output_file


def export_jf_for_date_range(conn, start_date: str, end_date: str,
                              output_dir: str = None) -> list[str]:
    """
    Export forecast results for a date range in JF format.
    
    Creates one file per day.
    
    Args:
        conn: DuckDB connection
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Output directory path
        
    Returns:
        List of paths to exported files
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    exported_files = []
    current = start
    
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        file_path = export_jf_for_date(conn, date_str, output_dir)
        if file_path:
            exported_files.append(file_path)
        current += timedelta(days=1)
    
    return exported_files


def export_jf_all(conn, output_dir: str = None) -> list[str]:
    """
    Export all forecast dates in JF format using settings date range.
    
    Args:
        conn: DuckDB connection
        output_dir: Output directory path
        
    Returns:
        List of paths to exported files
    """
    return export_jf_for_date_range(
        conn,
        settings.FORECAST_START_DATE,
        settings.FORECAST_END_DATE,
        output_dir
    )


def get_jf_export_summary(conn, forecast_date: str) -> dict:
    """
    Get a summary of what would be exported for a given date.
    
    Args:
        conn: DuckDB connection
        forecast_date: Forecast date (YYYY-MM-DD)
        
    Returns:
        Dictionary with summary information
    """
    query = get_jf_export_query(forecast_date)
    if not query:
        return {'stores': 0, 'items': 0, 'rows': 0, 'total_qty': 0}
    
    try:
        results = conn.execute(query).fetchall()
    except Exception as e:
        return {'error': str(e)}
    
    stores = set()
    items = set()
    total_qty = 0
    valid_rows = 0
    
    for row in results:
        store_no = row[0]
        item_no = row[1]
        forecast_qty = int(row[3])
        
        jf_customer = get_jf_customer_number(store_no)
        upc_code = get_upc_code(item_no)
        
        if jf_customer and upc_code:
            stores.add(store_no)
            items.add(item_no)
            total_qty += forecast_qty
            valid_rows += 1
    
    return {
        'date': forecast_date,
        'stores': len(stores),
        'items': len(items),
        'rows': valid_rows,
        'total_qty': total_qty
    }
