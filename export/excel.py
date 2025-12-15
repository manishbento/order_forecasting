"""
Excel Export Module
===================
Exports forecast results to formatted Excel files.

Enhanced with weather adjustment columns:
- Weather status indicator (unicode icons) - left aligned
- Weather severity and category
- Weather adjustment quantity
- Last week shrink %
- 4-week shipped qty trend (W4 > W3 > W2 > W1)
- 4-week sold qty trend (W4 > W3 > W2 > W1)

Features:
- Frozen first row
- Autofilter on first row
"""

import os
import polars as pl
from datetime import datetime, timedelta

from config import settings
from utils.xl_writer import XLWriter


def get_forecast_export_query(region: str, date_str: str) -> str:
    """
    Generate query for forecast export.
    
    Args:
        region: Region code
        date_str: Forecast date (YYYY-MM-DD)
        
    Returns:
        SQL query string
    """
    # Build inactive stores filter
    inactive_stores_filter = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_filter = f"AND fr.store_no NOT IN ({inactive_stores_str})"
    
    # Build inactive store-item combinations filter
    inactive_store_items_filter = ""
    if settings.INACTIVE_STORE_ITEMS:
        conditions = [
            f"(fr.store_no = {store_no} AND fr.item_no = {item_no})"
            for store_no, item_no in settings.INACTIVE_STORE_ITEMS
        ]
        inactive_store_items_filter = f"AND NOT ({' OR '.join(conditions)})"
    
    return f'''
        SELECT
            EXTRACT(WEEK FROM fr.date_forecast) AS "Fiscal Week #",
            fr.date_forecast AS "Date",
            strftime(fr.date_forecast, '%A') AS "Day Name",
            fr.region_code AS "Region",
            fr.store_no AS "Warehouse #",
            nm.store_name AS "Warehouse Name",
            fr.item_no AS "Item #",
            fr.item_desc AS "Item Description",
            fr.forecast_quantity AS "PO Qty (Units)",
            fr.forecast_quantity / fr.case_pack_size AS "PO Qty (Cases)"
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
        AND fr.date_forecast = '{date_str}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        ORDER BY fr.store_no, fr.item_no
    '''


def get_forecast_export_query_with_weather(region: str, date_str: str) -> str:
    """
    Generate query for forecast export with weather adjustment columns.
    
    Includes:
    - Weather status indicator
    - Weather severity and adjustment info
    - Last week shrink %
    - 4-week shipped/sold trends
    
    Args:
        region: Region code
        date_str: Forecast date (YYYY-MM-DD)
        
    Returns:
        SQL query string
    """
    # Build inactive stores filter
    inactive_stores_filter = ""
    if settings.INACTIVE_STORES:
        inactive_stores_str = ','.join(str(s) for s in settings.INACTIVE_STORES)
        inactive_stores_filter = f"AND fr.store_no NOT IN ({inactive_stores_str})"
    
    # Build inactive store-item combinations filter
    inactive_store_items_filter = ""
    if settings.INACTIVE_STORE_ITEMS:
        conditions = [
            f"(fr.store_no = {store_no} AND fr.item_no = {item_no})"
            for store_no, item_no in settings.INACTIVE_STORE_ITEMS
        ]
        inactive_store_items_filter = f"AND NOT ({' OR '.join(conditions)})"
    
    return f'''
        SELECT
            EXTRACT(WEEK FROM fr.date_forecast) AS "Fiscal Week #",
            fr.date_forecast AS "Date",
            strftime(fr.date_forecast, '%A') AS "Day Name",
            fr.region_code AS "Region",
            fr.store_no AS "Warehouse #",
            nm.store_name AS "Warehouse Name",
            fr.item_no AS "Item #",
            fr.item_desc AS "Item Description",
            
            -- Forecast quantities (simplified names)
            fr.forecast_quantity AS "PO Qty (Units)",
            fr.forecast_quantity / NULLIF(fr.case_pack_size, 0) AS "PO Qty (Cases)",
            
            -- Weather info
            COALESCE(fr.weather_status_indicator, '-') AS "Weather Status",
            COALESCE(fr.weather_severity_score, 0) AS "Weather Severity",
            COALESCE(fr.weather_severity_category, 'MINIMAL') AS "Severity Category",
            COALESCE(fr.weather_adjustment_qty, 0) AS "Weather Adj Qty",
            
            -- Last week shrink %
            ROUND(COALESCE(fr.w1_shrink_p, 0) * 100, 1) AS "LW Shrink %",
            
            -- 4-week shipped trend (W4 > W3 > W2 > W1)
            CONCAT(
                COALESCE(CAST(fr.w4_shipped AS VARCHAR), '-'), ' > ',
                COALESCE(CAST(fr.w3_shipped AS VARCHAR), '-'), ' > ',
                COALESCE(CAST(fr.w2_shipped AS VARCHAR), '-'), ' > ',
                COALESCE(CAST(fr.w1_shipped AS VARCHAR), '-')
            ) AS "Shipped Qty Trend (W4>W3>W2>W1)",
            
            -- 4-week sold trend (W4 > W3 > W2 > W1)
            CONCAT(
                COALESCE(CAST(fr.w4_sold AS VARCHAR), '-'), ' > ',
                COALESCE(CAST(fr.w3_sold AS VARCHAR), '-'), ' > ',
                COALESCE(CAST(fr.w2_sold AS VARCHAR), '-'), ' > ',
                COALESCE(CAST(fr.w1_sold AS VARCHAR), '-')
            ) AS "Sold Qty Trend (W4>W3>W2>W1)"
            
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
        AND fr.date_forecast = '{date_str}'
        {inactive_stores_filter}
        {inactive_store_items_filter}
        ORDER BY fr.store_no, fr.item_no
    '''


def get_weather_summary_query(region: str, date_str: str) -> str:
    """
    Generate query to get weather summary for a region/date.
    
    Args:
        region: Region code
        date_str: Forecast date
        
    Returns:
        SQL query string
    """
    return f'''
        SELECT 
            fr.store_no AS "Warehouse #",
            nm.store_name AS "Warehouse Name",
            MAX(fr.weather_severity_score) AS "Max Severity",
            MAX(fr.weather_severity_category) AS "Severity Category",
            SUM(fr.weather_adjustment_qty) AS "Total Weather Adj",
            SUM(fr.forecast_qty_pre_weather) AS "Pre-Weather Total",
            SUM(fr.forecast_quantity) AS "Post-Weather Total",
            COUNT(*) AS "Items Count",
            SUM(CASE WHEN fr.weather_adjusted = 1 THEN 1 ELSE 0 END) AS "Items Adjusted"
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
        AND fr.date_forecast = '{date_str}'
        GROUP BY fr.store_no, nm.store_name
        ORDER BY MAX(fr.weather_severity_score) DESC
    '''


def export_region_to_excel(conn, region: str, 
                           start_date: datetime, end_date: datetime,
                           output_dir: str = None,
                           include_weather: bool = True):
    """
    Export forecast results for a region to Excel.
    
    Creates one worksheet per day within the date range.
    
    Args:
        conn: DuckDB connection
        region: Region code
        start_date: Start date
        end_date: End date
        output_dir: Output directory path
        include_weather: Include weather adjustment columns (default True)
    """
    output_dir = output_dir or settings.EXCEL_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    suffix = '_WEATHER' if include_weather else ''
    output_file = os.path.join(
        output_dir, 
        f'Costco_{region}_PO_{start_str}_{end_str}_SOURCE{suffix}.xlsx'
    )
    
    xl = XLWriter(output_file)
    
    # Create custom formats for weather indicators (center-aligned)
    weather_red_format = xl.wb.add_format({
        'bg_color': '#FFC7CE',
        'font_color': '#9C0006',
        'align': 'center',
        'border': 1
    })
    weather_orange_format = xl.wb.add_format({
        'bg_color': '#FFEB9C',
        'font_color': '#9C6500',
        'align': 'center',
        'border': 1
    })
    weather_green_format = xl.wb.add_format({
        'bg_color': '#C6EFCE',
        'font_color': '#006100',
        'align': 'center',
        'border': 1
    })
    
    # Create left-aligned formats for Weather Status
    weather_status_red_format = xl.wb.add_format({
        'bg_color': '#FFC7CE',
        'font_color': '#9C0006',
        'align': 'left',
        'border': 1
    })
    weather_status_orange_format = xl.wb.add_format({
        'bg_color': '#FFEB9C',
        'font_color': '#9C6500',
        'align': 'left',
        'border': 1
    })
    weather_status_green_format = xl.wb.add_format({
        'bg_color': '#C6EFCE',
        'font_color': '#006100',
        'align': 'left',
        'border': 1
    })
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Get data using appropriate query
        if include_weather:
            query = get_forecast_export_query_with_weather(region, date_str)
        else:
            query = get_forecast_export_query(region, date_str)
            
        try:
            results = pl.from_pandas(conn.sql(query).to_df())
        except Exception as e:
            print(f"Error executing query for {region} on {date_str}: {e}")
            # Try basic query if enhanced fails
            query = get_forecast_export_query(region, date_str)
            results = pl.from_pandas(conn.sql(query).to_df())
            include_weather = False  # Disable for remaining dates
        
        if len(results) == 0:
            print(f"No data for {region} on {date_str}, skipping...")
            current_date += timedelta(days=1)
            continue
        
        # Create worksheet
        ws_name = current_date.strftime('%a').upper()
        ws = xl.wb.add_worksheet(name=ws_name)
        
        # Write header
        header = results.columns
        for col_num, col_name in enumerate(header):
            ws.write(0, col_num, col_name, xl.format_col_title)
        
        # Write data with conditional formatting for weather columns
        for row_num, row in enumerate(results.to_dicts(), start=1):
            for col_num, col_name in enumerate(header):
                value = row[col_name]
                
                if col_name == 'Date':
                    ws.write(row_num, col_num, value, xl.format_date_costco)
                elif col_name == 'Weather Status':
                    # Apply color based on severity (left-aligned)
                    severity = row.get('Weather Severity', 0) or 0
                    if severity >= 6:
                        ws.write(row_num, col_num, value, weather_status_red_format)
                    elif severity >= 4:
                        ws.write(row_num, col_num, value, weather_status_orange_format)
                    else:
                        ws.write(row_num, col_num, value, weather_status_green_format)
                elif col_name == 'Severity Category':
                    category = value or 'MINIMAL'
                    if category in ('SEVERE', 'HIGH'):
                        ws.write(row_num, col_num, value, weather_red_format)
                    elif category == 'MODERATE':
                        ws.write(row_num, col_num, value, weather_orange_format)
                    else:
                        ws.write(row_num, col_num, value, weather_green_format)
                elif col_name == 'Weather Adj Qty':
                    if value and value > 0:
                        ws.write(row_num, col_num, value, weather_orange_format)
                    else:
                        ws.write(row_num, col_num, value)
                elif col_name == 'LW Shrink %':
                    # Highlight high shrink percentages
                    if value and value >= 20:
                        ws.write(row_num, col_num, value, weather_red_format)
                    elif value and value >= 10:
                        ws.write(row_num, col_num, value, weather_orange_format)
                    else:
                        ws.write(row_num, col_num, value)
                else:
                    ws.write(row_num, col_num, value)
        
        # Freeze the first row
        ws.freeze_panes(1, 0)
        
        # Add autofilter to the first row
        if len(results) > 0:
            ws.autofilter(0, 0, len(results), len(header) - 1)
        
        ws.autofit()
        
        # Set column widths for specific columns
        if include_weather:
            for col_num, col_name in enumerate(header):
                if col_name == 'Weather Status':
                    ws.set_column(col_num, col_num, 50)  # Wider for weather status
                elif 'Trend' in col_name:
                    ws.set_column(col_num, col_num, 25)  # Wider for trend columns
        
        current_date += timedelta(days=1)
    
    xl.close()
    print(f"Exported: {output_file}")


def _add_weather_summary_sheet(xl, conn, region: str, 
                               start_date: datetime, end_date: datetime):
    """
    Add a weather summary sheet to the workbook.
    
    Args:
        xl: XLWriter instance
        conn: DuckDB connection
        region: Region code
        start_date: Start date
        end_date: End date
    """
    ws = xl.wb.add_worksheet(name='Weather Summary')
    
    # Create title format
    title_format = xl.wb.add_format({
        'bold': True,
        'font_size': 14,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1
    })
    
    # Create header format
    header_format = xl.wb.add_format({
        'bold': True,
        'align': 'center',
        'bg_color': '#D9E2F3',
        'border': 1
    })
    
    row_idx = 0
    
    # Title
    ws.merge_range(row_idx, 0, row_idx, 8, 
                   f'Weather Adjustment Summary - Region {region}', 
                   title_format)
    row_idx += 2
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        day_name = current_date.strftime('%A')
        
        try:
            query = get_weather_summary_query(region, date_str)
            results = pl.from_pandas(conn.sql(query).to_df())
            
            if len(results) > 0:
                # Date header
                ws.write(row_idx, 0, f'{day_name} ({date_str})', header_format)
                row_idx += 1
                
                # Column headers
                header = results.columns
                for col_num, col_name in enumerate(header):
                    ws.write(row_idx, col_num, col_name, header_format)
                row_idx += 1
                
                # Data rows
                for row in results.to_dicts():
                    for col_num, col_name in enumerate(header):
                        ws.write(row_idx, col_num, row[col_name])
                    row_idx += 1
                
                row_idx += 1  # Blank row between days
        except Exception as e:
            print(f"Error getting weather summary for {date_str}: {e}")
        
        current_date += timedelta(days=1)
    
    ws.autofit()


def export_all_regions_to_excel(conn, regions: list,
                                start_date: datetime, end_date: datetime,
                                output_dir: str = None,
                                include_weather: bool = True):
    """
    Export forecast results for all regions to Excel.
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date
        end_date: End date
        output_dir: Output directory path
        include_weather: Include weather adjustment columns (default True)
    """
    for region in regions:
        print(f"Exporting Excel for region: {region}")
        export_region_to_excel(
            conn, region, start_date, end_date, 
            output_dir, include_weather=include_weather
        )
