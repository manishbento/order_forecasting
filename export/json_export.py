"""
JSON Export Module
==================
Exports forecast results to JSON format.
"""

import os
from config import settings


def export_forecast_to_json(conn, region: str, forecast_date: str,
                            output_dir: str = None):
    """
    Export forecast results for a region and date to JSON.
    
    Args:
        conn: DuckDB connection
        region: Region code
        forecast_date: Forecast date (YYYY-MM-DD)
        output_dir: Output directory path
    """
    output_dir = output_dir or settings.JSON_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    
    query = f'''
        SELECT *
        FROM forecast_results
        WHERE region_code = '{region}'
        AND date_forecast = '{forecast_date}'
        ORDER BY store_no, item_no
    '''
    
    forecast_results = conn.execute(query).fetchdf()
    
    if len(forecast_results) == 0:
        print(f"No data for {region} on {forecast_date}, skipping JSON export...")
        return
    
    output_file = os.path.join(
        output_dir,
        f'Costco_{region}_PO_{forecast_date}.json'
    )
    
    # Export with split orientation for compact structure
    forecast_results.to_json(output_file, orient='split', index=False, indent=2)
    print(f"Exported: {output_file}")


def export_all_to_json(conn, regions: list, start_date: str, end_date: str,
                       output_dir: str = None):
    """
    Export forecast results for all regions and dates to JSON.
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Output directory path
    """
    from datetime import datetime, timedelta
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    for region in regions:
        current = start
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            export_forecast_to_json(conn, region, date_str, output_dir)
            current += timedelta(days=1)
