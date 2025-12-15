#!/usr/bin/env python
"""
Generate Executive Summary Report
=================================
Standalone script to generate executive summary reports.

This script can be run independently of the main forecasting pipeline
to generate executive summary Excel reports from existing forecast data.

Usage:
    python generate_executive_summary.py

The script will:
1. Connect to the existing DuckDB database
2. Generate executive summary for configured date range
3. Save to output/excel_summary/ directory
"""

import os
import sys

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from data.loader import DataLoader
from export.executive_summary import export_all_executive_summaries


def main():
    """Generate executive summary report."""
    print("=" * 70)
    print("EXECUTIVE SUMMARY REPORT GENERATOR")
    print("=" * 70)
    print(f"Forecast Range: {settings.FORECAST_START_DATE} to {settings.FORECAST_END_DATE}")
    print(f"Regions: {settings.REGION_CODES}")
    print("=" * 70)
    
    # Ensure output directories exist
    settings.ensure_directories()
    
    # Initialize data loader and get connection
    loader = DataLoader()
    conn = loader.get_connection()
    
    # Check if forecast data exists
    check_query = f"""
        SELECT COUNT(*) as cnt 
        FROM forecast_results
        WHERE date_forecast BETWEEN '{settings.FORECAST_START_DATE}' 
        AND '{settings.FORECAST_END_DATE}'
    """
    
    try:
        result = conn.execute(check_query).fetchone()
        record_count = result[0] if result else 0
        
        if record_count == 0:
            print("\nWARNING: No forecast data found for the specified date range!")
            print("Please run the main forecasting pipeline first: python main.py")
            loader.disconnect()
            return
        
        print(f"\nFound {record_count:,} forecast records.")
        
    except Exception as e:
        print(f"\nError checking forecast data: {e}")
        print("The forecast_results table may not exist. Run the main pipeline first.")
        loader.disconnect()
        return
    
    # Generate executive summary
    print("\nGenerating executive summary report...")
    
    try:
        filepaths = export_all_executive_summaries(
            conn, 
            settings.REGION_CODES,
            settings.FORECAST_START_DATE_V, 
            settings.FORECAST_END_DATE_V
        )
        
        print("\n" + "=" * 70)
        print("EXECUTIVE SUMMARY COMPLETE")
        print("=" * 70)
        
        if filepaths:
            print("\nGenerated files:")
            for fp in filepaths:
                print(f"  - {fp}")
        
    except Exception as e:
        print(f"\nError generating executive summary: {e}")
        import traceback
        traceback.print_exc()
    
    # Cleanup
    loader.disconnect()


if __name__ == "__main__":
    main()
