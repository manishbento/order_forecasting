"""
Executive Summary Export Module
===============================
Main export functions for generating executive summary Excel reports.

Creates a professional multi-sheet Excel workbook with:
1. Regional Summary - Compact overview by region/date
2. Waterfall Analysis - Detailed adjustment breakdown
3. Waterfall Summary - Compact columnar view
4. Weather Impact - Weather severity and adjustments
5. Daily Totals - Company-wide daily totals
"""

import os
import xlsxwriter
from datetime import datetime

from config import settings
from .formatting import create_executive_formats
from .writers import (
    write_regional_summary_sheet,
    write_waterfall_sheet,
    write_waterfall_columnar_sheet,
    write_weather_summary_sheet,
    write_daily_totals_sheet,
)


def export_executive_summary(conn, regions: list,
                              start_date: datetime, end_date: datetime,
                              output_dir: str = None) -> str:
    """
    Export executive summary to Excel.
    
    Creates a multi-sheet workbook with:
    - Regional Summary: Compact overview by region/date with key metrics
    - Waterfall Analysis: Detailed breakdown of adjustments
    - Waterfall Summary: Compact columnar view with qty (%)
    - Weather Impact: Weather severity and adjustment summary
    - Daily Totals: Company-wide daily totals
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date
        end_date: End date
        output_dir: Output directory (default: settings.OUTPUT_DIR/excel_summary)
        
    Returns:
        Path to the created Excel file
    """
    # Set up output directory
    if output_dir is None:
        output_dir = os.path.join(
            os.path.dirname(settings.EXCEL_OUTPUT_DIR),
            'excel_summary'
        )
    os.makedirs(output_dir, exist_ok=True)
    
    # Format dates
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Create filename
    regions_str = '_'.join(sorted(regions)) if len(regions) <= 3 else 'ALL'
    filename = f'Executive_Summary_{regions_str}_{start_str}_{end_str}.xlsx'
    filepath = os.path.join(output_dir, filename)
    
    print(f"Creating executive summary: {filepath}")
    
    # Create workbook with optimization settings
    wb = xlsxwriter.Workbook(filepath, {
        'strings_to_numbers': True,
        'constant_memory': False,  # Allow full features
    })
    
    # Create formats
    formats = create_executive_formats(wb)
    
    # Create worksheets
    print("  Writing Regional Summary...")
    write_regional_summary_sheet(wb, conn, regions, start_str, end_str, formats)
    
    print("  Writing Waterfall Analysis...")
    write_waterfall_sheet(wb, conn, regions, start_str, end_str, formats)
    
    print("  Writing Waterfall Summary...")
    write_waterfall_columnar_sheet(wb, conn, regions, start_str, end_str, formats)
    
    print("  Writing Weather Impact...")
    write_weather_summary_sheet(wb, conn, regions, start_str, end_str, formats)
    
    print("  Writing Daily Totals...")
    write_daily_totals_sheet(wb, conn, regions, start_str, end_str, formats)
    
    # Close workbook
    wb.close()
    
    print(f"Executive summary exported: {filepath}")
    return filepath


def export_all_executive_summaries(conn, regions: list,
                                    start_date: datetime, end_date: datetime,
                                    output_dir: str = None) -> list:
    """
    Export executive summaries.
    
    Creates:
    1. One combined summary for all regions
    2. Individual summaries per region (optional)
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date
        end_date: End date
        output_dir: Output directory
        
    Returns:
        List of created file paths
    """
    filepaths = []
    
    # Export combined summary for all regions
    try:
        filepath = export_executive_summary(
            conn, regions, start_date, end_date, output_dir
        )
        filepaths.append(filepath)
    except Exception as e:
        print(f"Error exporting combined executive summary: {e}")
        import traceback
        traceback.print_exc()
    
    return filepaths
