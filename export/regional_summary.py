"""
Regional Summary Excel Export Module
====================================
Creates comprehensive regional summary reports for stakeholder review.

This module generates a multi-sheet Excel workbook with:
1. Daily Summary - Aggregated metrics by forecast date with trends and expected shrink
2. Store Summary - Daily breakdown by store with weather indicators
3. Item Summary - Daily breakdown by item with weather impact
4. Weather Impact - Weather impact summary with indicator logos
5. Item Details - Full item/store level detail with weather indicators

The export is designed to provide stakeholders with a complete view of
the forecasting process and weather adjustments.

NOTE: This module has been refactored into smaller components:
- summary_queries.py: SQL query functions
- summary_formatting.py: Format definitions and color helpers
- summary_writers.py: Worksheet writing functions
"""

import os
import xlsxwriter
from datetime import datetime

from config import settings

# Import from refactored modules
from .summary_formatting import create_summary_formats
from .summary_writers import (
    write_daily_summary_sheet,
    write_store_summary_sheet,
    write_item_summary_sheet,
    write_item_detail_sheet,
    write_weather_impact_sheet
)


def export_regional_summary(conn, region: str,
                            start_date: datetime, end_date: datetime,
                            output_dir: str = None) -> str:
    """
    Export comprehensive regional summary to Excel.
    
    Creates a multi-sheet workbook with:
    - Daily Summary: Aggregated metrics by forecast date with growth %
    - Store Summary: Aggregated metrics by store with weather
    - Item Summary: Aggregated metrics by item per date with growth %
    - Weather Impact: Comprehensive weather impact analysis
    - Item Details: Full item/store level detail with growth %
    
    Args:
        conn: DuckDB connection
        region: Region code
        start_date: Start date
        end_date: End date
        output_dir: Output directory (default: settings.EXCEL_OUTPUT_DIR/excel_summary)
        
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
    filename = f'{region}_{start_str}_{end_str}_Summary.xlsx'
    filepath = os.path.join(output_dir, filename)
    
    print(f"Creating regional summary for {region}: {filepath}")
    
    # Create workbook
    wb = xlsxwriter.Workbook(filepath, {'strings_to_numbers': True})
    
    # Create formats
    formats = create_summary_formats(wb)
    
    # Create worksheets
    write_daily_summary_sheet(wb, conn, region, start_str, end_str, formats)
    write_store_summary_sheet(wb, conn, region, start_str, end_str, formats)
    write_item_summary_sheet(wb, conn, region, start_str, end_str, formats)
    write_weather_impact_sheet(wb, conn, region, start_str, end_str, formats)
    write_item_detail_sheet(wb, conn, region, start_str, end_str, formats)
    
    # Close workbook
    wb.close()
    
    print(f"Regional summary exported: {filepath}")
    return filepath


def export_all_regional_summaries(conn, regions: list,
                                  start_date: datetime, end_date: datetime,
                                  output_dir: str = None) -> list:
    """
    Export regional summaries for all regions.
    
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
    for region in regions:
        try:
            filepath = export_regional_summary(conn, region, start_date, end_date, output_dir)
            filepaths.append(filepath)
        except Exception as e:
            print(f"Error exporting summary for region {region}: {e}")
    
    return filepaths
