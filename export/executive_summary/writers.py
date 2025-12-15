"""
Executive Summary Worksheet Writers
===================================
Worksheet writing functions for executive summary reports.

This module contains all worksheet creation functions:
- Regional Summary sheet
- Waterfall Analysis sheet
- Weather Impact Summary sheet
"""

import polars as pl

from .queries import (
    get_regional_summary_query,
    get_waterfall_components_query,
    get_weather_summary_query,
    get_all_regions_total_query,
)
from .formatting import (
    get_delta_format,
    get_shrink_format,
    get_waterfall_format,
    format_qty_with_pct,
)


def write_regional_summary_sheet(wb, conn, regions: list,
                                  start_date: str, end_date: str,
                                  formats: dict):
    """
    Create the Regional Summary worksheet.
    
    Compact overview by region and date with:
    - Forecast shipped vs last week shipped
    - Forecast sales vs last week sold  
    - Delta percentages
    - Store counts
    
    Args:
        wb: xlsxwriter workbook object
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    ws = wb.add_worksheet('Regional Summary')
    
    # Set column widths
    ws.set_column('A:A', 10)   # Region
    ws.set_column('B:B', 12)   # Date
    ws.set_column('C:C', 10)   # Day
    ws.set_column('D:E', 8)    # Stores
    ws.set_column('F:F', 8)    # Items
    ws.set_column('G:H', 14)   # Forecast Shipped/Sales
    ws.set_column('I:J', 14)   # LW Shipped/Sold
    ws.set_column('K:L', 12)   # Delta %
    ws.set_column('M:N', 12)   # Shrink %
    
    # Title
    ws.merge_range('A1:N1', 'Executive Summary - Regional Overview', formats['title'])
    ws.merge_range('A2:N2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 28)
    ws.set_row(1, 22)
    
    # Headers
    headers = [
        'Region', 'Date', 'Day', 
        'Active Stores', 'LW Stores', 'Items',
        'Forecast Shipped', 'Forecast Sales',
        'LW Shipped', 'LW Sold',
        'Δ Shipped %', 'Δ Sales %',
        'Exp Shrink %', 'LW Shrink %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['header_primary'])
    ws.set_row(3, 35)
    
    # Get data
    query = get_regional_summary_query(regions, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting regional summary: {e}")
        data = []
    
    # Track totals for grand total row
    totals = {
        'active_stores': 0, 'lw_stores': 0, 'item_count': 0,
        'forecast_shipped': 0, 'forecast_sales': 0,
        'lw_shipped': 0, 'lw_sold': 0,
    }
    
    # Write data rows
    row = 4
    current_region = None
    
    for d in data:
        region_code = d.get('region_code')
        
        # Add region separator if region changes
        if region_code != current_region and current_region is not None:
            row += 1  # Empty row between regions
        current_region = region_code
        
        col = 0
        
        # Region
        ws.write(row, col, region_code, formats['text_center'])
        col += 1
        
        # Date
        ws.write(row, col, d.get('date_forecast'), formats['date'])
        col += 1
        
        # Day
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        
        # Store counts
        active_stores = d.get('active_stores') or 0
        lw_stores = d.get('lw_stores') or 0
        ws.write(row, col, active_stores, formats['number'])
        col += 1
        ws.write(row, col, lw_stores, formats['number'])
        col += 1
        
        # Items
        item_count = d.get('item_count') or 0
        ws.write(row, col, item_count, formats['number'])
        col += 1
        
        # Forecast metrics
        forecast_shipped = d.get('forecast_shipped') or 0
        forecast_sales = d.get('forecast_sales') or 0
        ws.write(row, col, forecast_shipped, formats['number'])
        col += 1
        ws.write(row, col, forecast_sales, formats['number'])
        col += 1
        
        # Last week metrics
        lw_shipped = d.get('lw_shipped') or 0
        lw_sold = d.get('lw_sold') or 0
        ws.write(row, col, lw_shipped, formats['number'])
        col += 1
        ws.write(row, col, lw_sold, formats['number'])
        col += 1
        
        # Delta percentages
        delta_shipped_pct = d.get('delta_shipped_pct') or 0
        delta_sales_pct = d.get('delta_sales_pct') or 0
        ws.write(row, col, delta_shipped_pct, get_delta_format(formats, delta_shipped_pct))
        col += 1
        ws.write(row, col, delta_sales_pct, get_delta_format(formats, delta_sales_pct))
        col += 1
        
        # Shrink percentages
        exp_shrink_pct = d.get('expected_shrink_pct') or 0
        lw_shrink_pct = d.get('lw_shrink_pct') or 0
        ws.write(row, col, exp_shrink_pct, get_shrink_format(formats, exp_shrink_pct))
        col += 1
        ws.write(row, col, lw_shrink_pct, get_shrink_format(formats, lw_shrink_pct))
        
        # Accumulate totals
        totals['active_stores'] += active_stores
        totals['lw_stores'] += lw_stores
        totals['item_count'] += item_count
        totals['forecast_shipped'] += forecast_shipped
        totals['forecast_sales'] += forecast_sales
        totals['lw_shipped'] += lw_shipped
        totals['lw_sold'] += lw_sold
        
        row += 1
    
    # Grand total row
    row += 1
    ws.write(row, 0, 'TOTAL', formats['total_label'])
    ws.write(row, 1, '', formats['total_label'])
    ws.write(row, 2, '', formats['total_label'])
    ws.write(row, 3, '', formats['total_number'])  # Don't sum stores (would double count)
    ws.write(row, 4, '', formats['total_number'])
    ws.write(row, 5, '', formats['total_number'])  # Don't sum items (would double count)
    ws.write(row, 6, totals['forecast_shipped'], formats['total_number'])
    ws.write(row, 7, totals['forecast_sales'], formats['total_number'])
    ws.write(row, 8, totals['lw_shipped'], formats['total_number'])
    ws.write(row, 9, totals['lw_sold'], formats['total_number'])
    
    # Calculate overall deltas
    total_delta_shipped_pct = (totals['forecast_shipped'] - totals['lw_shipped']) / totals['lw_shipped'] if totals['lw_shipped'] > 0 else 0
    total_delta_sales_pct = (totals['forecast_sales'] - totals['lw_sold']) / totals['lw_sold'] if totals['lw_sold'] > 0 else 0
    total_exp_shrink = (totals['forecast_shipped'] - totals['forecast_sales']) / totals['forecast_shipped'] if totals['forecast_shipped'] > 0 else 0
    total_lw_shrink = (totals['lw_shipped'] - totals['lw_sold']) / totals['lw_shipped'] if totals['lw_shipped'] > 0 else 0
    
    ws.write(row, 10, total_delta_shipped_pct, formats['total_percent'])
    ws.write(row, 11, total_delta_sales_pct, formats['total_percent'])
    ws.write(row, 12, total_exp_shrink, formats['total_percent'])
    ws.write(row, 13, total_lw_shrink, formats['total_percent'])
    
    # Freeze panes
    ws.freeze_panes(4, 1)


def write_waterfall_sheet(wb, conn, regions: list,
                          start_date: str, end_date: str,
                          formats: dict):
    """
    Create the Waterfall Analysis worksheet.
    
    Shows the adjustment flow from last week SALES to final forecast with detailed
    component breakdowns. Uses pre-calculated values from waterfall_aggregate table.
    
    No icons used - professional presentation with clear metrics.
    Separates rounding into up/down, store pass into growth/decline.
    
    Args:
        wb: xlsxwriter workbook object
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    ws = wb.add_worksheet('Waterfall Analysis')
    
    # Set column widths
    ws.set_column('A:A', 10)   # Region
    ws.set_column('B:B', 12)   # Date
    ws.set_column('C:C', 10)   # Day
    ws.set_column('D:D', 35)   # Component
    ws.set_column('E:E', 14)   # Quantity
    ws.set_column('F:F', 12)   # % of LW Sales
    ws.set_column('G:G', 12)   # Items/Stores
    ws.set_column('H:H', 55)   # Notes/Remarks
    
    # Title
    ws.merge_range('A1:H1', 'Waterfall Analysis - Forecast Adjustments', formats['title'])
    ws.merge_range('A2:H2', f'Forecast Period: {start_date} to {end_date} | Starting from LW Sales to Final Forecast', formats['subtitle'])
    ws.set_row(0, 28)
    ws.set_row(1, 22)
    
    # Headers
    headers = ['Region', 'Date', 'Day', 'Component', 'Qty', '% of LW Sales', 'Items/Stores', 'Remarks']
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['header_primary'])
    ws.set_row(3, 30)
    
    # Get data from waterfall_aggregate table
    query = get_waterfall_components_query(regions, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting waterfall data: {e}")
        data = []
    
    # Write data - one section per region/date
    row = 4
    
    for d in data:
        region = d.get('region_code')
        date_val = d.get('date_forecast')
        day_name = d.get('day_name')
        
        store_count = d.get('store_count') or 0
        item_count = d.get('item_count') or 0
        line_count = d.get('line_count') or 0
        
        lw_shipped = d.get('lw_shipped') or 0
        lw_sold = d.get('lw_sold') or 0
        final_forecast = d.get('final_forecast_qty') or 0
        
        # Get baseline source breakdown
        baseline_lw_count = d.get('baseline_lw_sales_count') or 0
        baseline_ema_count = d.get('baseline_ema_count') or 0
        baseline_avg_count = d.get('baseline_avg_count') or 0
        baseline_min_count = d.get('baseline_min_case_count') or 0
        
        # Get adjustment values
        baseline_uplift = d.get('baseline_uplift_qty') or 0
        baseline_uplift_count = d.get('baseline_uplift_count') or 0
        
        ema_uplift = d.get('ema_uplift_qty') or 0
        ema_uplift_count = d.get('ema_uplift_count') or 0
        
        decline_adj = d.get('decline_adj_qty') or 0
        decline_adj_count = d.get('decline_adj_count') or 0
        
        high_shrink_adj = d.get('high_shrink_adj_qty') or 0
        high_shrink_adj_count = d.get('high_shrink_adj_count') or 0
        
        base_cover_total = d.get('base_cover_total_qty') or 0
        base_cover_default = d.get('base_cover_default_qty') or 0
        base_cover_default_count = d.get('base_cover_default_count') or 0
        base_cover_soldout = d.get('base_cover_soldout_qty') or 0
        base_cover_soldout_count = d.get('base_cover_soldout_count') or 0
        
        rounding_up = d.get('rounding_up_qty') or 0
        rounding_up_count = d.get('rounding_up_count') or 0
        rounding_down = d.get('rounding_down_qty') or 0
        rounding_down_count = d.get('rounding_down_count') or 0
        rounding_net = d.get('rounding_net_qty') or 0
        
        safety_stock = d.get('safety_stock_qty') or 0
        safety_stock_count = d.get('safety_stock_count') or 0
        
        store_pass_decline = d.get('store_pass_decline_qty') or 0
        store_pass_decline_count = d.get('store_pass_decline_count') or 0
        store_pass_growth = d.get('store_pass_growth_qty') or 0
        store_pass_growth_count = d.get('store_pass_growth_count') or 0
        store_pass_stores = d.get('store_pass_stores_adjusted') or 0
        
        weather_adj = d.get('weather_adj_qty') or 0
        weather_adj_count = d.get('weather_adj_count') or 0
        
        # Get new adjustment type values
        promo_adj = d.get('promo_adj_qty') or 0
        promo_adj_count = d.get('promo_adj_count') or 0
        
        holiday_increase_adj = d.get('holiday_increase_adj_qty') or 0
        holiday_increase_adj_count = d.get('holiday_increase_adj_count') or 0
        
        cannibalism_adj = d.get('cannibalism_adj_qty') or 0
        cannibalism_adj_count = d.get('cannibalism_adj_count') or 0
        
        adhoc_increase_adj = d.get('adhoc_increase_adj_qty') or 0
        adhoc_increase_adj_count = d.get('adhoc_increase_adj_count') or 0
        
        adhoc_decrease_adj = d.get('adhoc_decrease_adj_qty') or 0
        adhoc_decrease_adj_count = d.get('adhoc_decrease_adj_count') or 0
        
        store_specific_adj = d.get('store_specific_adj_qty') or 0
        store_specific_adj_count = d.get('store_specific_adj_count') or 0
        
        item_specific_adj = d.get('item_specific_adj_qty') or 0
        item_specific_adj_count = d.get('item_specific_adj_count') or 0
        
        regional_adj = d.get('regional_adj_qty') or 0
        regional_adj_count = d.get('regional_adj_count') or 0
        
        delta_pct = d.get('delta_from_lw_sales_pct') or 0
        
        # Build baseline source description
        baseline_sources = []
        if baseline_lw_count > 0:
            baseline_sources.append(f"LW Sales: {baseline_lw_count}")
        if baseline_ema_count > 0:
            baseline_sources.append(f"EMA uplift: {baseline_ema_count}")
        if baseline_avg_count > 0:
            baseline_sources.append(f"Average: {baseline_avg_count}")
        if baseline_min_count > 0:
            baseline_sources.append(f"Minimum case: {baseline_min_count}")
        baseline_breakdown = " | ".join(baseline_sources)
        
        # Define waterfall components with detailed breakdown
        components = [
            {
                'name': 'Starting Point: LW Sales',
                'qty': lw_sold,
                'items_stores': f'{store_count} stores',
                'note': f'Baseline: {lw_sold:,.0f} units sold LW (LW Shipped: {lw_shipped:,.0f}). Source: {baseline_breakdown}',
                'is_start': True,
            },
            {
                'name': 'Baseline Uplift',
                'qty': baseline_uplift,
                'items_stores': f'{baseline_uplift_count} items',
                'note': f'Total uplift from LW sold to baseline (includes EMA: {ema_uplift_count} items, Average, Min case logic)',
                'is_adjustment': True,
            },
            {
                'name': 'Decline Pattern Adj',
                'qty': decline_adj,
                'items_stores': f'{decline_adj_count} items',
                'note': 'Items with WoW decline - use older week average to avoid over-correction',
                'is_adjustment': True,
            },
            {
                'name': 'High Shrink Adj',
                'qty': high_shrink_adj,  # Already negative
                'items_stores': f'{high_shrink_adj_count} items',
                'note': 'Items with consecutive high shrink - conservative forecast to reduce waste',
                'is_adjustment': True,
            },
            {
                'name': 'Base Cover (Default)',
                'qty': base_cover_default,
                'items_stores': f'{base_cover_default_count} items',
                'note': 'Standard coverage buffer for demand variability (5% of baseline)',
                'is_adjustment': True,
            },
            {
                'name': 'Base Cover (Sold-Out)',
                'qty': base_cover_soldout,
                'items_stores': f'{base_cover_soldout_count} items',
                'note': 'Additional cover for items sold out LW (6% of baseline)',
                'is_adjustment': True,
            },
            {
                'name': 'Safety Stock',
                'qty': safety_stock,
                'items_stores': f'{safety_stock_count} items',
                'note': 'Extra case added for declining hero items with volatility',
                'is_adjustment': True,
            },
            {
                'name': 'Rounding (Case Pack)',
                'qty': rounding_net,
                'items_stores': f'{rounding_up_count}↑ {rounding_down_count}↓',
                'note': f'Net rounding to full case packs (Up: +{rounding_up:.0f}, Down: -{rounding_down:.0f})',
                'is_adjustment': True,
            },
            {
                'name': 'Store Pass (Decline)',
                'qty': store_pass_decline,  # Already negative in aggregate
                'items_stores': f'{store_pass_stores} stores',
                'note': 'Store-level shrink control - reduces forecast for high-shrink stores',
                'is_adjustment': True,
            },
            {
                'name': 'Store Pass (Growth)',
                'qty': store_pass_growth,
                'items_stores': f'{store_pass_growth_count} items',
                'note': 'Store-level coverage add - ensures minimum coverage',
                'is_adjustment': True,
            },
            # ===== New Adjustment Types =====
            {
                'name': 'Promotional Adj',
                'qty': promo_adj,
                'items_stores': f'{promo_adj_count} items',
                'note': 'Promotional uplift for sales events',
                'is_adjustment': True,
            },
            {
                'name': 'Holiday Increase',
                'qty': holiday_increase_adj,
                'items_stores': f'{holiday_increase_adj_count} items',
                'note': 'Holiday-related demand increase',
                'is_adjustment': True,
            },
            {
                'name': 'Cannibalism Adj',
                'qty': cannibalism_adj,  # Typically negative
                'items_stores': f'{cannibalism_adj_count} items',
                'note': 'Demand reduction due to competing products',
                'is_adjustment': True,
            },
            {
                'name': 'Adhoc Increase',
                'qty': adhoc_increase_adj,
                'items_stores': f'{adhoc_increase_adj_count} items',
                'note': 'One-time temporary demand increase',
                'is_adjustment': True,
            },
            {
                'name': 'Adhoc Decrease',
                'qty': adhoc_decrease_adj,  # Typically negative
                'items_stores': f'{adhoc_decrease_adj_count} items',
                'note': 'One-time temporary demand decrease',
                'is_adjustment': True,
            },
            {
                'name': 'Store Specific Adj',
                'qty': store_specific_adj,
                'items_stores': f'{store_specific_adj_count} items',
                'note': 'Store-level specific adjustments',
                'is_adjustment': True,
            },
            {
                'name': 'Item Specific Adj',
                'qty': item_specific_adj,
                'items_stores': f'{item_specific_adj_count} items',
                'note': 'Item-level specific adjustments',
                'is_adjustment': True,
            },
            {
                'name': 'Regional Adj',
                'qty': regional_adj,
                'items_stores': f'{regional_adj_count} items',
                'note': 'Region-wide adjustments',
                'is_adjustment': True,
            },
            {
                'name': 'Weather Adjustment',
                'qty': weather_adj,  # Already negative from aggregate
                'items_stores': f'{weather_adj_count} items',
                'note': 'Weather-based quantity adjustment (reduces for severe weather)',
                'is_adjustment': True,
            },
            {
                'name': 'Final Forecast',
                'qty': final_forecast,
                'items_stores': f'{item_count} items',
                'note': f'Final: {final_forecast:,.0f} units ({delta_pct:+.1%} vs LW Sales)',
                'is_final': True,
            },
        ]
        
        # Write each component row
        for comp in components:
            qty = comp['qty']
            
            # Skip zero adjustments (but always show start and final)
            if not comp.get('is_start') and not comp.get('is_final') and (qty == 0 or qty is None):
                continue
            
            # Region, Date, Day (only on first row of group)
            if comp.get('is_start'):
                ws.write(row, 0, region, formats['region_header'])
                ws.write(row, 1, date_val, formats['date'])
                ws.write(row, 2, day_name, formats['text_center'])
            else:
                ws.write(row, 0, '', formats['text_center'])
                ws.write(row, 1, '', formats['text_center'])
                ws.write(row, 2, '', formats['text_center'])
            
            # Component name (no icons)
            ws.write(row, 3, comp['name'], formats['text_left'])
            
            # Quantity with appropriate formatting
            if comp.get('is_start'):
                ws.write(row, 4, qty, formats['waterfall_start'])
            elif comp.get('is_final'):
                ws.write(row, 4, qty, formats['waterfall_final'])
            else:
                ws.write(row, 4, qty, get_waterfall_format(formats, qty))
            
            # Percentage of LW Sales
            pct_of_lw = qty / lw_sold if lw_sold > 0 else 0
            if comp.get('is_start') or comp.get('is_final'):
                ws.write(row, 5, '', formats['text_center'])
            else:
                ws.write(row, 5, pct_of_lw, formats['percent'])
            
            # Items/Stores count
            ws.write(row, 6, comp['items_stores'], formats['text_center'])
            
            # Remarks
            ws.write(row, 7, comp['note'], formats['text_left'])
            
            row += 1
        
        # Add separator row
        row += 1
    
    # Freeze panes
    ws.freeze_panes(4, 3)


def write_waterfall_columnar_sheet(wb, conn, regions: list,
                                    start_date: str, end_date: str,
                                    formats: dict):
    """
    Create a columnar Waterfall Summary worksheet.
    
    More compact view with regions as rows and components as columns.
    Shows qty (%) in each cell for quick scanning.
    Starting point is LW Sales (Sold), not LW Shipped.
    
    Args:
        wb: xlsxwriter workbook object
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    ws = wb.add_worksheet('Waterfall Summary')
    
    # Set column widths for detailed waterfall
    ws.set_column('A:A', 10)   # Region
    ws.set_column('B:B', 12)   # Date
    ws.set_column('C:C', 12)   # LW Sales
    ws.set_column('D:D', 12)   # Baseline Uplift
    ws.set_column('E:E', 11)   # Decline Adj
    ws.set_column('F:F', 11)   # High Shrink
    ws.set_column('G:G', 12)   # Cover Default
    ws.set_column('H:H', 12)   # Cover Sold-Out
    ws.set_column('I:I', 11)   # Safety Stock
    ws.set_column('J:J', 11)   # Rounding
    ws.set_column('K:K', 11)   # Store Pass
    ws.set_column('L:L', 11)   # Promo
    ws.set_column('M:M', 11)   # Holiday
    ws.set_column('N:N', 11)   # Cannibalism
    ws.set_column('O:O', 11)   # Weather
    ws.set_column('P:P', 12)   # Final Fcst
    ws.set_column('Q:Q', 10)   # Δ %
    
    # Title
    ws.merge_range('A1:Q1', 'Waterfall Summary - Detailed Component Breakdown', formats['title'])
    ws.merge_range('A2:Q2', f'Forecast Period: {start_date} to {end_date} | Values shown as Qty (% of LW Sales)', formats['subtitle'])
    ws.set_row(0, 28)
    ws.set_row(1, 22)
    
    # Headers - All waterfall components
    headers = [
        'Region', 'Date', 
        'LW Sales',
        'Baseline Up', 'Decline', 'Hi Shrink',
        'Cover Std', 'Cover SO',
        'Safety', 'Rounding',
        'Store Pass',
        'Promo', 'Holiday', 'Cannibal',
        'Weather',
        'Final', 'Δ %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['header_primary'])
    ws.set_row(3, 35)
    
    # Get data
    query = get_waterfall_components_query(regions, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting waterfall data: {e}")
        data = []
    
    # Write data rows
    row = 4
    
    for d in data:
        region = d.get('region_code')
        date_val = d.get('date_forecast')
        
        lw_sold = d.get('lw_sold') or 0
        final_forecast = d.get('final_forecast_qty') or 0
        delta_pct = d.get('delta_from_lw_sales_pct') or 0
        
        # Get all component values (already properly signed in aggregate)
        baseline_uplift = d.get('baseline_uplift_qty') or 0
        decline_adj = d.get('decline_adj_qty') or 0
        high_shrink = d.get('high_shrink_adj_qty') or 0
        cover_default = d.get('base_cover_default_qty') or 0
        cover_soldout = d.get('base_cover_soldout_qty') or 0
        safety_stock = d.get('safety_stock_qty') or 0
        rounding_net = d.get('rounding_net_qty') or 0
        store_pass = (d.get('store_pass_decline_qty') or 0) + (d.get('store_pass_growth_qty') or 0)
        
        # New adjustment types
        promo_adj = d.get('promo_adj_qty') or 0
        holiday_adj = d.get('holiday_increase_adj_qty') or 0
        cannibalism_adj = d.get('cannibalism_adj_qty') or 0
        
        weather = d.get('weather_adj_qty') or 0
        
        col = 0
        
        # Region, Date
        ws.write(row, col, region, formats['text_center'])
        col += 1
        ws.write(row, col, date_val, formats['date'])
        col += 1
        
        # LW Sales (Starting Point)
        ws.write(row, col, lw_sold, formats['waterfall_start'])
        col += 1
        
        # All adjustments with percentage of LW Sales
        adjustments = [
            baseline_uplift,
            decline_adj,
            high_shrink,
            cover_default,
            cover_soldout,
            safety_stock,
            rounding_net,
            store_pass,
            promo_adj,
            holiday_adj,
            cannibalism_adj,
            weather,
        ]
        
        for adj_qty in adjustments:
            pct = adj_qty / lw_sold if lw_sold > 0 else 0
            cell_text = format_qty_with_pct(adj_qty, pct)
            ws.write(row, col, cell_text, formats['qty_pct_cell'])
            col += 1
        
        # Final forecast
        ws.write(row, col, final_forecast, formats['waterfall_final'])
        col += 1
        
        # Total delta % from LW Sales (not shipped)
        total_delta_pct = (final_forecast - lw_sold) / lw_sold if lw_sold > 0 else 0
        ws.write(row, col, total_delta_pct, get_delta_format(formats, total_delta_pct))
        
        row += 1
    
    # Freeze panes
    ws.freeze_panes(4, 2)


def write_weather_summary_sheet(wb, conn, regions: list,
                                 start_date: str, end_date: str,
                                 formats: dict):
    """
    Create the Weather Impact Summary worksheet with comprehensive weather metrics.
    
    Shows by region/date:
    - Store counts by severity category
    - Key weather variables (rain, snow, wind, visibility)
    - Weather-adjusted item count
    - Quantity adjustment from weather
    
    Args:
        wb: xlsxwriter workbook object
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    ws = wb.add_worksheet('Weather Impact')
    
    # Set column widths - expanded for more weather data
    ws.set_column('A:A', 8)    # Region
    ws.set_column('B:B', 11)   # Date
    ws.set_column('C:C', 10)   # Day
    ws.set_column('D:H', 7)    # Severity store counts
    ws.set_column('I:I', 8)    # Total Stores
    ws.set_column('J:K', 8)    # Avg/Max Severity
    ws.set_column('L:M', 8)    # Impact factors
    ws.set_column('N:O', 6)    # Rain
    ws.set_column('P:Q', 6)    # Snow
    ws.set_column('R:S', 6)    # Snow Depth
    ws.set_column('T:U', 6)    # Wind
    ws.set_column('V:W', 6)    # Visibility
    ws.set_column('X:Y', 6)    # Temp range
    ws.set_column('Z:Z', 10)   # Items Adjusted
    ws.set_column('AA:AA', 10) # Weather Adj
    ws.set_column('AB:AB', 8)  # Reduction %
    ws.set_column('AC:AC', 16) # Primary Condition
    
    # Title
    ws.merge_range('A1:AC1', 'Weather Impact Summary - All Weather Variables', formats['title'])
    ws.merge_range('A2:AC2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 28)
    ws.set_row(1, 22)
    
    # Headers - two rows for grouping
    # Row 3 - Group headers
    ws.merge_range('D3:H3', 'Severity Distribution', formats['header_secondary'])
    ws.merge_range('J3:M3', 'Severity Metrics', formats['header_secondary'])
    ws.merge_range('N3:Y3', 'Weather Variables', formats['header_secondary'])
    ws.merge_range('Z3:AB3', 'Adjustment Impact', formats['header_secondary'])
    
    # Row 4 - Detail headers
    headers = [
        'Region', 'Date', 'Day',
        '🔴Sev', '🟠High', '🟡Mod', '🟢Low', '✅Min',
        'Stores', 'Avg', 'Max', 'AvgImp', 'MinImp',
        'Rain"', 'MaxR', 'Snow"', 'MaxS', 'Depth', 'MaxD',
        'Wind', 'Gust', 'AvgVis', 'MinVis',
        'Lo°F', 'Hi°F',
        'Items Adj',
        'Qty Adj', 'Red%',
        'Primary Condition'
    ]
    
    for col, header in enumerate(headers):
        ws.write(4, col, header, formats['header_primary'])
    ws.set_row(4, 35)
    
    # Get data
    query = get_weather_summary_query(regions, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting weather summary: {e}")
        data = []
    
    # Write data rows
    row = 5
    
    for d in data:
        col = 0
        
        # Region, Date, Day
        ws.write(row, col, d.get('region_code'), formats['text_center'])
        col += 1
        ws.write(row, col, d.get('date_forecast'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        
        # Severity store counts with conditional formatting
        severe = d.get('severe_stores') or 0
        high = d.get('high_stores') or 0
        moderate = d.get('moderate_stores') or 0
        low = d.get('low_stores') or 0
        minimal = d.get('minimal_stores') or 0
        
        ws.write(row, col, severe, formats['severity_severe'] if severe > 0 else formats['number'])
        col += 1
        ws.write(row, col, high, formats['severity_high'] if high > 0 else formats['number'])
        col += 1
        ws.write(row, col, moderate, formats['severity_moderate'] if moderate > 0 else formats['number'])
        col += 1
        ws.write(row, col, low, formats['severity_low'] if low > 0 else formats['number'])
        col += 1
        ws.write(row, col, minimal, formats['severity_minimal'] if minimal > 0 else formats['number'])
        col += 1
        
        # Total stores
        ws.write(row, col, d.get('total_stores') or 0, formats['number'])
        col += 1
        
        # Severity metrics
        avg_severity = d.get('avg_severity_score') or 0
        max_severity = d.get('max_severity_score') or 0
        ws.write(row, col, avg_severity, formats['decimal'])
        col += 1
        ws.write(row, col, max_severity, formats['decimal'])
        col += 1
        ws.write(row, col, d.get('avg_impact_factor'), formats['decimal3'] if 'decimal3' in formats else formats['decimal'])
        col += 1
        ws.write(row, col, d.get('min_impact_factor'), formats['decimal3'] if 'decimal3' in formats else formats['decimal'])
        col += 1
        
        # Weather variables - Rain
        ws.write(row, col, d.get('avg_rain'), formats['decimal'])
        col += 1
        ws.write(row, col, d.get('max_rain'), formats['decimal'])
        col += 1
        
        # Snow
        ws.write(row, col, d.get('avg_snow'), formats['decimal'])
        col += 1
        ws.write(row, col, d.get('max_snow'), formats['decimal'])
        col += 1
        
        # Snow Depth
        ws.write(row, col, d.get('avg_snow_depth'), formats['decimal'])
        col += 1
        ws.write(row, col, d.get('max_snow_depth'), formats['decimal'])
        col += 1
        
        # Wind
        ws.write(row, col, d.get('avg_wind'), formats['decimal'])
        col += 1
        ws.write(row, col, d.get('max_wind_gust'), formats['decimal'])
        col += 1
        
        # Visibility
        ws.write(row, col, d.get('avg_visibility'), formats['decimal'])
        col += 1
        ws.write(row, col, d.get('min_visibility'), formats['decimal'])
        col += 1
        
        # Temperature
        ws.write(row, col, d.get('avg_temp_min'), formats['decimal'])
        col += 1
        ws.write(row, col, d.get('avg_temp_max'), formats['decimal'])
        col += 1
        
        # Items adjusted
        items_adjusted = d.get('items_adjusted') or 0
        total_items = d.get('total_items') or 0
        ws.write(row, col, f"{items_adjusted}/{total_items}", formats['text_center'])
        col += 1
        
        # Quantities
        weather_adj = d.get('weather_adj_qty') or 0
        reduction_pct = d.get('weather_reduction_pct') or 0
        
        ws.write(row, col, weather_adj, formats['number'] if weather_adj >= 0 else formats['waterfall_decrease'])
        col += 1
        ws.write(row, col, reduction_pct, formats['percent'])
        col += 1
        
        # Primary condition
        ws.write(row, col, d.get('primary_condition') or 'Normal', formats['text_left'])
        
        row += 1
    
    # Add legend
    row += 2
    ws.write(row, 0, 'Severity Legend:', formats['section_title'])
    row += 1
    
    legend_items = [
        ('🔴 Severe (7+)', 'Major weather event - significant forecast reduction'),
        ('🟠 High (5-7)', 'Poor conditions - moderate forecast reduction'),
        ('🟡 Moderate (3-5)', 'Unfavorable conditions - slight forecast reduction'),
        ('🟢 Low (1.5-3)', 'Minor weather impact - minimal adjustment'),
        ('✅ Minimal (0-1.5)', 'Normal conditions - no adjustment'),
    ]
    
    for icon_text, description in legend_items:
        ws.write(row, 0, icon_text, formats['text_left'])
        ws.merge_range(row, 1, row, 5, description, formats['text_left'])
        row += 1
    
    # Add weather variable legend
    row += 2
    ws.write(row, 0, 'Weather Variables:', formats['section_title'])
    row += 1
    
    variable_legend = [
        ('Rain"/MaxR', 'Average and maximum rainfall in inches'),
        ('Snow"/MaxS', 'Average and maximum new snowfall in inches'),
        ('Depth/MaxD', 'Average and maximum snow depth accumulation'),
        ('Wind/Gust', 'Average wind speed and maximum wind gust (mph)'),
        ('AvgVis/MinVis', 'Average and minimum visibility (miles)'),
        ('Lo°F/Hi°F', 'Average low and high temperatures'),
        ('AvgImp/MinImp', 'Impact factor (1.0=no impact, lower=more reduction)'),
    ]
    
    for var, desc in variable_legend:
        ws.write(row, 0, var, formats['header_primary'] if 'header_primary' in formats else formats['text_left'])
        ws.merge_range(row, 1, row, 6, desc, formats['text_left'])
        row += 1
    
    # Freeze panes
    ws.freeze_panes(5, 3)


def write_daily_totals_sheet(wb, conn, regions: list,
                              start_date: str, end_date: str,
                              formats: dict):
    """
    Create a Daily Totals summary worksheet.
    
    Company-wide totals across all regions by date.
    
    Args:
        wb: xlsxwriter workbook object
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date string
        end_date: End date string
        formats: Dictionary of format objects
    """
    ws = wb.add_worksheet('Daily Totals')
    
    # Set column widths
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:C', 10)   # Regions
    ws.set_column('D:D', 10)   # Stores
    ws.set_column('E:E', 10)   # Items
    ws.set_column('F:G', 14)   # Forecast qty
    ws.set_column('H:I', 14)   # LW qty
    ws.set_column('J:K', 14)   # Adjustments
    ws.set_column('L:M', 12)   # Percentages
    
    # Title
    ws.merge_range('A1:M1', 'Company-Wide Daily Totals', formats['title'])
    ws.merge_range('A2:M2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 28)
    ws.set_row(1, 22)
    
    # Headers
    headers = [
        'Date', 'Day', 'Regions', 'Stores', 'Items',
        'Fcst Shipped', 'Fcst Sales',
        'LW Shipped', 'LW Sold',
        'Weather Adj', 'Store Adj',
        'Δ from LW %', 'Exp Shrink %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['header_primary'])
    ws.set_row(3, 35)
    
    # Get data
    query = get_all_regions_total_query(regions, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting daily totals: {e}")
        data = []
    
    # Track grand totals
    grand_totals = {
        'stores': 0, 'items': 0,
        'forecast_shipped': 0, 'forecast_sales': 0,
        'lw_shipped': 0, 'lw_sold': 0,
        'weather_adj': 0, 'store_adj': 0,
    }
    
    # Write data rows
    row = 4
    
    for d in data:
        col = 0
        
        # Date, Day
        ws.write(row, col, d.get('date_forecast'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        
        # Counts
        ws.write(row, col, d.get('region_count') or 0, formats['number'])
        col += 1
        
        stores = d.get('total_stores') or 0
        ws.write(row, col, stores, formats['number'])
        col += 1
        
        items = d.get('total_items') or 0
        ws.write(row, col, items, formats['number'])
        col += 1
        
        # Forecast quantities
        fcst_shipped = d.get('total_forecast_shipped') or 0
        fcst_sales = d.get('total_forecast_sales') or 0
        ws.write(row, col, fcst_shipped, formats['number'])
        col += 1
        ws.write(row, col, fcst_sales, formats['number'])
        col += 1
        
        # LW quantities
        lw_shipped = d.get('total_lw_shipped') or 0
        lw_sold = d.get('total_lw_sold') or 0
        ws.write(row, col, lw_shipped, formats['number'])
        col += 1
        ws.write(row, col, lw_sold, formats['number'])
        col += 1
        
        # Adjustments
        weather_adj = d.get('total_weather_adj') or 0
        store_adj = d.get('total_store_adj') or 0
        ws.write(row, col, weather_adj, formats['number'])
        col += 1
        ws.write(row, col, store_adj, formats['number'])
        col += 1
        
        # Percentages
        delta_pct = d.get('delta_from_lw_pct') or 0
        shrink_pct = d.get('expected_shrink_pct') or 0
        ws.write(row, col, delta_pct, get_delta_format(formats, delta_pct))
        col += 1
        ws.write(row, col, shrink_pct, get_shrink_format(formats, shrink_pct))
        
        # Accumulate totals
        grand_totals['stores'] = max(grand_totals['stores'], stores)  # Max, not sum
        grand_totals['items'] = max(grand_totals['items'], items)
        grand_totals['forecast_shipped'] += fcst_shipped
        grand_totals['forecast_sales'] += fcst_sales
        grand_totals['lw_shipped'] += lw_shipped
        grand_totals['lw_sold'] += lw_sold
        grand_totals['weather_adj'] += weather_adj
        grand_totals['store_adj'] += store_adj
        
        row += 1
    
    # Grand total row
    row += 1
    ws.write(row, 0, 'TOTAL', formats['total_label'])
    for c in range(1, 5):
        ws.write(row, c, '', formats['total_label'])
    
    ws.write(row, 5, grand_totals['forecast_shipped'], formats['total_number'])
    ws.write(row, 6, grand_totals['forecast_sales'], formats['total_number'])
    ws.write(row, 7, grand_totals['lw_shipped'], formats['total_number'])
    ws.write(row, 8, grand_totals['lw_sold'], formats['total_number'])
    ws.write(row, 9, grand_totals['weather_adj'], formats['total_number'])
    ws.write(row, 10, grand_totals['store_adj'], formats['total_number'])
    
    # Calculate grand total percentages
    total_delta = (grand_totals['forecast_shipped'] - grand_totals['lw_shipped']) / grand_totals['lw_shipped'] if grand_totals['lw_shipped'] > 0 else 0
    total_shrink = (grand_totals['forecast_shipped'] - grand_totals['forecast_sales']) / grand_totals['forecast_shipped'] if grand_totals['forecast_shipped'] > 0 else 0
    
    ws.write(row, 11, total_delta, formats['total_percent'])
    ws.write(row, 12, total_shrink, formats['total_percent'])
    
    # Freeze panes
    ws.freeze_panes(4, 2)
