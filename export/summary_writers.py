"""
Summary Worksheet Writers Module
================================
Worksheet writing functions for regional summary reports.

This module contains all worksheet creation functions used for 
generating the multi-sheet Excel summary reports.
"""

import polars as pl

from .summary_queries import (
    get_daily_summary_query,
    get_store_summary_query,
    get_item_summary_query,
    get_item_detail_query,
    get_weather_summary_by_date_query,
    get_weather_store_detail_query
)
from .summary_formatting import (
    WEATHER_ICONS,
    SEVERITY_ICONS,
    get_weather_indicator_icon,
    get_severity_format,
    get_shrink_pct_format,
    get_growth_pct_format,
    build_sales_trend_string
)


def write_daily_summary_sheet(wb, conn, region: str, 
                               start_date: str, end_date: str,
                               formats: dict):
    """
    Create the Daily Summary worksheet with trends, growth %, expected shrink, and weather.
    """
    ws = wb.add_worksheet('Daily Summary')
    
    # Set column widths
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:E', 8)    # Stores, Items, Lines
    ws.set_column('F:K', 12)   # Forecast quantities
    ws.set_column('L:L', 14)   # Fcst Avg
    ws.set_column('M:M', 26)   # Shipped Trend
    ws.set_column('N:N', 26)   # Sold Trend
    ws.set_column('O:P', 11)   # Growth %
    ws.set_column('Q:T', 11)   # Expected Shrink columns
    ws.set_column('U:Y', 8)    # Weather severity counts
    ws.set_column('Z:Z', 11)   # Avg Weather
    ws.set_column('AA:AB', 12) # Delta, Delta %
    
    # Title
    ws.merge_range('A1:AB1', f'Daily Forecast Summary - Region {region}', formats['title'])
    ws.merge_range('A2:AB2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Column headers
    headers = [
        'Forecast Date', 'Day', 'Stores', 'Items', 'Lines',
        'Fcst (Pre Store Adj)', 'Store Adj Qty', 'Fcst (Pre Weather)', 
        'Weather Adj', 'Fcst Final',
        'Fcst Avg (Exp Sales)',
        'Shipped Trend (W4>W3>W2>W1)', 'Sold Trend (W4>W3>W2>W1)',
        'Growth vs W1 %', 'Growth vs W2 %',
        'Exp Shrink (Avg) %', 'Exp Shrink (LW) %', 'Exp Shrink (2W) %', 'LW Shrink %',
        '🔴 Severe', '🟠 High', '🟡 Moderate', '🟢 Low', '✅ Minimal',
        'Avg Weather', 'Weather Adj Items',
        'Delta from LW', 'Delta LW %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Get data
    query = get_daily_summary_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting daily summary: {e}")
        data = []
    
    # Write data rows
    row = 4
    totals = {
        'stores': 0, 'items': 0, 'lines': 0,
        'pre_store_pass': 0, 'store_adj': 0, 'pre_weather': 0,
        'weather_adj': 0, 'forecast': 0, 'forecast_avg': 0,
        'w4_shipped': 0, 'w3_shipped': 0, 'w2_shipped': 0, 'w1_shipped': 0,
        'w4_sold': 0, 'w3_sold': 0, 'w2_sold': 0, 'w1_sold': 0,
        'severe': 0, 'high': 0, 'moderate': 0, 'low': 0, 'minimal': 0,
        'weather_adjusted': 0, 'delta': 0
    }
    
    for d in data:
        col = 0
        # Basic info
        ws.write(row, col, d.get('forecast_date'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        ws.write(row, col, d.get('store_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('item_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('line_count'), formats['number'])
        col += 1
        
        # Forecast quantities
        ws.write(row, col, d.get('total_forecast_pre_store_pass'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_store_level_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_pre_weather'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_weather_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_qty'), formats['number'])
        col += 1
        
        # Forecast Average
        ws.write(row, col, d.get('total_forecast_average'), formats['number'])
        col += 1
        
        # Shipped Trend
        shipped_trend = build_sales_trend_string(
            d.get('w4_shipped_total'), d.get('w3_shipped_total'),
            d.get('w2_shipped_total'), d.get('w1_shipped_total')
        )
        ws.write(row, col, shipped_trend, formats['trend'])
        col += 1
        
        # Sold Trend
        sold_trend = build_sales_trend_string(
            d.get('w4_sold_total'), d.get('w3_sold_total'),
            d.get('w2_sold_total'), d.get('w1_sold_total')
        )
        ws.write(row, col, sold_trend, formats['trend'])
        col += 1
        
        # Growth % (NEW COLUMNS)
        growth_w1 = d.get('growth_vs_w1_pct') or 0
        growth_w2 = d.get('growth_vs_w2_pct') or 0
        ws.write(row, col, growth_w1, get_growth_pct_format(formats, growth_w1))
        col += 1
        ws.write(row, col, growth_w2, get_growth_pct_format(formats, growth_w2))
        col += 1
        
        # Expected Shrink with conditional formatting
        exp_shrink_avg = d.get('expected_shrink_from_avg') or 0
        exp_shrink_lw = d.get('expected_shrink_from_lw') or 0
        exp_shrink_2w = d.get('expected_shrink_from_2w') or 0
        lw_shrink = d.get('lw_shrink_pct') or 0
        
        ws.write(row, col, exp_shrink_avg, get_shrink_pct_format(formats, exp_shrink_avg))
        col += 1
        ws.write(row, col, exp_shrink_lw, get_shrink_pct_format(formats, exp_shrink_lw))
        col += 1
        ws.write(row, col, exp_shrink_2w, get_shrink_pct_format(formats, exp_shrink_2w))
        col += 1
        ws.write(row, col, lw_shrink, get_shrink_pct_format(formats, lw_shrink))
        col += 1
        
        # Weather severity counts
        severe = d.get('severe_count', 0) or 0
        high = d.get('high_count', 0) or 0
        moderate = d.get('moderate_count', 0) or 0
        low = d.get('low_count', 0) or 0
        minimal = d.get('minimal_count', 0) or 0
        
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
        
        # Avg weather
        avg_sev = d.get('avg_weather_severity') or 0
        ws.write(row, col, avg_sev, get_severity_format(formats, avg_sev))
        col += 1
        ws.write(row, col, d.get('items_weather_adjusted'), formats['number'])
        col += 1
        
        # Delta from LW
        ws.write(row, col, d.get('delta_from_lw'), formats['number'])
        col += 1
        delta_pct = d.get('delta_from_lw_pct') or 0
        ws.write(row, col, delta_pct, formats['pct'])
        col += 1
        
        # Accumulate totals
        totals['stores'] = max(totals['stores'], d.get('store_count') or 0)
        totals['items'] = max(totals['items'], d.get('item_count') or 0)
        totals['lines'] += d.get('line_count') or 0
        totals['pre_store_pass'] += d.get('total_forecast_pre_store_pass') or 0
        totals['store_adj'] += d.get('total_store_level_adj') or 0
        totals['pre_weather'] += d.get('total_forecast_pre_weather') or 0
        totals['weather_adj'] += d.get('total_weather_adj') or 0
        totals['forecast'] += d.get('total_forecast_qty') or 0
        totals['forecast_avg'] += d.get('total_forecast_average') or 0
        totals['w4_shipped'] += d.get('w4_shipped_total') or 0
        totals['w3_shipped'] += d.get('w3_shipped_total') or 0
        totals['w2_shipped'] += d.get('w2_shipped_total') or 0
        totals['w1_shipped'] += d.get('w1_shipped_total') or 0
        totals['w4_sold'] += d.get('w4_sold_total') or 0
        totals['w3_sold'] += d.get('w3_sold_total') or 0
        totals['w2_sold'] += d.get('w2_sold_total') or 0
        totals['w1_sold'] += d.get('w1_sold_total') or 0
        totals['severe'] += severe
        totals['high'] += high
        totals['moderate'] += moderate
        totals['low'] += low
        totals['minimal'] += minimal
        totals['weather_adjusted'] += d.get('items_weather_adjusted') or 0
        totals['delta'] += d.get('delta_from_lw') or 0
        
        row += 1
    
    # Write totals row
    _write_daily_totals_row(ws, row, totals, formats)
    
    # Freeze panes
    ws.freeze_panes(4, 2)
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(headers) - 1)


def _write_daily_totals_row(ws, row, totals, formats):
    """Write the totals row for daily summary."""
    col = 0
    ws.write(row, col, 'TOTAL', formats['text_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['stores'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['items'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['lines'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_store_pass'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['store_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['pre_weather'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['weather_adj'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['forecast_avg'], formats['number_bold'])
    col += 1
    
    # Trends
    shipped_total_trend = build_sales_trend_string(
        totals['w4_shipped'], totals['w3_shipped'],
        totals['w2_shipped'], totals['w1_shipped']
    )
    ws.write(row, col, shipped_total_trend, formats['trend'])
    col += 1
    
    sold_total_trend = build_sales_trend_string(
        totals['w4_sold'], totals['w3_sold'],
        totals['w2_sold'], totals['w1_sold']
    )
    ws.write(row, col, sold_total_trend, formats['trend'])
    col += 1
    
    # Growth totals
    total_growth_w1 = (totals['forecast_avg'] - totals['w1_sold']) / totals['w1_sold'] if totals['w1_sold'] > 0 else 0
    total_growth_w2 = (totals['forecast_avg'] - totals['w2_sold']) / totals['w2_sold'] if totals['w2_sold'] > 0 else 0
    ws.write(row, col, total_growth_w1, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_growth_w2, formats['pct_bold'])
    col += 1
    
    # Shrink totals
    total_exp_shrink_avg = (totals['forecast'] - totals['forecast_avg']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_lw = (totals['forecast'] - totals['w1_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_exp_shrink_2w = (totals['forecast'] - totals['w2_sold']) / totals['forecast'] if totals['forecast'] > 0 else 0
    total_lw_shrink = (totals['w1_shipped'] - totals['w1_sold']) / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    
    ws.write(row, col, total_exp_shrink_avg, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_lw, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_exp_shrink_2w, formats['pct_bold'])
    col += 1
    ws.write(row, col, total_lw_shrink, formats['pct_bold'])
    col += 1
    
    # Weather totals
    ws.write(row, col, totals['severe'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['high'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['moderate'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['low'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['minimal'], formats['number_bold'])
    col += 1
    ws.write(row, col, '', formats['text_bold'])
    col += 1
    ws.write(row, col, totals['weather_adjusted'], formats['number_bold'])
    col += 1
    ws.write(row, col, totals['delta'], formats['number_bold'])
    col += 1
    total_delta_pct = totals['delta'] / totals['w1_shipped'] if totals['w1_shipped'] > 0 else 0
    ws.write(row, col, total_delta_pct, formats['pct_bold'])


def write_store_summary_sheet(wb, conn, region: str,
                               start_date: str, end_date: str,
                               formats: dict):
    """Create the Store Summary worksheet BY DATE."""
    ws = wb.add_worksheet('Store Summary')
    
    # Set column widths
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:C', 10)   # Store #
    ws.set_column('D:D', 22)   # Store name
    ws.set_column('E:E', 8)    # Weather Icon
    ws.set_column('F:G', 8)    # Items, Lines
    ws.set_column('H:L', 12)   # Forecast quantities
    ws.set_column('M:M', 14)   # Fcst Avg
    ws.set_column('N:N', 26)   # Shipped Trend
    ws.set_column('O:O', 26)   # Sold Trend
    ws.set_column('P:Q', 11)   # Growth %
    ws.set_column('R:U', 11)   # Expected Shrink columns
    ws.set_column('V:W', 11)   # Weather severity
    ws.set_column('X:Y', 12)   # Delta columns
    
    # Title
    ws.merge_range('A1:Y1', f'Store Daily Summary - Region {region}', formats['title'])
    ws.merge_range('A2:Y2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Column headers
    headers = [
        'Date', 'Day', 'Store #', 'Store Name', 'Weather',
        'Items', 'Lines',
        'Fcst (Pre Store Adj)', 'Store Adj Qty', 'Fcst (Pre Weather)',
        'Weather Adj', 'Fcst Final',
        'Fcst Avg (Exp Sales)',
        'Shipped Trend (W4>W3>W2>W1)', 'Sold Trend (W4>W3>W2>W1)',
        'Growth vs W1 %', 'Growth vs W2 %',
        'Exp Shrink (Avg) %', 'Exp Shrink (LW) %', 'Exp Shrink (2W) %', 'LW Shrink %',
        'Weather Severity', 'Severity Category',
        'Delta from LW', 'Delta LW %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Get data
    query = get_store_summary_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting store summary: {e}")
        data = []
    
    # Write data rows
    row = 4
    for d in data:
        col = 0
        
        # Date and Day
        ws.write(row, col, d.get('forecast_date'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        
        # Store info
        ws.write(row, col, d.get('store_no'), formats['number'])
        col += 1
        ws.write(row, col, d.get('store_name') or f"Store {d.get('store_no')}", formats['text'])
        col += 1
        
        # Weather indicator icon
        weather_condition = d.get('weather_condition') or ''
        severity_cat = d.get('max_severity_category') or 'MINIMAL'
        severity_score = d.get('max_weather_severity') or 0
        weather_icon = get_weather_indicator_icon(
            condition=weather_condition,
            severity_category=severity_cat,
            severity_score=severity_score
        )
        ws.write(row, col, f"{weather_icon} {severity_cat}", get_severity_format(formats, severity_score, severity_cat))
        col += 1
        
        # Counts
        ws.write(row, col, d.get('item_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('line_count'), formats['number'])
        col += 1
        
        # Forecast quantities
        ws.write(row, col, d.get('total_forecast_pre_store_pass'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_store_level_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_pre_weather'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_weather_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_qty'), formats['number'])
        col += 1
        
        # Forecast Average
        ws.write(row, col, d.get('total_forecast_average'), formats['number'])
        col += 1
        
        # Trends
        shipped_trend = build_sales_trend_string(
            d.get('w4_shipped_total'), d.get('w3_shipped_total'),
            d.get('w2_shipped_total'), d.get('w1_shipped_total')
        )
        ws.write(row, col, shipped_trend, formats['trend'])
        col += 1
        
        sold_trend = build_sales_trend_string(
            d.get('w4_sold_total'), d.get('w3_sold_total'),
            d.get('w2_sold_total'), d.get('w1_sold_total')
        )
        ws.write(row, col, sold_trend, formats['trend'])
        col += 1
        
        # Growth %
        growth_w1 = d.get('growth_vs_w1_pct') or 0
        growth_w2 = d.get('growth_vs_w2_pct') or 0
        ws.write(row, col, growth_w1, get_growth_pct_format(formats, growth_w1))
        col += 1
        ws.write(row, col, growth_w2, get_growth_pct_format(formats, growth_w2))
        col += 1
        
        # Expected Shrink
        exp_shrink_avg = d.get('expected_shrink_from_avg') or 0
        exp_shrink_lw = d.get('expected_shrink_from_lw') or 0
        exp_shrink_2w = d.get('expected_shrink_from_2w') or 0
        lw_shrink = d.get('lw_shrink_pct') or 0
        
        ws.write(row, col, exp_shrink_avg, get_shrink_pct_format(formats, exp_shrink_avg))
        col += 1
        ws.write(row, col, exp_shrink_lw, get_shrink_pct_format(formats, exp_shrink_lw))
        col += 1
        ws.write(row, col, exp_shrink_2w, get_shrink_pct_format(formats, exp_shrink_2w))
        col += 1
        ws.write(row, col, lw_shrink, get_shrink_pct_format(formats, lw_shrink))
        col += 1
        
        # Weather severity
        ws.write(row, col, severity_score, get_severity_format(formats, severity_score))
        col += 1
        ws.write(row, col, severity_cat, get_severity_format(formats, severity_score, severity_cat))
        col += 1
        
        # Delta
        ws.write(row, col, d.get('delta_from_lw'), formats['number'])
        col += 1
        delta_pct = d.get('delta_from_lw_pct') or 0
        ws.write(row, col, delta_pct, formats['pct'])
        col += 1
        
        row += 1
    
    # Freeze panes
    ws.freeze_panes(4, 4)
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(headers) - 1)


def write_item_summary_sheet(wb, conn, region: str,
                              start_date: str, end_date: str,
                              formats: dict):
    """Create the Item Summary worksheet BY DATE."""
    ws = wb.add_worksheet('Item Summary')
    
    # Set column widths
    ws.set_column('A:A', 12)   # Date
    ws.set_column('B:B', 10)   # Day
    ws.set_column('C:C', 12)   # Item #
    ws.set_column('D:D', 35)   # Item Description
    ws.set_column('E:F', 8)    # Stores, Lines
    ws.set_column('G:K', 12)   # Forecast quantities
    ws.set_column('L:L', 14)   # Fcst Avg
    ws.set_column('M:M', 26)   # Shipped Trend
    ws.set_column('N:N', 26)   # Sold Trend
    ws.set_column('O:P', 11)   # Growth %
    ws.set_column('Q:T', 11)   # Expected Shrink columns
    ws.set_column('U:Y', 8)    # Weather severity counts
    ws.set_column('Z:Z', 11)   # Avg Weather
    ws.set_column('AA:AB', 12) # Delta, Delta %
    
    # Title
    ws.merge_range('A1:AB1', f'Item Daily Summary - Region {region}', formats['title'])
    ws.merge_range('A2:AB2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Column headers
    headers = [
        'Forecast Date', 'Day', 'Item #', 'Item Description',
        'Stores', 'Lines',
        'Fcst (Pre Store Adj)', 'Store Adj Qty', 'Fcst (Pre Weather)', 
        'Weather Adj', 'Fcst Final',
        'Fcst Avg (Exp Sales)',
        'Shipped Trend (W4>W3>W2>W1)', 'Sold Trend (W4>W3>W2>W1)',
        'Growth vs W1 %', 'Growth vs W2 %',
        'Exp Shrink (Avg) %', 'Exp Shrink (LW) %', 'Exp Shrink (2W) %', 'LW Shrink %',
        '🔴 Severe', '🟠 High', '🟡 Moderate', '🟢 Low', '✅ Minimal',
        'Avg Weather',
        'Delta from LW', 'Delta LW %'
    ]
    
    for col, header in enumerate(headers):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Get data
    query = get_item_summary_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting item summary: {e}")
        data = []
    
    # Write data rows
    row = 4
    for d in data:
        col = 0
        
        # Basic info
        ws.write(row, col, d.get('forecast_date'), formats['date'])
        col += 1
        ws.write(row, col, d.get('day_name'), formats['text_center'])
        col += 1
        ws.write(row, col, d.get('item_no'), formats['number'])
        col += 1
        ws.write(row, col, d.get('item_desc') or f"Item {d.get('item_no')}", formats['text'])
        col += 1
        ws.write(row, col, d.get('store_count'), formats['number'])
        col += 1
        ws.write(row, col, d.get('line_count'), formats['number'])
        col += 1
        
        # Forecast quantities
        ws.write(row, col, d.get('total_forecast_pre_store_pass'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_store_level_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_pre_weather'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_weather_adj'), formats['number'])
        col += 1
        ws.write(row, col, d.get('total_forecast_qty'), formats['number'])
        col += 1
        
        # Forecast Average
        ws.write(row, col, d.get('total_forecast_average'), formats['number'])
        col += 1
        
        # Trends
        shipped_trend = build_sales_trend_string(
            d.get('w4_shipped_total'), d.get('w3_shipped_total'),
            d.get('w2_shipped_total'), d.get('w1_shipped_total')
        )
        ws.write(row, col, shipped_trend, formats['trend'])
        col += 1
        
        sold_trend = build_sales_trend_string(
            d.get('w4_sold_total'), d.get('w3_sold_total'),
            d.get('w2_sold_total'), d.get('w1_sold_total')
        )
        ws.write(row, col, sold_trend, formats['trend'])
        col += 1
        
        # Growth %
        growth_w1 = d.get('growth_vs_w1_pct') or 0
        growth_w2 = d.get('growth_vs_w2_pct') or 0
        ws.write(row, col, growth_w1, get_growth_pct_format(formats, growth_w1))
        col += 1
        ws.write(row, col, growth_w2, get_growth_pct_format(formats, growth_w2))
        col += 1
        
        # Expected Shrink
        exp_shrink_avg = d.get('expected_shrink_from_avg') or 0
        exp_shrink_lw = d.get('expected_shrink_from_lw') or 0
        exp_shrink_2w = d.get('expected_shrink_from_2w') or 0
        lw_shrink = d.get('lw_shrink_pct') or 0
        
        ws.write(row, col, exp_shrink_avg, get_shrink_pct_format(formats, exp_shrink_avg))
        col += 1
        ws.write(row, col, exp_shrink_lw, get_shrink_pct_format(formats, exp_shrink_lw))
        col += 1
        ws.write(row, col, exp_shrink_2w, get_shrink_pct_format(formats, exp_shrink_2w))
        col += 1
        ws.write(row, col, lw_shrink, get_shrink_pct_format(formats, lw_shrink))
        col += 1
        
        # Weather severity counts
        severe = d.get('severe_count', 0) or 0
        high = d.get('high_count', 0) or 0
        moderate = d.get('moderate_count', 0) or 0
        low = d.get('low_count', 0) or 0
        minimal = d.get('minimal_count', 0) or 0
        
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
        
        # Avg weather
        avg_sev = d.get('avg_weather_severity') or 0
        ws.write(row, col, avg_sev, get_severity_format(formats, avg_sev))
        col += 1
        
        # Delta
        ws.write(row, col, d.get('delta_from_lw'), formats['number'])
        col += 1
        delta_pct = d.get('delta_from_lw_pct') or 0
        ws.write(row, col, delta_pct, formats['pct'])
        col += 1
        
        row += 1
    
    # Freeze panes
    ws.freeze_panes(4, 4)
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(headers) - 1)


def write_item_detail_sheet(wb, conn, region: str,
                            start_date: str, end_date: str,
                            formats: dict):
    """Create the Item Details worksheet with full detail."""
    ws = wb.add_worksheet('Item Details')
    
    # Title
    ws.merge_range('A1:AD1', f'Item/Store Details - Region {region}', formats['title'])
    ws.merge_range('A2:AD2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    # Get data
    query = get_item_detail_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
    except Exception as e:
        print(f"Error getting item details: {e}")
        return
    
    if len(df) == 0:
        ws.write(4, 0, "No data available", formats['text'])
        return
    
    # Get column names
    columns = df.columns
    
    # Write headers
    for col, header in enumerate(columns):
        ws.write(3, col, header, formats['col_header'])
    ws.set_row(3, 40)
    
    # Set column widths
    col_widths = {
        'Forecast Date': 12, 'Day': 10, 'Store #': 8, 'Store Name': 20,
        'Item #': 10, 'Item Description': 30, 'Case Pack': 8,
        'Fcst Pre-Store Adj': 14, 'Store Adj Qty': 12, 'Fcst Pre-Weather': 14,
        'Fcst Final': 12, 'Fcst Cases': 10, 'Weather Adj': 10,
        'Fcst Avg (Exp Sales)': 14,
        'W4 Ship': 10, 'W3 Ship': 10, 'W2 Ship': 10, 'W1 Ship': 10,
        'W4 Sold': 10, 'W3 Sold': 10, 'W2 Sold': 10, 'W1 Sold': 10,
        'Growth vs W1 %': 12, 'Growth vs W2 %': 12,
        'Exp Shrink (Avg) %': 14, 'Exp Shrink (LW) %': 14, 'Exp Shrink (2W) %': 14,
        'W1 Shrink %': 12,
        'Weather Severity': 12, 'Severity Category': 14, 'Weather Condition': 18,
        'Weather Indicator': 30,
        'Delta from LW': 12, 'Delta LW %': 11, 'Cover Applied': 10
    }
    
    for col, header in enumerate(columns):
        width = col_widths.get(header, 12)
        ws.set_column(col, col, width)
    
    # Write data
    row = 4
    data = df.to_dicts()
    
    for d in data:
        for col, header in enumerate(columns):
            value = d.get(header)
            
            # Apply appropriate format
            if 'Date' in header:
                ws.write(row, col, value, formats['date'])
            elif 'Shrink' in header and '%' in header:
                pct_val = (value or 0) / 100
                ws.write(row, col, pct_val, get_shrink_pct_format(formats, pct_val))
            elif 'Growth vs W' in header:
                pct_val = (value or 0) / 100
                ws.write(row, col, pct_val, get_growth_pct_format(formats, pct_val))
            elif header == 'Severity Category':
                severity_score = d.get('Weather Severity') or 0
                weather_condition = d.get('Weather Condition') or ''
                severity_cat = value or 'MINIMAL'
                icon = get_weather_indicator_icon(
                    condition=weather_condition,
                    severity_category=severity_cat,
                    severity_score=severity_score
                )
                ws.write(row, col, f"{icon} {severity_cat}", get_severity_format(formats, severity_score, value))
            elif header == 'Weather Indicator':
                severity_score = d.get('Weather Severity') or 0
                severity_cat = d.get('Severity Category') or 'MINIMAL'
                weather_condition = d.get('Weather Condition') or ''
                icon = get_weather_indicator_icon(
                    condition=weather_condition,
                    severity_category=severity_cat,
                    severity_score=severity_score
                )
                ws.write(row, col, f"{icon} {weather_condition}", get_severity_format(formats, severity_score, severity_cat))
            elif header == 'Weather Severity':
                ws.write(row, col, value, get_severity_format(formats, value or 0))
            elif header in ('Day', 'Store Name', 'Item Description', 'Weather Condition'):
                ws.write(row, col, value, formats['text'])
            elif 'Delta LW %' in header:
                pct_val = (value or 0) / 100
                ws.write(row, col, pct_val, formats['pct'])
            elif 'Cover' in header:
                ws.write(row, col, value, formats['decimal2'])
            else:
                ws.write(row, col, value, formats['number'])
        
        row += 1
    
    # Freeze panes
    ws.freeze_panes(4, 6)
    
    # Add autofilter
    ws.autofilter(3, 0, row - 1, len(columns) - 1)


def write_weather_impact_sheet(wb, conn, region: str,
                               start_date: str, end_date: str,
                               formats: dict):
    """Create the Weather Impact Summary worksheet with comprehensive weather data."""
    ws = wb.add_worksheet('Weather Impact')
    
    # Title
    ws.merge_range('A1:AD1', f'Weather Impact Summary - Region {region}', formats['title'])
    ws.merge_range('A2:AD2', f'Forecast Period: {start_date} to {end_date}', formats['subtitle'])
    ws.set_row(0, 30)
    ws.set_row(1, 25)
    
    current_row = 4
    
    # Section 1: Weather Summary by Date
    ws.merge_range(current_row, 0, current_row, 20, 'Daily Weather Summary', formats['section'])
    current_row += 1
    
    query = get_weather_summary_by_date_query(region, start_date, end_date)
    try:
        df = pl.from_pandas(conn.sql(query).to_df())
        data = df.to_dicts()
    except Exception as e:
        print(f"Error getting weather summary: {e}")
        data = []
    
    if data:
        # Write headers - updated to match new query columns
        headers = ['Date', 'Day', 'Store Count', 
                  '🔴 Severe', '🟠 High', '🟡 Moderate', '🟢 Low', '✅ Minimal',
                  'Avg Severity', 'Max Severity', 'Avg Impact', 'Min Impact',
                  'Avg Min°F', 'Avg Max°F', 'Coldest', 'Warmest',
                  'Rain Likely', 'Rain', 'Snow', 'Snow >2"',
                  'Avg Rain"', 'Avg Snow"', 'Avg Depth"', 'Avg Wind', 'Max Gust',
                  'Qty Adj', 'Items Adj']
        
        for col, header in enumerate(headers):
            ws.write(current_row, col, header, formats['col_header'])
        current_row += 1
        
        for d in data:
            col = 0
            ws.write(current_row, col, d.get('Date'), formats['date'])
            col += 1
            ws.write(current_row, col, d.get('Day'), formats['text'])
            col += 1
            ws.write(current_row, col, d.get('Store Count'), formats['number'])
            col += 1
            
            # Severity counts with conditional formatting
            severe_count = d.get('Severe', 0) or 0
            high_count = d.get('High', 0) or 0
            moderate_count = d.get('Moderate', 0) or 0
            low_count = d.get('Low', 0) or 0
            minimal_count = d.get('Minimal', 0) or 0
            
            ws.write(current_row, col, severe_count, 
                    formats['severity_severe'] if severe_count > 0 else formats['number'])
            col += 1
            ws.write(current_row, col, high_count,
                    formats['severity_high'] if high_count > 0 else formats['number'])
            col += 1
            ws.write(current_row, col, moderate_count,
                    formats['severity_moderate'] if moderate_count > 0 else formats['number'])
            col += 1
            ws.write(current_row, col, low_count,
                    formats['severity_low'] if low_count > 0 else formats['number'])
            col += 1
            ws.write(current_row, col, minimal_count,
                    formats['severity_minimal'] if minimal_count > 0 else formats['number'])
            col += 1
            
            # Severity scores
            avg_sev = d.get('Avg Severity') or 0
            max_sev = d.get('Max Severity') or 0
            ws.write(current_row, col, avg_sev, get_severity_format(formats, avg_sev))
            col += 1
            ws.write(current_row, col, max_sev, get_severity_format(formats, max_sev))
            col += 1
            
            ws.write(current_row, col, d.get('Avg Impact Factor'), formats['decimal3'])
            col += 1
            ws.write(current_row, col, d.get('Min Impact Factor'), formats['decimal3'])
            col += 1
            
            # Temperatures
            ws.write(current_row, col, d.get('Avg Temp Min'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Avg Temp Max'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Coldest Temp'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Warmest Temp'), formats['decimal'])
            col += 1
            
            # Precipitation counts
            ws.write(current_row, col, d.get('Stores w/ Rain Likely'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Stores w/ Rain'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Stores w/ Snow'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Stores w/ Snow Depth > 2in'), formats['number'])
            col += 1
            
            # Average weather metrics
            ws.write(current_row, col, d.get('Avg Rain'), formats['decimal2'])
            col += 1
            ws.write(current_row, col, d.get('Avg Snow'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Avg Snow Depth'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Avg Wind'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Max Wind Gust'), formats['decimal'])
            col += 1
            
            # Adjustment impact
            ws.write(current_row, col, d.get('Total Qty Adj'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Total Items Adj'), formats['number'])
            
            current_row += 1
    
    # Section 2: Store-Level Weather Details (ranked by severity)
    current_row += 3
    ws.merge_range(current_row, 0, current_row, 29, 'Store-Level Weather Details (Ranked by Severity)', formats['section'])
    current_row += 1
    
    store_query = get_weather_store_detail_query(region, start_date, end_date)
    try:
        store_df = pl.from_pandas(conn.sql(store_query).to_df())
        store_data = store_df.to_dicts()
    except Exception as e:
        print(f"Error getting store weather details: {e}")
        store_data = []
    
    if store_data:
        # Write headers - expanded with all new weather variables
        store_headers = ['⚠', 'Date', 'Day', 'Store #', 'Store Name', 'Conditions',
                        'Min°F', 'Max°F', 
                        'Precip"', 'Precip%', 'Cover%',
                        'Snow"', 'Depth"', 
                        'Wind', 'Gust',
                        'Vis(mi)', 'Humid%', 'Cloud%', 'SevRisk',
                        'RainSv', 'SnowSv', 'WindSv', 'VisSv', 'TempSv',
                        'Score', 'Category', 'Impact',
                        'QtyAdj', 'Items']
        
        # Set column widths for store detail section
        col_widths = {
            0: 4, 1: 11, 2: 10, 3: 8, 4: 20, 5: 18,
            6: 6, 7: 6, 8: 7, 9: 7, 10: 7,
            11: 6, 12: 6, 13: 6, 14: 6,
            15: 7, 16: 7, 17: 7, 18: 7,
            19: 7, 20: 7, 21: 7, 22: 6, 23: 7,
            24: 6, 25: 10, 26: 7,
            27: 8, 28: 6
        }
        for c, w in col_widths.items():
            ws.set_column(c, c, w)
        
        for col, header in enumerate(store_headers):
            ws.write(current_row, col, header, formats['col_header'])
        current_row += 1
        
        # Write store detail rows
        for d in store_data:
            col = 0
            
            # Weather indicator icon
            severity_score = d.get('Severity Score') or 0
            category = d.get('Category') or 'MINIMAL'
            condition = d.get('Conditions') or ''
            snow_amt = d.get('Snow (in)') or 0
            rain_amt = d.get('Precip (in)') or 0
            temp_min = d.get('Temp Min')
            temp_max = d.get('Temp Max')
            wind_speed = d.get('Wind (mph)') or 0
            
            weather_icon = get_weather_indicator_icon(
                condition=condition,
                severity_category=category,
                snow_amount=snow_amt,
                rain_amount=rain_amt,
                temp_min=temp_min,
                temp_max=temp_max,
                wind_speed=wind_speed,
                severity_score=severity_score
            )
            ws.write(current_row, col, weather_icon, get_severity_format(formats, severity_score, category))
            col += 1
            
            ws.write(current_row, col, d.get('Date'), formats['date'])
            col += 1
            ws.write(current_row, col, d.get('Day'), formats['text_center'])
            col += 1
            ws.write(current_row, col, d.get('Store #'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Store Name'), formats['text'])
            col += 1
            ws.write(current_row, col, d.get('Conditions') or '', formats['text'])
            col += 1
            ws.write(current_row, col, d.get('Temp Min'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Temp Max'), formats['decimal'])
            col += 1
            
            # Precipitation details
            ws.write(current_row, col, d.get('Precip (in)'), formats['decimal2'])
            col += 1
            ws.write(current_row, col, d.get('Precip %'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Precip Cover %'), formats['number'])
            col += 1
            
            # Snow details
            ws.write(current_row, col, d.get('Snow (in)'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Snow Depth'), formats['decimal'])
            col += 1
            
            # Wind details
            ws.write(current_row, col, d.get('Wind (mph)'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Wind Gust'), formats['decimal'])
            col += 1
            
            # Atmosphere
            ws.write(current_row, col, d.get('Visibility'), formats['decimal'])
            col += 1
            ws.write(current_row, col, d.get('Humidity %'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Cloud Cover %'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Severe Risk'), formats['number'])
            col += 1
            
            # Component severity scores
            rain_sev = d.get('Rain Sev') or 0
            snow_sev = d.get('Snow Sev') or 0
            wind_sev = d.get('Wind Sev') or 0
            vis_sev = d.get('Vis Sev') or 0
            temp_sev = d.get('Temp Sev') or 0
            
            ws.write(current_row, col, rain_sev, get_severity_format(formats, rain_sev))
            col += 1
            ws.write(current_row, col, snow_sev, get_severity_format(formats, snow_sev))
            col += 1
            ws.write(current_row, col, wind_sev, get_severity_format(formats, wind_sev))
            col += 1
            ws.write(current_row, col, vis_sev, get_severity_format(formats, vis_sev))
            col += 1
            ws.write(current_row, col, temp_sev, get_severity_format(formats, temp_sev))
            col += 1
            
            # Severity Score with conditional formatting
            ws.write(current_row, col, severity_score, get_severity_format(formats, severity_score))
            col += 1
            
            # Category with conditional formatting
            ws.write(current_row, col, category, get_severity_format(formats, severity_score, category))
            col += 1
            
            ws.write(current_row, col, d.get('Impact Factor'), formats['decimal3'])
            col += 1
            ws.write(current_row, col, d.get('Qty Adjusted'), formats['number'])
            col += 1
            ws.write(current_row, col, d.get('Items Adj'), formats['number'])
            
            current_row += 1
    
    # Add legend at the bottom
    current_row += 3
    ws.merge_range(current_row, 0, current_row, 5, 'Severity Category Legend:', formats['section'])
    current_row += 1
    
    legend_items = [
        (f"{SEVERITY_ICONS['SEVERE']} SEVERE (7-10)", 'severity_severe', 'Dangerous conditions - significant travel hazard'),
        (f"{SEVERITY_ICONS['HIGH']} HIGH (5-7)", 'severity_high', 'Poor conditions - notable impact on foot traffic'),
        (f"{SEVERITY_ICONS['MODERATE']} MODERATE (3-5)", 'severity_moderate', 'Fair conditions - some impact expected'),
        (f"{SEVERITY_ICONS['LOW']} LOW (1.5-3)", 'severity_low', 'Minor conditions - minimal impact'),
        (f"{SEVERITY_ICONS['MINIMAL']} MINIMAL (0-1.5)", 'severity_minimal', 'Good conditions - no weather impact'),
    ]
    
    for label, fmt_key, desc in legend_items:
        ws.write(current_row, 0, label, formats[fmt_key])
        ws.merge_range(current_row, 1, current_row, 5, desc, formats['text'])
        current_row += 1
    
    # Add component severity explanation
    current_row += 2
    ws.merge_range(current_row, 0, current_row, 8, 'Component Severity Scores (0-10 scale):', formats['section'])
    current_row += 1
    
    severity_components = [
        ('RainSv', 'Rain Severity', 'Based on precip amount × probability (0.05-1.0+ inches)'),
        ('SnowSv', 'Snow Severity', 'Based on new snow amount + accumulated depth bonus'),
        ('WindSv', 'Wind Severity', 'Compounds with rain/snow; minimal standalone impact'),
        ('VisSv', 'Visibility Severity', 'Fog/low visibility (<0.5mi can trigger alone)'),
        ('TempSv', 'Temperature Severity', 'Extreme cold (<10°F) or heat (>100°F); max 3.0'),
    ]
    
    for abbrev, name, desc in severity_components:
        ws.write(current_row, 0, abbrev, formats['col_header'])
        ws.write(current_row, 1, name, formats['text'])
        ws.merge_range(current_row, 2, current_row, 8, desc, formats['text'])
        current_row += 1
    
    # Add weather icon legend
    current_row += 2
    ws.merge_range(current_row, 0, current_row, 5, 'Weather Condition Icons:', formats['section'])
    current_row += 1
    
    icon_legend = [
        (WEATHER_ICONS['severe'], 'Severe/Dangerous'),
        (WEATHER_ICONS['thunderstorm'], 'Thunderstorm'),
        (WEATHER_ICONS['snow_heavy'], 'Heavy Snow'),
        (WEATHER_ICONS['snow'], 'Snow'),
        (WEATHER_ICONS['rain_heavy'], 'Heavy Rain'),
        (WEATHER_ICONS['rain'], 'Rain'),
        (WEATHER_ICONS['fog'], 'Fog/Mist'),
        (WEATHER_ICONS['wind'], 'High Wind'),
        (WEATHER_ICONS['extreme_cold'], 'Extreme Cold'),
        (WEATHER_ICONS['clear'], 'Clear/Sunny'),
    ]
    
    for icon, desc in icon_legend:
        ws.write(current_row, 0, icon, formats['text_center'])
        ws.write(current_row, 1, desc, formats['text'])
        current_row += 1
    
    # Freeze panes
    ws.freeze_panes(6, 4)
