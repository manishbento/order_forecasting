"""
Forecasting Engine
==================
Core forecasting logic for calculating base forecasts.

This module handles:
- Sales velocity calculation
- Exponential Moving Average (EMA) calculation
- Base forecast determination
- Sales volatility metrics
"""

import numpy as np
from typing import Tuple


def calculate_sales_velocity(w4_sold: float, w3_sold: float, 
                             w2_sold: float, w1_sold: float) -> float:
    """
    Calculate the sales velocity trend using linear regression slope.
    
    Sales velocity measures the rate of change in sales over time.
    A positive value indicates growing sales, negative indicates declining.
    
    Args:
        w4_sold: Sales from four weeks ago
        w3_sold: Sales from three weeks ago
        w2_sold: Sales from two weeks ago
        w1_sold: Sales from most recent week
        
    Returns:
        Slope of the best-fit line (weekly sales velocity)
        Returns 0.0 if insufficient data points
    """
    # Time points relative to forecast date
    weeks = np.array([-3, -2, -1, 0])
    sales = np.array([w4_sold, w3_sold, w2_sold, w1_sold], dtype=float)
    
    # Only use valid (non-null/NaN) data points
    valid_points = ~np.isnan(sales)
    
    # Need at least 2 points to calculate trend
    if np.sum(valid_points) < 2:
        return 0.0
    
    # polyfit returns coefficients [slope, intercept] for degree 1
    slope = np.polyfit(weeks[valid_points], sales[valid_points], 1)[0]
    
    return float(slope)


def calculate_average_sold(w1_sold: float, w2_sold: float, 
                           w3_sold: float, w4_sold: float) -> float:
    """
    Calculate average sales, excluding weeks with zero sales.
    
    This provides a more accurate baseline by not diluting the average
    with weeks where the item may have been out of stock.
    
    Args:
        w1_sold: Most recent week sales
        w2_sold: Two weeks ago sales
        w3_sold: Three weeks ago sales
        w4_sold: Four weeks ago sales
        
    Returns:
        Average of non-zero weeks, or 0 if all weeks are zero
    """
    weekly_sales = [w4_sold, w3_sold, w2_sold, w1_sold]
    weeks_with_sales = [s for s in weekly_sales if s and s > 0]
    
    if weeks_with_sales:
        return sum(weeks_with_sales) / len(weeks_with_sales)
    return 0.0


def calculate_ema(w1_sold: float, w2_sold: float, w3_sold: float, w4_sold: float,
                  weights: Tuple[float, float, float, float] = (0.6, 0.2, 0.1, 0.1)) -> float:
    """
    Calculate Exponential Moving Average with configurable weights.
    
    EMA gives more weight to recent observations, making it responsive
    to recent trends while smoothing out noise.
    
    Args:
        w1_sold: Most recent week sales
        w2_sold: Two weeks ago sales
        w3_sold: Three weeks ago sales
        w4_sold: Four weeks ago sales
        weights: Tuple of weights for (W1, W2, W3, W4), must sum to 1.0
        
    Returns:
        Weighted average of sales
    """
    w1_weight, w2_weight, w3_weight, w4_weight = weights
    
    weights_arr = np.array([w1_weight, w2_weight, w3_weight, w4_weight], dtype=float)
    sold_arr = np.array([w1_sold, w2_sold, w3_sold, w4_sold], dtype=float)
    
    # Handle NaN values
    valid_mask = ~np.isnan(sold_arr)
    
    if np.sum(valid_mask) == 0:
        return 0.0
    
    # Normalize weights for valid entries only
    valid_weights = weights_arr[valid_mask]
    valid_sales = sold_arr[valid_mask]
    
    return float(np.average(valid_sales, weights=valid_weights))


def calculate_sales_volatility(w1_sold: float, w2_sold: float, 
                               w3_sold: float, w4_sold: float) -> float:
    """
    Calculate sales volatility as standard deviation.
    
    Higher volatility indicates more unpredictable sales patterns,
    which may warrant higher safety stock.
    
    Args:
        w1_sold: Most recent week sales
        w2_sold: Two weeks ago sales
        w3_sold: Three weeks ago sales
        w4_sold: Four weeks ago sales
        
    Returns:
        Standard deviation of valid sales values
    """
    sold_arr = np.array([w1_sold, w2_sold, w3_sold, w4_sold], dtype=float)
    valid_mask = ~np.isnan(sold_arr)
    
    if np.sum(valid_mask) < 2:
        return 0.0
    
    return float(np.std(sold_arr[valid_mask]))


def calculate_base_forecast(row: dict, weights: Tuple[float, float, float, float]) -> dict:
    """
    Calculate the base forecast metrics for a single item-store combination.
    
    This is the core calculation that determines the initial forecast
    before adjustments and rounding are applied.
    
    Also tracks the baseline source for waterfall analysis:
    - 'lw_sales': Last week sales was used (LW >= EMA)
    - 'ema': EMA was used because LW sales < EMA (uplift applied)
    - 'average': Average was used (no LW shipments)
    - 'minimum_case': Minimum 1 case was used (no recent sales)
    
    Args:
        row: Dictionary containing item-store data with weekly sales
        weights: Tuple of EMA weights (W1, W2, W3, W4)
        
    Returns:
        Updated row dictionary with calculated metrics:
        - sales_velocity
        - average_sold
        - ema
        - sales_volatility
        - forecast_average (initial base forecast)
        - baseline_source (source of baseline value)
        - baseline_qty (the baseline value)
        - ema_uplift_applied (1 if EMA uplift was used)
        - ema_uplift_qty (amount of EMA uplift)
    """
    # Extract and clean weekly sales data
    w4_sold = row.get('w4_sold', 0) or 0
    w3_sold = row.get('w3_sold', 0) or 0
    w2_sold = row.get('w2_sold', 0) or 0
    w1_sold = row.get('w1_sold', 0) or 0
    
    # Calculate metrics
    sales_velocity = calculate_sales_velocity(w4_sold, w3_sold, w2_sold, w1_sold)
    average_sold = calculate_average_sold(w1_sold, w2_sold, w3_sold, w4_sold)
    ema = calculate_ema(w1_sold, w2_sold, w3_sold, w4_sold, weights)
    sales_volatility = calculate_sales_volatility(w1_sold, w2_sold, w3_sold, w4_sold)
    
    # Store calculated values
    row['sales_velocity'] = sales_velocity
    row['average_sold'] = average_sold
    row['ema'] = ema
    row['sales_volatility'] = sales_volatility
    
    # Initialize baseline tracking fields
    row['baseline_source'] = 'lw_sales'
    row['baseline_qty'] = w1_sold
    row['baseline_adj_qty'] = 0.0
    row['ema_uplift_applied'] = 0
    row['ema_uplift_qty'] = 0.0
    row['baseline_uplift_qty'] = 0.0  # Tracks change from w1_sold to baseline (for all sources)
    
    # Determine base forecast (max of recent week and EMA)
    # This prevents over-correction when recent sales are strong
    if w1_sold >= ema:
        # LW sales is on track or above trend - use LW sales
        row['forecast_average'] = w1_sold
        row['baseline_source'] = 'lw_sales'
        row['baseline_qty'] = w1_sold
        row['ema_uplift_applied'] = 0
        row['ema_uplift_qty'] = 0.0
        row['baseline_uplift_qty'] = 0.0  # No change from w1_sold
    else:
        # LW sales below trend - uplift to EMA
        row['forecast_average'] = ema
        row['baseline_source'] = 'ema'
        row['baseline_qty'] = ema
        row['ema_uplift_applied'] = 1
        row['ema_uplift_qty'] = ema - w1_sold  # Positive value showing uplift amount
        row['baseline_uplift_qty'] = ema - w1_sold  # Same as ema_uplift for EMA case
    
    # Special case: if no shipments last week, use average/EMA
    if row.get('w1_shipped') is None or row.get('w1_shipped') == 0:
        if average_sold >= ema:
            row['forecast_average'] = average_sold
            row['baseline_source'] = 'average'
            row['baseline_qty'] = average_sold
            row['baseline_uplift_qty'] = average_sold - w1_sold  # Track uplift from w1_sold (usually 0)
        else:
            row['forecast_average'] = ema
            row['baseline_source'] = 'ema'
            row['baseline_qty'] = ema
            row['baseline_uplift_qty'] = ema - w1_sold
        row['ema_uplift_applied'] = 0
        row['ema_uplift_qty'] = 0.0

    # Special case: if no sales last week and no sales in the prior week too, 
    # then use minimum of 1 case
    if (row.get('w1_sold') is None or row.get('w1_sold') == 0) and \
       (row.get('w2_sold') is None or row.get('w2_sold') == 0):
        if ema >= 1.0:
            row['forecast_average'] = ema
            row['baseline_source'] = 'ema'
            row['baseline_qty'] = ema
            row['baseline_uplift_qty'] = ema - w1_sold
        else:
            row['forecast_average'] = 1.0
            row['baseline_source'] = 'minimum_case'
            row['baseline_qty'] = 1.0
            row['baseline_uplift_qty'] = 1.0 - w1_sold  # Track uplift from w1_sold (usually 0)
        row['ema_uplift_applied'] = 0
        row['ema_uplift_qty'] = 0.0
    
    # Calculate baseline adjustment (how much we adjusted from LW sales)
    row['baseline_adj_qty'] = row['forecast_average'] - w1_sold
    
    return row


def apply_decline_adjustment(row: dict, decline_threshold: float = 0.15) -> dict:
    """
    Adjust forecast when there's a significant week-over-week decline.
    
    If sales declined significantly and this matches the store trend,
    the forecast is adjusted to use older week data.
    
    Args:
        row: Dictionary with item-store data
        decline_threshold: Percentage decline threshold (default 15%)
        
    Returns:
        Updated row with adjusted forecast_average if applicable
        Also sets:
        - decline_adj_applied: 1 if adjustment was applied
        - decline_adj_qty: Amount of adjustment added
    """
    w1_sold = row.get('w1_sold', 0) or 0
    w2_sold = row.get('w2_sold', 0) or 0
    w3_sold = row.get('w3_sold', 0) or 0
    w4_sold = row.get('w4_sold', 0) or 0
    
    # Initialize tracking fields
    row['decline_adj_applied'] = 0
    row['decline_adj_qty'] = 0.0
    
    pre_adjustment = row.get('forecast_average', 0)
    
    if w1_sold and w2_sold and w2_sold > 0:
        # Calculate decline percentages
        item_decline = (w2_sold - w1_sold) / w2_sold
        
        store_w1_sold = row.get('store_w1_sold', 0) or 0
        store_w2_sold = row.get('store_w2_sold', 0) or 0
        store_decline = 0
        if store_w2_sold and store_w2_sold > 0:
            store_decline = (store_w2_sold - store_w1_sold) / store_w2_sold
        
        # If both item and store show significant decline
        if item_decline >= decline_threshold and store_decline >= decline_threshold:
            # Use weighted average of older weeks
            adjusted = w2_sold * 0.5 + w3_sold * 0.4 + w4_sold * 0.1
            new_forecast = max(row['forecast_average'], adjusted, row['ema'])
            
            # Track the adjustment
            if new_forecast > pre_adjustment:
                row['decline_adj_applied'] = 1
                row['decline_adj_qty'] = new_forecast - pre_adjustment
            
            row['forecast_average'] = new_forecast
    
    return row


def apply_high_shrink_adjustment(row: dict, high_shrink_threshold: float = 0.15) -> dict:
    """
    Apply conservative forecast when recent weeks had high shrink.
    
    Consecutive high-shrink weeks indicate overordering, so we use
    a more conservative forecast approach.
    
    Args:
        row: Dictionary with item-store data
        high_shrink_threshold: Threshold for "high" shrink (default 15%)
        
    Returns:
        Updated row with adjusted forecast_average if applicable
    """
    # Initialize tracking fields
    row['high_shrink_adj_applied'] = 0
    row['high_shrink_adj_qty'] = 0.0
    
    w1_shrink = row.get('w1_shrink_p')
    w2_shrink = row.get('w2_shrink_p')
    
    if w1_shrink is not None and w2_shrink is not None:
        if w1_shrink >= high_shrink_threshold and w2_shrink >= high_shrink_threshold:
            # Track pre-adjustment value
            pre_adjustment = row.get('forecast_average', 0) or 0
            
            # Use conservative estimate
            w1_sold = row.get('w1_sold', 0) or 0
            new_forecast = max(w1_sold, row['ema'])
            
            # Track the adjustment
            row['high_shrink_adj_applied'] = 1
            row['high_shrink_adj_qty'] = new_forecast - pre_adjustment
            
            row['forecast_average'] = new_forecast
    
    return row
