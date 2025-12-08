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
    
    # Determine base forecast (max of recent week and EMA)
    # This prevents over-correction when recent sales are strong
    row['forecast_average'] = max(w1_sold, ema)
    
    # Special case: if no shipments last week, use average/EMA
    if row.get('w1_shipped') is None or row.get('w1_shipped') == 0:
        row['forecast_average'] = max(average_sold, ema)

    # if no sales last week and no sales in the prior week too, then use minimum of 1 case
    if (row.get('w1_sold') is None or row.get('w1_sold') == 0) and \
       (row.get('w2_sold') is None or row.get('w2_sold') == 0):
        row['forecast_average'] = max(1.0, ema)
    
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
    """
    w1_sold = row.get('w1_sold', 0) or 0
    w2_sold = row.get('w2_sold', 0) or 0
    w3_sold = row.get('w3_sold', 0) or 0
    w4_sold = row.get('w4_sold', 0) or 0
    
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
            row['forecast_average'] = max(row['forecast_average'], adjusted, row['ema'])
    
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
    w1_shrink = row.get('w1_shrink_p')
    w2_shrink = row.get('w2_shrink_p')
    
    if w1_shrink is not None and w2_shrink is not None:
        if w1_shrink >= high_shrink_threshold and w2_shrink >= high_shrink_threshold:
            # Use conservative estimate
            w1_sold = row.get('w1_sold', 0) or 0
            row['forecast_average'] = max(w1_sold, row['ema'])
    
    return row
