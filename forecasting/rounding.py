"""
Rounding and Safety Stock Module
================================
Handles case pack rounding logic and safety stock calculations.

This module is responsible for:
- Converting forecast quantities to case pack multiples
- Intelligent round-up/round-down decisions
- Safety stock calculations
- Shrink-aware rounding guardrails
"""

import math
from typing import List

from config import settings


def calculate_safety_stock(sales_volatility: float, k_factor: float) -> int:
    """
    Calculate safety stock based on sales volatility.
    
    Safety stock provides a buffer against demand variability.
    Higher volatility or service level targets increase safety stock.
    
    Args:
        sales_volatility: Standard deviation of recent sales
        k_factor: Service level factor (z-score, e.g., 0.25 for ~60%)
        
    Returns:
        Safety stock quantity (integer)
    """
    return int(math.floor(k_factor * sales_volatility))


def calculate_base_cover_quantity(forecast_average: float, 
                                  base_cover: float,
                                  was_sold_out: bool,
                                  base_cover_sold_out: float) -> tuple:
    """
    Calculate base cover quantity to add to forecast.
    
    Base cover provides additional inventory beyond the average forecast
    to handle normal demand variability.
    
    Args:
        forecast_average: Base forecast quantity
        base_cover: Normal base cover percentage
        was_sold_out: Whether last week was sold out
        base_cover_sold_out: Higher cover percentage for sold-out items
        
    Returns:
        Tuple of (cover_quantity, applied_cover_percentage)
    """
    if was_sold_out:
        cover_percentage = base_cover_sold_out
    else:
        cover_percentage = base_cover
    
    cover_quantity = forecast_average * cover_percentage
    
    return cover_quantity, cover_percentage


def round_to_case_pack(quantity: float, case_pack_size: int) -> int:
    """
    Round quantity up to nearest case pack multiple.
    
    Args:
        quantity: Quantity to round
        case_pack_size: Case pack size
        
    Returns:
        Quantity rounded up to case pack multiple
    """
    return int(math.ceil(quantity / case_pack_size)) * case_pack_size


def apply_intelligent_rounding(row: dict, case_pack_size: int,
                               round_down_shrink_threshold: float,
                               non_hero_items: List[int] = None) -> dict:
    """
    Apply intelligent rounding based on shrink and business rules.
    
    This function decides whether to round up or down based on:
    - Recent shrink percentages
    - How close the quantity is to the next case pack
    - Item classification (hero vs non-hero)
    
    Args:
        row: Item-store data dictionary with forecast_average_w_cover
        case_pack_size: Case pack size for this item
        round_down_shrink_threshold: Shrink threshold for round-down
        non_hero_items: List of non-hero item numbers
        
    Returns:
        Updated row with rounding applied
    """
    non_hero_items = non_hero_items or settings.NON_HERO_ITEMS
    
    # Initial round-up to case pack
    forecast_w_cover = row['forecast_average_w_cover']
    rounded_quantity = round_to_case_pack(forecast_w_cover, case_pack_size)
    
    # Calculate round-up amount
    round_up_quantity = rounded_quantity - forecast_w_cover
    row['round_up_quantity'] = round_up_quantity
    
    # Determine if we should round down instead
    w1_shrink = row.get('w1_shrink_p')
    round_up_revised = round_up_quantity
    
    if w1_shrink is not None:
        # If we're adding almost a full case (within 1 unit), round down
        if round_up_quantity >= (case_pack_size - 1):
            round_up_revised = -(case_pack_size - round_up_quantity)
        
        # If adding 4+ units and shrink is high, round down
        elif round_up_quantity >= (case_pack_size - 2) and \
             w1_shrink >= (round_down_shrink_threshold + 0.03):
            round_up_revised = -(case_pack_size - round_up_quantity)
    
    # Prevent rounding down below forecast average
    if (forecast_w_cover + round_up_revised) < row['forecast_average']:
        round_up_revised = round_up_quantity
    
    row['round_up_final'] = round_up_revised
    row['forecast_quantity'] = forecast_w_cover + round_up_revised
    
    # Calculate impact of rounding
    if row['forecast_quantity'] > forecast_w_cover:
        row['impact_of_rounding'] = row['forecast_quantity'] - forecast_w_cover
    else:
        row['impact_of_rounding'] = 0
    
    return row


def apply_safety_stock_buffer(row: dict, case_pack_size: int,
                              non_hero_items: List[int] = None) -> dict:
    """
    Apply safety stock buffer if conditions warrant.
    
    Safety stock is added when:
    - Item is a hero item (not in non_hero_items)
    - Safety stock calculation suggests it's needed
    - Recent sales are declining
    
    Args:
        row: Item-store data dictionary
        case_pack_size: Case pack size
        non_hero_items: List of non-hero item numbers
        
    Returns:
        Updated row with safety stock applied
    """
    non_hero_items = non_hero_items or settings.NON_HERO_ITEMS
    
    item_no = int(row.get('item_no', 0))
    safety_stock = row.get('forecast_safety_stock', 0) or 0
    w1_sold = row.get('w1_sold', 0) or 0
    w4_sold = row.get('w4_sold', 0) or 0
    average_sold = row.get('average_sold', 0) or 0
    round_up_final = row.get('round_up_final', 0) or 0
    
    # Check if safety stock should be applied
    should_apply = (
        item_no not in non_hero_items and
        safety_stock > 0 and
        w1_sold is not None and
        w4_sold is not None and
        w1_sold < average_sold and
        w1_sold < w4_sold
    )
    
    if should_apply and round_up_final < safety_stock:
        row['forecast_safety_stock_applied'] = case_pack_size
        row['forecast_quantity'] += case_pack_size
    else:
        row['forecast_safety_stock_applied'] = 0
    
    return row


def apply_effective_cover_guardrail(row: dict, base_cover_sold_out: float) -> dict:
    """
    Apply guardrail to prevent over-ordering due to rounding.
    
    If rounding has caused effective coverage to exceed targets and
    shrink was zero, reduce forecast to target coverage level.
    
    Args:
        row: Item-store data dictionary
        base_cover_sold_out: Maximum acceptable coverage percentage
        
    Returns:
        Updated row with guardrail applied
    """
    forecast_qty = row.get('forecast_quantity', 0)
    forecast_avg = row.get('forecast_average', 0)
    w1_shrink = row.get('w1_shrink_p')
    case_pack_size = row.get('case_pack_size', 6)
    
    if forecast_avg <= 0:
        return row
    
    effective_cover = forecast_qty / forecast_avg
    
    # If shrink was zero and coverage is too high, reduce
    if w1_shrink == 0:
        if effective_cover > (1 + base_cover_sold_out) and (forecast_qty / case_pack_size) > 2:
            target_qty = math.ceil(forecast_avg * (1 + base_cover_sold_out) / case_pack_size) * case_pack_size
            if target_qty < forecast_qty:
                row['forecast_quantity'] = target_qty
    
    return row


def apply_inactive_store_override(row: dict, inactive_stores: List[int] = None) -> dict:
    """
    Set forecast to zero for inactive stores.
    
    Args:
        row: Item-store data dictionary
        inactive_stores: List of inactive store numbers
        
    Returns:
        Updated row with zero forecast for inactive stores
    """
    inactive_stores = inactive_stores or settings.INACTIVE_STORES
    
    if row.get('store_no') in inactive_stores:
        row['forecast_quantity'] = 0
    
    return row


def apply_all_rounding(row: dict, params: dict) -> dict:
    """
    Apply all rounding and safety stock logic in correct order.
    
    Order of operations:
    1. Calculate base cover quantity
    2. Apply intelligent rounding
    3. Apply safety stock buffer
    4. Apply effective cover guardrail
    5. Apply inactive store override
    
    Args:
        row: Item-store data dictionary with forecast_average
        params: Dictionary of forecast parameters
        
    Returns:
        Fully processed row with final forecast_quantity
    """
    # Use row's case_pack_size if available (e.g., 3 for platters), otherwise use params default
    case_pack_size = row.get('case_pack_size') or params.get('CASE_SIZE', 6)
    base_cover = row.get('base_cover', params.get('BASE_COVER', 0.05))
    base_cover_sold_out = row.get('base_cover_sold_out', params.get('BASE_COVER_SOLD_OUT', 0.06))
    
    # Determine if sold out last week
    w1_shipped = row.get('w1_shipped')
    w1_sold = row.get('w1_sold')
    was_sold_out = (w1_shipped is not None and w1_sold is not None and w1_shipped == w1_sold)
    
    # Calculate base cover
    cover_qty, applied_cover = calculate_base_cover_quantity(
        row['forecast_average'], base_cover, was_sold_out, base_cover_sold_out
    )
    row['base_cover_quantity'] = cover_qty
    row['base_cover_applied'] = applied_cover
    row['forecast_average_w_cover'] = row['forecast_average'] + cover_qty
    
    # Store case pack size
    row['case_pack_size'] = case_pack_size
    row['result_forecast_case_pack_size'] = case_pack_size
    
    # Apply intelligent rounding
    row = apply_intelligent_rounding(
        row, case_pack_size, params.get('ROUND_DOWN_SHRINK_THRESHOLD', 0.0)
    )
    
    # Apply safety stock
    row = apply_safety_stock_buffer(row, case_pack_size)
    
    # Apply guardrails
    row = apply_effective_cover_guardrail(row, base_cover_sold_out)
    row = apply_inactive_store_override(row)
    
    # Calculate delta from last week
    w1_shipped = row.get('w1_shipped') or 0
    row['delta_from_last_week'] = row['forecast_quantity'] - w1_shipped
    
    return row
