"""
Forecast Adjustments Module
===========================
Handles promotional uplifts, event adjustments, and region-specific modifications.

This module applies business rules that modify the base forecast based on:
- Promotional events
- Holidays and special occasions
- Regional variations
- Store-specific adjustments
- Item-specific rules
- Weather conditions
- Store-level pass (shrink control and coverage)
"""

from datetime import datetime
from typing import List, Tuple
from copy import deepcopy

from config.settings import NON_HERO_ITEMS


# =============================================================================
# PROMOTION CONFIGURATIONS
# =============================================================================
# These can be moved to external configuration files in the future

PROMOTIONS = [
    {
        'name': 'LA_August_2025',
        'regions': ['LA'],
        'start_date': datetime(2025, 8, 19),
        'end_date': datetime(2025, 8, 25),
        'multiplier': 1.05
    },
    {
        'name': 'BA_August_2025',
        'regions': ['BA'],
        'start_date': datetime(2025, 8, 25),
        'end_date': datetime(2025, 8, 31),
        'multiplier': 1.10
    },
    {
        'name': 'BA_National_Sept_2025',
        'regions': ['BA'],
        'start_date': datetime(2025, 9, 22),
        'end_date': datetime(2025, 9, 28),
        'multiplier': 1.05
    },
]

THANKSGIVING_ADJUSTMENTS = [
    {
        'name': 'BA_SD_Cannibalism',
        'regions': ['BA', 'SD'],
        'start_date': datetime(2025, 11, 21),
        'end_date': datetime(2025, 11, 23),
        'multiplier': 0.88
    },
    {
        'name': 'Other_Regions_Thanksgiving',
        'regions': ['LA', 'NE', 'TE'],  # Excludes BA, SD, SE
        'start_date': datetime(2025, 11, 24),
        'end_date': datetime(2025, 11, 26),
        'multiplier': 1.10
    },
    {
        'name': 'SE_Thanksgiving',
        'regions': ['SE'],
        'start_date': datetime(2025, 11, 24),
        'end_date': datetime(2025, 11, 24),
        'multiplier': 1.05
    },
]

DECEMBER_ADJUSTMENTS = [
    {
        'name': 'BA_Platter_Cannibalism',
        'regions': ['BA'],
        'start_date': datetime(2025, 12, 4),
        'end_date': datetime(2025, 12, 7),
        'multiplier': 0.88
    },
]


# =============================================================================
# ADJUSTMENT FUNCTIONS
# =============================================================================

def apply_region_base_cover(row: dict, current_date: datetime, 
                            base_cover: float, base_cover_sold_out: float) -> tuple:
    """
    Apply region-specific base cover adjustments.
    
    Some regions require different coverage levels based on historical performance.
    
    Args:
        row: Item-store data dictionary
        current_date: Current forecast date
        base_cover: Default base cover value
        base_cover_sold_out: Default sold-out cover value
        
    Returns:
        Tuple of (adjusted_base_cover, adjusted_base_cover_sold_out)
    """
    region = row.get('region_code')
    
    # Northeast region gets higher base cover
    if region == 'NE':
        base_cover = 0.07
    
    # Bay Area and LA special dates
    if region in ('BA', 'LA'):
        if datetime(2025, 11, 13) <= current_date <= datetime(2025, 11, 16):
            base_cover = 0.035
            base_cover_sold_out = 0.035
        elif datetime(2025, 11, 17) <= current_date <= datetime(2025, 11, 19):
            base_cover = 0.04
            base_cover_sold_out = 0.04
    
    return base_cover, base_cover_sold_out


def apply_promotional_adjustments(row: dict, current_date: datetime) -> dict:
    """
    Apply promotional uplift adjustments to forecast.
    
    Args:
        row: Item-store data dictionary with forecast_average
        current_date: Current forecast date
        
    Returns:
        Updated row with promotional adjustments applied
    """
    region = row.get('region_code')
    
    # Apply standard promotions
    for promo in PROMOTIONS:
        if region in promo['regions']:
            if promo['start_date'] <= current_date <= promo['end_date']:
                row['forecast_average'] *= promo['multiplier']
    
    return row


def apply_holiday_adjustments(row: dict, current_date: datetime) -> dict:
    """
    Apply holiday-specific adjustments (Thanksgiving, etc.).
    
    Args:
        row: Item-store data dictionary with forecast_average
        current_date: Current forecast date
        
    Returns:
        Updated row with holiday adjustments applied
    """
    region = row.get('region_code')
    
    # Apply Thanksgiving adjustments
    for adj in THANKSGIVING_ADJUSTMENTS:
        if region in adj['regions']:
            if adj['start_date'] <= current_date <= adj['end_date']:
                row['forecast_average'] *= adj['multiplier']
    
    # Apply December adjustments
    for adj in DECEMBER_ADJUSTMENTS:
        if region in adj['regions']:
            if adj['start_date'] <= current_date <= adj['end_date']:
                row['forecast_average'] *= adj['multiplier']
    
    return row


def apply_store_specific_adjustments(row: dict, current_date: datetime) -> dict:
    """
    Apply store-specific forecast adjustments.
    
    Some stores have unique characteristics that require special handling.
    
    Args:
        row: Item-store data dictionary
        current_date: Current forecast date
        
    Returns:
        Updated row with store adjustments applied
    """
    store_no = int(row.get('store_no', 0))
    
    # Specific store groups with temporary adjustments
    high_volume_stores_group1 = [490, 674, 691, 738, 1375, 1653]
    high_volume_stores_group2 = [465, 1058, 427, 481, 644, 436, 736, 1028, 1620, 431, 407, 1079]
    
    if store_no in high_volume_stores_group1:
        if datetime(2025, 10, 30) <= current_date <= datetime(2025, 11, 2):
            if row.get('w1_sold') is not None and row.get('w1_shrink_p') is not None:
                row['forecast_average'] *= 1.20
    
    if store_no in high_volume_stores_group2:
        if datetime(2025, 10, 10) <= current_date <= datetime(2025, 10, 16):
            if row.get('w1_sold') is not None and row.get('w1_shrink_p') is not None:
                row['forecast_average'] *= 1.10
    
    if store_no == 423:
        if datetime(2025, 11, 3) <= current_date <= datetime(2025, 11, 9):
            row['forecast_average'] *= 0.90
    
    return row


def apply_item_specific_adjustments(row: dict, current_date: datetime) -> dict:
    """
    Apply item-specific forecast adjustments.
    
    New items or items with special circumstances may need different treatment.
    
    Args:
        row: Item-store data dictionary
        current_date: Current forecast date
        
    Returns:
        Updated row with item adjustments applied
    """
    item_no = int(row.get('item_no', 0))
    region = row.get('region_code')
    
    # New item launch adjustments
    if item_no == 1984587:
        if current_date <= datetime(2025, 9, 21):
            row['forecast_average'] *= 1.20
        elif datetime(2025, 9, 22) <= current_date <= datetime(2025, 9, 28):
            row['forecast_average'] *= 1.07
        
        # BA region specific
        if region == 'BA':
            if datetime(2025, 11, 17) <= current_date <= datetime(2025, 11, 23):
                row['forecast_average'] *= 1.10
    
    # Salmon Combo (1713314) Texas adjustment
    if item_no == 1713314 and region == 'TE':
        if datetime(2025, 10, 20) <= current_date <= datetime(2025, 11, 2):
            row['forecast_average'] *= 1.30
    
    # Item 1896526 BA adjustment
    if item_no == 1896526 and region == 'BA':
        if datetime(2025, 11, 17) <= current_date <= datetime(2025, 11, 23):
            row['forecast_average'] *= 1.30
    
    # Northeast region boost
    if region == 'NE':
        if datetime(2025, 10, 20) <= current_date <= datetime(2025, 11, 2):
            row['forecast_average'] *= 1.15
    
    return row


def apply_weather_adjustments(row: dict) -> dict:
    """
    Apply weather-based forecast adjustments using OpenWeatherMap severity score.
    
    Weather impacts customer footfall and shopping behavior:
    - Severe weather (severity >= 8): Major reduction
    - Moderate weather (severity 5-7): Moderate reduction
    - Light weather (severity 3-4): Minor reduction
    - Alerts present: Additional reduction
    - Snow/Ice: Significant impact
    
    Args:
        row: Item-store data dictionary with weather features
        
    Returns:
        Updated row with weather adjustments applied
    """
    # Get OpenWeatherMap severity score (0-10 scale)
    severity_score = row.get('owm_severity_score', 0) or 0
    has_alerts = row.get('owm_has_alerts', 0)
    snow_expected = row.get('owm_total_snow_expected', 0) or 0
    rain_expected = row.get('owm_total_rain_expected', 0) or 0
    
    # Base multiplier (no adjustment)
    weather_multiplier = 1.0
    
    # Apply severity-based adjustments
    if severity_score >= 8:
        # Severe weather: 20-30% reduction
        weather_multiplier = 0.70
    elif severity_score >= 6:
        # Moderate to severe: 10-20% reduction
        weather_multiplier = 0.85
    elif severity_score >= 4:
        # Moderate weather: 5-10% reduction
        weather_multiplier = 0.92
    elif severity_score >= 2:
        # Light weather impact: 2-5% reduction
        weather_multiplier = 0.97
    
    # Additional reduction for official weather alerts
    if has_alerts:
        weather_multiplier *= 0.92
    
    # Snow is a major barrier - additional reduction
    if snow_expected >= 6.0:
        weather_multiplier *= 0.70  # Blizzard conditions
    elif snow_expected >= 2.0:
        weather_multiplier *= 0.85  # Significant snowfall
    elif snow_expected >= 0.5:
        weather_multiplier *= 0.95  # Light snow
    
    # Heavy rain additional impact
    if rain_expected >= 1.5:
        weather_multiplier *= 0.90  # Washout
    
    # Store the weather multiplier for visibility
    row['weather_adjustment_multiplier'] = round(weather_multiplier, 4)
    
    # Apply weather adjustment to forecast
    if 'forecast_average' in row and row['forecast_average'] is not None:
        row['forecast_average'] *= weather_multiplier
    
    return row


def apply_all_adjustments(row: dict, current_date: datetime,
                          base_cover: float, base_cover_sold_out: float) -> dict:
    """
    Apply all forecast adjustments in the correct order.
    
    Order of operations:
    1. Region base cover adjustments
    2. Promotional uplifts
    3. Holiday adjustments
    4. Store-specific adjustments
    5. Item-specific adjustments
    6. Weather-based adjustments
    
    Args:
        row: Item-store data dictionary with base forecast
        current_date: Current forecast date
        base_cover: Default base coverage
        base_cover_sold_out: Default sold-out coverage
        
    Returns:
        Updated row with all adjustments applied
    """
    # Get adjusted base covers
    adj_base_cover, adj_base_cover_sold_out = apply_region_base_cover(
        row, current_date, base_cover, base_cover_sold_out
    )
    row['base_cover'] = adj_base_cover
    row['base_cover_sold_out'] = adj_base_cover_sold_out
    
    # Apply promotional uplifts
    row = apply_promotional_adjustments(row, current_date)
    
    # Apply holiday adjustments
    row = apply_holiday_adjustments(row, current_date)
    
    # Apply store-specific adjustments
    row = apply_store_specific_adjustments(row, current_date)
    
    # Apply item-specific adjustments
    row = apply_item_specific_adjustments(row, current_date)
    
    # Apply weather-based adjustments (after business rules)
    row = apply_weather_adjustments(row)
    
    return row


# =============================================================================
# STORE-LEVEL PASS ADJUSTMENTS
# =============================================================================
# These are applied AFTER the base forecast is calculated but BEFORE weather
# adjustments, to ensure store-level shrink and coverage targets are met.

# Store-level shrink pass logic:
# - Reduces forecast for stores where expected shrink > threshold (20%)
# - Adjusts ALL items (not just non-hero items) starting from highest coverage
# - Prioritizes items with highest coverage (most over-forecasted relative to avg)
# - Maintains minimum 1 case per item to ensure store availability

# Store-level pass thresholds
STORE_SHRINK_THRESHOLD = 0.20  # 20% - max acceptable forecast shrink at store level
STORE_MIN_COVERAGE = 0.00      # 0% - minimum coverage required (trigger for increase)


def calculate_store_level_metrics(indexed_rows: List[Tuple[int, dict]]) -> Tuple[float, float, float, float]:
    """
    Calculate store-level forecast metrics for a group of items.
    
    Args:
        indexed_rows: List of (index, row) tuples for a store-date
        
    Returns:
        Tuple of (total_forecast_qty, total_w1_sold, store_shrink_pct, min_item_coverage)
    """
    total_forecast_qty = 0
    total_w1_sold = 0
    min_item_coverage = float('inf')
    
    for _, row in indexed_rows:
        forecast_qty = row.get('forecast_quantity', 0) or 0
        w1_sold = row.get('w1_sold', 0) or 0
        forecast_avg = row.get('forecast_average', 0) or 0
        
        total_forecast_qty += forecast_qty
        total_w1_sold += w1_sold
        
        # Calculate item-level coverage (forecast vs average)
        if forecast_avg > 0:
            coverage = forecast_qty / forecast_avg
            min_item_coverage = min(min_item_coverage, coverage)
    
    # Calculate store-level shrink percentage
    if total_forecast_qty > 0:
        store_shrink_pct = (total_forecast_qty - total_w1_sold) / total_forecast_qty
    else:
        store_shrink_pct = 0
    
    if min_item_coverage == float('inf'):
        min_item_coverage = 0
    
    return total_forecast_qty, total_w1_sold, store_shrink_pct, min_item_coverage


def apply_store_level_shrink_pass(
    forecast_results: List[dict],
    shrink_threshold: float = STORE_SHRINK_THRESHOLD,
    verbose: bool = True
) -> List[dict]:
    """
    Apply store-level shrink pass to reduce forecast when store shrink % > threshold.
    
    This function:
    1. Groups forecasts by store-date
    2. Calculates store-level forecast shrink % = (forecast_qty - w1_sold) / forecast_qty
    3. If shrink % > threshold (default 20%), iteratively reduces items by 1 case
    4. Prioritizes items with highest coverage (forecast/avg) - most over-forecasted first
    5. Maintains minimum 1 case per item (only reduces items with >= 2 cases)
    6. Continues until shrink % <= threshold or no more items can be reduced
    
    Args:
        forecast_results: List of forecast result dictionaries
        shrink_threshold: Maximum acceptable store-level shrink percentage (default 0.20)
        verbose: Print detailed adjustment information
        
    Returns:
        Updated forecast results with store-level adjustments applied
    """
    if verbose:
        print("\n" + "=" * 60)
        print("STORE-LEVEL SHRINK PASS")
        print("=" * 60)
        print(f"Shrink threshold: {shrink_threshold:.0%}")
    
    # Group results by store-date
    store_date_groups = {}
    for i, row in enumerate(forecast_results):
        store_no = str(row.get('store_no', ''))
        date_forecast = str(row.get('date_forecast', ''))
        key = (store_no, date_forecast)
        
        if key not in store_date_groups:
            store_date_groups[key] = []
        store_date_groups[key].append((i, deepcopy(row)))
    
    # Track statistics
    stats = {
        'stores_evaluated': 0,
        'stores_adjusted': 0,
        'items_adjusted': 0,
        'total_cases_reduced': 0,
        'total_units_reduced': 0,
    }
    
    adjusted_results = [None] * len(forecast_results)
    
    for key, indexed_rows in store_date_groups.items():
        store_no, date_forecast = key
        stats['stores_evaluated'] += 1
        
        # Initialize store-level pass fields for all rows
        for idx, row in indexed_rows:
            row['forecast_qty_pre_store_pass'] = row.get('forecast_quantity', 0)
            row['store_level_adjustment_qty'] = 0
            row['store_level_adjustment_reason'] = ''
            row['store_level_adjusted'] = 0
        
        # Calculate initial store-level metrics
        total_forecast, total_sold, store_shrink_pct, min_coverage = calculate_store_level_metrics(indexed_rows)
        
        # Store the metrics in rows
        for idx, row in indexed_rows:
            row['store_level_shrink_pct'] = store_shrink_pct
            row['store_level_coverage_pct'] = min_coverage
        
        # Skip if already below threshold
        if store_shrink_pct <= shrink_threshold:
            for idx, row in indexed_rows:
                row['store_level_adjustment_reason'] = f"Shrink {store_shrink_pct:.1%} within threshold"
                adjusted_results[idx] = row
            continue
        
        if verbose and store_shrink_pct > shrink_threshold:
            print(f"\n  Store {store_no} | {date_forecast}")
            print(f"    Initial shrink: {store_shrink_pct:.1%} (threshold: {shrink_threshold:.0%})")
            print(f"    Store forecast: {total_forecast}, W1 Sold: {total_sold}")
        
        # Iteratively reduce items until shrink <= threshold
        # Priority: reduce items with HIGHEST coverage first (most over-forecasted)
        # IMPORTANT CONSTRAINTS:
        # 1. Keep at least 1 case per item
        # 2. NEVER reduce forecast below forecast_average (expected shrink must stay >= 0%)
        iteration = 0
        max_iterations = 100  # Safety limit
        total_reduced = 0
        
        while store_shrink_pct > shrink_threshold and iteration < max_iterations:
            iteration += 1
            
            # Find ALL items that can be reduced
            # Constraints:
            # - At least 2 cases (to keep minimum 1)
            # - After reduction, forecast must be >= forecast_average (expected shrink >= 0%)
            reducible_items = []
            for idx, row in indexed_rows:
                item_no = int(row.get('item_no', 0))
                forecast_qty = row.get('forecast_quantity', 0) or 0
                forecast_avg = row.get('forecast_average', 0) or 0
                case_pack_size = row.get('case_pack_size', 6) or 6
                current_cases = forecast_qty / case_pack_size if case_pack_size > 0 else 0
                
                # Calculate what forecast would be after removing 1 case
                forecast_after_reduction = forecast_qty - case_pack_size
                
                # Only reduce items if:
                # 1. At least 2 cases (keep minimum 1 case)
                # 2. After reduction, forecast >= forecast_average (expected shrink >= 0%)
                if current_cases >= 2 and forecast_after_reduction >= forecast_avg:
                    # Calculate coverage (forecast / average) - higher = more over-forecasted
                    coverage = forecast_qty / forecast_avg if forecast_avg > 0 else 1.0
                    
                    # Also calculate item-level shrink for secondary sorting
                    shrink_lw = row.get('forecast_shrink_last_week_sales', 0) or 0
                    shrink_avg = row.get('forecast_shrink_average', 0) or 0
                    item_shrink = max(shrink_lw, shrink_avg)
                    
                    reducible_items.append({
                        'idx': idx,
                        'row': row,
                        'coverage': coverage,
                        'shrink': item_shrink,
                        'case_pack_size': case_pack_size,
                        'cases': current_cases,
                        'item_no': item_no
                    })
            
            # Sort by coverage (highest first) - reduce most over-forecasted items first
            # Secondary sort by shrink (highest first) for items with same coverage
            reducible_items.sort(key=lambda x: (x['coverage'], x['shrink']), reverse=True)
            
            # If no items can be reduced, stop
            if not reducible_items:
                break
            
            # Reduce 1 case from the highest coverage item
            top_item = reducible_items[0]
            case_size = top_item['case_pack_size']
            
            # Apply reduction
            top_item['row']['forecast_quantity'] -= case_size
            top_item['row']['store_level_adjustment_qty'] += case_size
            top_item['row']['store_level_adjusted'] = 1
            
            total_reduced += case_size
            
            # Recalculate store-level metrics
            _, _, store_shrink_pct, _ = calculate_store_level_metrics(indexed_rows)
            
            # Update reason
            top_item['row']['store_level_adjustment_reason'] = (
                f"Store shrink pass: reduced {top_item['row']['store_level_adjustment_qty']} units "
                f"({top_item['row']['store_level_adjustment_qty'] // case_size} cases). "
                f"Coverage: {top_item['coverage']:.1f}x, Store shrink: {store_shrink_pct:.1%}"
            )
        
        # Update final metrics for all rows
        for idx, row in indexed_rows:
            row['store_level_shrink_pct'] = store_shrink_pct
            adjusted_results[idx] = row
        
        # Track statistics
        store_adjusted = False
        for idx, row in indexed_rows:
            if row.get('store_level_adjusted', 0):
                stats['items_adjusted'] += 1
                stats['total_units_reduced'] += row.get('store_level_adjustment_qty', 0)
                stats['total_cases_reduced'] += row.get('store_level_adjustment_qty', 0) // (row.get('case_pack_size', 6) or 6)
                store_adjusted = True
        
        if store_adjusted:
            stats['stores_adjusted'] += 1
            if verbose:
                print(f"    Final shrink: {store_shrink_pct:.1%}")
                print(f"    Reduced: {total_reduced} units in {iteration} iterations")
                if store_shrink_pct > shrink_threshold:
                    print("    NOTE: Couldn't reach target (can't reduce items below expected sales)")
    
    # Print summary
    if verbose:
        print("\nStore-Level Shrink Pass Summary:")
        print(f"  Stores evaluated: {stats['stores_evaluated']}")
        print(f"  Stores adjusted: {stats['stores_adjusted']}")
        print(f"  Items adjusted: {stats['items_adjusted']}")
        print(f"  Total units reduced: {stats['total_units_reduced']}")
        print(f"  Total cases reduced: {stats['total_cases_reduced']}")
    
    return adjusted_results


def apply_store_level_coverage_pass(
    forecast_results: List[dict],
    min_coverage: float = STORE_MIN_COVERAGE,
    verbose: bool = True
) -> List[dict]:
    """
    Apply store-level coverage pass to increase items when store has 0% coverage.
    
    This function:
    1. Groups forecasts by store-date
    2. Identifies items with 0% coverage (forecast_qty = 0)
    3. Increases the item with lowest coverage by 1 case to ensure some coverage
    
    Args:
        forecast_results: List of forecast result dictionaries
        min_coverage: Minimum coverage threshold (default 0% - only add when no coverage)
        verbose: Print detailed adjustment information
        
    Returns:
        Updated forecast results with coverage adjustments applied
    """
    if verbose:
        print("\n" + "=" * 60)
        print("STORE-LEVEL COVERAGE PASS")
        print("=" * 60)
        print(f"Min coverage threshold: {min_coverage:.0%}")
    
    # Group results by store-date
    store_date_groups = {}
    for i, row in enumerate(forecast_results):
        store_no = str(row.get('store_no', ''))
        date_forecast = str(row.get('date_forecast', ''))
        key = (store_no, date_forecast)
        
        if key not in store_date_groups:
            store_date_groups[key] = []
        store_date_groups[key].append((i, row))  # Use existing row reference
    
    # Track statistics
    stats = {
        'stores_evaluated': 0,
        'stores_adjusted': 0,
        'items_adjusted': 0,
        'total_cases_added': 0,
        'total_units_added': 0,
    }
    
    for key, indexed_rows in store_date_groups.items():
        store_no, date_forecast = key
        stats['stores_evaluated'] += 1
        
        # Calculate store-level coverage
        _, _, _, store_min_coverage = calculate_store_level_metrics(indexed_rows)
        
        # Check if any item has 0% coverage (forecast_qty = 0 but forecast_average > 0)
        zero_coverage_items = []
        for idx, row in indexed_rows:
            forecast_qty = row.get('forecast_quantity', 0) or 0
            forecast_avg = row.get('forecast_average', 0) or 0
            case_pack_size = row.get('case_pack_size', 6) or 6
            
            # Item has demand (forecast_avg > 0) but no forecast quantity
            if forecast_qty == 0 and forecast_avg > 0:
                item_coverage = 0
                zero_coverage_items.append({
                    'idx': idx,
                    'row': row,
                    'coverage': item_coverage,
                    'case_pack_size': case_pack_size,
                    'forecast_avg': forecast_avg
                })
        
        # Skip if no zero-coverage items
        if not zero_coverage_items:
            continue
        
        # Sort by forecast_average (highest demand first)
        zero_coverage_items.sort(key=lambda x: x['forecast_avg'], reverse=True)
        
        if verbose:
            print(f"\n  Store {store_no} | {date_forecast}")
            print(f"    Found {len(zero_coverage_items)} items with 0% coverage")
        
        # Increase the highest demand item with 0% coverage by 1 case
        top_item = zero_coverage_items[0]
        case_size = top_item['case_pack_size']
        
        # Apply increase
        top_item['row']['forecast_quantity'] = (top_item['row'].get('forecast_quantity', 0) or 0) + case_size
        
        # Track adjustment - negative value indicates increase
        current_adj = top_item['row'].get('store_level_adjustment_qty', 0) or 0
        top_item['row']['store_level_adjustment_qty'] = current_adj - case_size  # Negative = increase
        top_item['row']['store_level_adjusted'] = 1
        
        # Update reason
        existing_reason = top_item['row'].get('store_level_adjustment_reason', '') or ''
        coverage_reason = f"Coverage pass: added {case_size} units (1 case) for 0% coverage item."
        if existing_reason:
            top_item['row']['store_level_adjustment_reason'] = f"{existing_reason} | {coverage_reason}"
        else:
            top_item['row']['store_level_adjustment_reason'] = coverage_reason
        
        # Update statistics
        stats['stores_adjusted'] += 1
        stats['items_adjusted'] += 1
        stats['total_cases_added'] += 1
        stats['total_units_added'] += case_size
        
        if verbose:
            print(f"    Added 1 case ({case_size} units) to item {top_item['row'].get('item_no')}")
    
    # Print summary
    if verbose:
        print("\nStore-Level Coverage Pass Summary:")
        print(f"  Stores evaluated: {stats['stores_evaluated']}")
        print(f"  Stores adjusted: {stats['stores_adjusted']}")
        print(f"  Items adjusted: {stats['items_adjusted']}")
        print(f"  Total units added: {stats['total_units_added']}")
        print(f"  Total cases added: {stats['total_cases_added']}")
    
    return forecast_results


def apply_store_level_pass(
    forecast_results: List[dict],
    shrink_threshold: float = STORE_SHRINK_THRESHOLD,
    verbose: bool = True
) -> List[dict]:
    """
    Apply complete store-level pass including shrink reduction and coverage increase.
    
    This is the main entry point that:
    1. First applies shrink pass to reduce high-shrink stores
    2. Then applies coverage pass to add items for 0% coverage stores
    
    Args:
        forecast_results: List of forecast result dictionaries
        shrink_threshold: Maximum acceptable store-level shrink percentage (default 0.20)
        verbose: Print detailed adjustment information
        
    Returns:
        Updated forecast results with store-level adjustments applied
    """
    # Step 1: Apply shrink pass (reduce NON_HERO items if store shrink > threshold)
    forecast_results = apply_store_level_shrink_pass(
        forecast_results, 
        shrink_threshold=shrink_threshold,
        verbose=verbose
    )
    
    # Step 2: Apply coverage pass (add items if any have 0% coverage)
    forecast_results = apply_store_level_coverage_pass(
        forecast_results,
        verbose=verbose
    )
    
    return forecast_results


def get_store_level_adjustment_summary(forecast_results: List[dict]) -> dict:
    """
    Generate summary statistics for store-level adjustments.
    
    Args:
        forecast_results: List of adjusted forecast results
        
    Returns:
        Dictionary with adjustment statistics
    """
    summary = {
        'total_rows': len(forecast_results),
        'adjusted_rows': 0,
        'total_pre_store_pass_qty': 0,
        'total_post_store_pass_qty': 0,
        'total_reduction': 0,
        'total_increase': 0,
        'by_store': {},
        'by_date': {},
    }
    
    for row in forecast_results:
        pre_qty = row.get('forecast_qty_pre_store_pass', 0) or 0
        post_qty = row.get('forecast_quantity', 0) or 0
        adjustment = row.get('store_level_adjustment_qty', 0) or 0
        store = str(row.get('store_no', 'UNKNOWN'))
        date = str(row.get('date_forecast', 'UNKNOWN'))
        
        summary['total_pre_store_pass_qty'] += pre_qty
        summary['total_post_store_pass_qty'] += post_qty
        
        if adjustment > 0:
            summary['total_reduction'] += adjustment
            summary['adjusted_rows'] += 1
        elif adjustment < 0:
            summary['total_increase'] += abs(adjustment)
            summary['adjusted_rows'] += 1
        
        # By store
        if store not in summary['by_store']:
            summary['by_store'][store] = {'reduction': 0, 'increase': 0}
        if adjustment > 0:
            summary['by_store'][store]['reduction'] += adjustment
        elif adjustment < 0:
            summary['by_store'][store]['increase'] += abs(adjustment)
        
        # By date
        if date not in summary['by_date']:
            summary['by_date'][date] = {'reduction': 0, 'increase': 0}
        if adjustment > 0:
            summary['by_date'][date]['reduction'] += adjustment
        elif adjustment < 0:
            summary['by_date'][date]['increase'] += abs(adjustment)
    
    return summary

