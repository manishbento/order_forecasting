"""
Weather-Based Forecast Adjustment Module
========================================
Applies weather severity adjustments to forecasted quantities at the STORE level.

This module is executed as a separate step in the forecasting pipeline,
AFTER the base forecast is calculated. It adjusts quantities based on:

1. Weather severity score from VisualCrossing (threshold >= 4.0)
2. Store-level target reduction based on expected sales impact
3. Item prioritization: Non-hero items first, then by forecast coverage
4. Iterative case-by-case reduction across prioritized items
5. GUARANTEE: Minimum 1 item × 1 case reduction when severity > 4

The adjustment process:
1. Identify store-days with significant weather impact (severity >= threshold)
2. Calculate STORE-LEVEL target reduction based on severity and sales impact
3. Prioritize items for reduction:
   - Non-hero items get higher priority (adjusted first)
   - Higher forecast coverage items get adjusted before lower coverage
   - Items with more available cases get more flexibility
4. Apply reductions iteratively, 1 case at a time per item per pass
5. Multiple passes through all items until target reduction met
6. GUARANTEE: If severity > 4 and no adjustments made, force 1 case reduction
7. Track pre/post adjustment quantities and reasons for transparency

Key Changes (v2 - Store-Level Approach):
- Weather adjustment now runs at store level, not item level
- Iterative multi-pass reduction across all items
- Priority: Non-hero items first, then by forecast coverage (high to low)
- Guarantee: Minimum 1 item × 1 case when severity > 4

Usage:
    from forecasting.weather_adjustment import apply_weather_adjustments
    
    adjusted_results = apply_weather_adjustments(
        forecast_results, 
        weather_data,
        severity_threshold=4.0
    )
"""

from typing import Dict, List, Tuple
from copy import deepcopy


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default thresholds for weather adjustment
DEFAULT_SEVERITY_THRESHOLD = 4.0      # Minimum severity to trigger adjustment
DEFAULT_MAX_STORE_REDUCTION_PCT = 0.40  # Max reduction at store level (40%)

# Weather unicode icons for status indicator
WEATHER_ICONS = {
    'clear': '☀️',
    'partly_cloudy': '⛅',
    'cloudy': '☁️',
    'rain_light': '🌦️',
    'rain': '🌧️',
    'rain_heavy': '⛈️',
    'thunderstorm': '🌩️',
    'snow_light': '🌨️',
    'snow': '❄️',
    'snow_heavy': '🌨️❄️',
    'fog': '🌫️',
    'wind': '💨',
    'extreme_cold': '🥶',
    'extreme_heat': '🥵',
    'severe': '⚠️',
    'unknown': '❓'
}

# Severity category to icon mapping
SEVERITY_ICONS = {
    'MINIMAL': '✅',
    'LOW': '🟢',
    'MODERATE': '🟡',
    'HIGH': '🟠',
    'SEVERE': '🔴'
}


# =============================================================================
# WEATHER STATUS INDICATOR FUNCTIONS
# =============================================================================

def get_weather_icon(condition: str, severity_score: float = 0,
                     rain_severity: float = 0, snow_severity: float = 0,
                     wind_severity: float = 0, temp_severity: float = 0,
                     temp_max: float = None, temp_min: float = None) -> str:
    """
    Get appropriate weather icon based on conditions.
    
    Args:
        condition: Weather condition string
        severity_score: Overall severity score
        rain_severity: Rain severity component
        snow_severity: Snow severity component
        wind_severity: Wind severity component
        temp_severity: Temperature severity component
        temp_max: Maximum temperature
        temp_min: Minimum temperature
        
    Returns:
        Unicode weather icon string
    """
    if not condition:
        return WEATHER_ICONS['unknown']
    
    condition_lower = condition.lower()
    
    # Check for severe conditions first
    if severity_score >= 7:
        return WEATHER_ICONS['severe']
    
    # Snow conditions
    if snow_severity >= 6 or 'blizzard' in condition_lower:
        return WEATHER_ICONS['snow_heavy']
    if snow_severity >= 3 or 'snow' in condition_lower:
        return WEATHER_ICONS['snow']
    if snow_severity > 0:
        return WEATHER_ICONS['snow_light']
    
    # Rain/storm conditions
    if 'thunder' in condition_lower or 'storm' in condition_lower:
        return WEATHER_ICONS['thunderstorm']
    if rain_severity >= 6:
        return WEATHER_ICONS['rain_heavy']
    if rain_severity >= 3 or 'rain' in condition_lower:
        return WEATHER_ICONS['rain']
    if rain_severity > 0 or 'drizzle' in condition_lower or 'shower' in condition_lower:
        return WEATHER_ICONS['rain_light']
    
    # Fog
    if 'fog' in condition_lower or 'mist' in condition_lower:
        return WEATHER_ICONS['fog']
    
    # Wind
    if wind_severity >= 5:
        return WEATHER_ICONS['wind']
    
    # Temperature extremes
    if temp_min is not None and temp_min < 20:
        return WEATHER_ICONS['extreme_cold']
    if temp_max is not None and temp_max > 100:
        return WEATHER_ICONS['extreme_heat']
    
    # Clear/cloudy
    if 'clear' in condition_lower or 'sunny' in condition_lower:
        return WEATHER_ICONS['clear']
    if 'partly' in condition_lower or 'partial' in condition_lower:
        return WEATHER_ICONS['partly_cloudy']
    if 'cloud' in condition_lower or 'overcast' in condition_lower:
        return WEATHER_ICONS['cloudy']
    
    return WEATHER_ICONS['clear']


def build_weather_status_indicator(row: dict) -> str:
    """
    Build a comprehensive weather status indicator string for Excel export.
    
    Format: [ICON] Condition | Severity: X.X (CATEGORY) | Impact: X%
    
    Args:
        row: Forecast row with weather data
        
    Returns:
        Formatted weather status string
    """
    condition = row.get('weather_day_condition', '') or ''
    severity_score = row.get('weather_severity_score') or 0
    severity_category = row.get('weather_severity_category', 'MINIMAL') or 'MINIMAL'
    sales_impact = row.get('weather_sales_impact_factor', 1.0) or 1.0
    rain_severity = row.get('weather_rain_severity', 0) or 0
    snow_severity = row.get('weather_snow_severity', 0) or 0
    wind_severity = row.get('weather_wind_severity', 0) or 0
    temp_severity = row.get('weather_temp_severity', 0) or 0
    temp_max = row.get('weather_temp_max')
    temp_min = row.get('weather_temp_min')
    total_rain = row.get('weather_total_rain_expected', 0) or 0
    snow_amount = row.get('weather_snow_amount', 0) or 0
    wind_speed = row.get('weather_wind_speed', 0) or 0
    weather_adjusted = row.get('weather_adjusted', 0)
    adjustment_qty = row.get('weather_adjustment_qty', 0) or 0
    
    # Get icons
    weather_icon = get_weather_icon(
        condition, severity_score, rain_severity, snow_severity,
        wind_severity, temp_severity, temp_max, temp_min
    )
    severity_icon = SEVERITY_ICONS.get(severity_category, '❓')
    
    # Build status parts
    parts = []
    
    # Main condition
    if condition:
        parts.append(f"{weather_icon} {condition}")
    else:
        parts.append(f"{weather_icon} No data")
    
    # Severity info
    if severity_score > 0:
        parts.append(f"Severity: {severity_score:.1f} {severity_icon}")
    
    # Weather details (only if significant)
    details = []
    if total_rain > 0.1:
        details.append(f"Rain: {total_rain:.2f}\"")
    if snow_amount > 0:
        details.append(f"Snow: {snow_amount:.1f}\"")
    if wind_speed > 15:
        details.append(f"Wind: {wind_speed:.0f}mph")
    if temp_max is not None and temp_min is not None:
        details.append(f"Temp: {temp_min:.0f}°-{temp_max:.0f}°F")
    
    if details:
        parts.append(" | ".join(details))
    
    # Adjustment info
    if weather_adjusted and adjustment_qty > 0:
        impact_pct = (1 - sales_impact) * 100
        parts.append(f"⬇️ Adjusted: -{adjustment_qty} units ({impact_pct:.0f}% impact)")
    
    return " | ".join(parts)


def build_sales_trend_string(w4_sold, w3_sold, w2_sold, w1_sold) -> str:
    """
    Build a visual sales trend string showing 4 weeks of data.
    
    Format: W4 → W3 → W2 → W1 (oldest to newest)
    
    Args:
        w4_sold: Week 4 sales (oldest)
        w3_sold: Week 3 sales
        w2_sold: Week 2 sales
        w1_sold: Week 1 sales (most recent)
        
    Returns:
        Formatted trend string like "10 → 15 → 20 → 25"
    """
    def fmt(val):
        if val is None:
            return "-"
        return str(int(val))
    
    return f"{fmt(w4_sold)} → {fmt(w3_sold)} → {fmt(w2_sold)} → {fmt(w1_sold)}"


# =============================================================================
# PRIORITY CALCULATION FUNCTIONS  
# =============================================================================

def calculate_item_reduction_priority(
    forecast_shrink_last_week: float,
    forecast_shrink_average: float,
    forecast_qty: int,
    case_pack_size: int
) -> tuple:
    """
    Calculate reduction priority score for an item based on forecast shrink metrics.
    
    Items with higher forecast shrink have more "room" for reduction without
    risking stockouts, so they get higher priority.
    
    Args:
        forecast_shrink_last_week: (forecast_qty - w1_sold) / forecast_qty
        forecast_shrink_average: (forecast_qty - forecast_avg) / forecast_qty
        forecast_qty: Current forecast quantity
        case_pack_size: Case pack size
        
    Returns:
        Tuple of (priority_score, shrink_headroom, can_reduce)
    """
    # Use max of the two shrink metrics as the "headroom" indicator
    shrink_headroom = max(
        forecast_shrink_last_week or 0,
        forecast_shrink_average or 0
    )
    
    # Can only reduce if we have at least 2 cases (leave minimum 1 case)
    min_cases = 2
    current_cases = forecast_qty / case_pack_size if case_pack_size > 0 else 0
    can_reduce = current_cases >= min_cases
    
    # Priority score: higher shrink headroom = higher priority
    # Also factor in number of cases (more cases = more flexibility)
    case_factor = min(2.0, 1.0 + (current_cases - 2) * 0.1) if current_cases > 2 else 1.0
    
    priority_score = shrink_headroom * case_factor if can_reduce else 0
    
    return priority_score, shrink_headroom, can_reduce


def recalculate_forecast_shrink_metrics(row: dict) -> dict:
    """
    Recalculate forecast shrink metrics after adjustment.
    
    Args:
        row: Forecast row with updated forecast_quantity
        
    Returns:
        Updated row with recalculated shrink metrics
    """
    forecast_qty = row.get('forecast_quantity', 0) or 0
    w1_sold = row.get('w1_sold', 0) or 0
    forecast_avg = row.get('forecast_average', 0) or 0
    
    if forecast_qty > 0:
        row['forecast_shrink_last_week_sales'] = (forecast_qty - w1_sold) / forecast_qty if w1_sold > 0 else 0
        row['forecast_shrink_average'] = (forecast_qty - forecast_avg) / forecast_qty if forecast_avg > 0 else 0
    else:
        row['forecast_shrink_last_week_sales'] = 0
        row['forecast_shrink_average'] = 0
    
    return row


# =============================================================================
# ITEM PRIORITIZATION FOR STORE-LEVEL ADJUSTMENT
# =============================================================================

def prioritize_items_for_reduction(indexed_rows: List[Tuple[int, dict]]) -> List[Tuple[int, dict, float]]:
    """
    Prioritize items for weather reduction at the store level.
    
    Priority order (highest to lowest):
    1. Non-Hero items first (hero_item != 1)
    2. Higher forecast coverage (forecast_coverage)
    3. More available cases to reduce
    
    Args:
        indexed_rows: List of (index, row) tuples for a store-date
        
    Returns:
        List of (index, row, priority_score) tuples sorted by priority (highest first)
    """
    prioritized = []
    
    for idx, row in indexed_rows:
        forecast_qty = row.get('forecast_quantity', 0) or 0
        case_pack_size = row.get('case_pack_size', 6) or 6
        
        # Check if item can be reduced (need at least 2 cases)
        current_cases = forecast_qty / case_pack_size
        can_reduce = current_cases >= 2
        
        if not can_reduce or forecast_qty <= 0:
            # Item cannot be reduced - priority 0
            prioritized.append((idx, row, 0.0))
            continue
        
        # Hero item flag (1 = hero, 0 or None = non-hero)
        is_hero = row.get('hero_item', 0) == 1
        
        # Forecast coverage (higher = more likely to sell out = reduce with caution)
        # But we prioritize higher coverage for reduction because we want to prevent overstocking
        forecast_coverage = row.get('forecast_coverage', 0) or 0
        
        # Available cases to reduce (leave minimum 1 case)
        available_cases = current_cases - 1
        
        # Priority calculation:
        # - Non-hero items get +100 base priority (adjusted first)
        # - Higher coverage adds to priority (0-50 range based on coverage %)
        # - More available cases adds flexibility (0-10 range)
        
        hero_penalty = 0 if not is_hero else 100  # Hero items get lower priority
        coverage_score = min(50, forecast_coverage * 50) if forecast_coverage else 0
        flexibility_score = min(10, available_cases * 2)
        
        priority_score = 100 - hero_penalty + coverage_score + flexibility_score
        
        prioritized.append((idx, row, priority_score))
    
    # Sort by priority (highest first)
    prioritized.sort(key=lambda x: x[2], reverse=True)
    
    return prioritized


def apply_store_level_reduction(
    indexed_rows: List[Tuple[int, dict]],
    target_reduction_units: float,
    severity_score: float,
    severity_category: str,
    target_reduction_pct: float,
    verbose: bool = False,
    store_no: str = '',
    date_forecast: str = ''
) -> Tuple[List[Tuple[int, dict]], int, int]:
    """
    Apply weather reduction at the store level using iterative multi-pass approach.
    
    This function:
    1. Prioritizes items for reduction (non-hero, high coverage first)
    2. Iteratively reduces 1 case at a time from highest priority reducible items
    3. Continues passes until target reduction met or no more reductions possible
    4. Guarantees minimum 1 item × 1 case reduction when severity > 4
    
    Args:
        indexed_rows: List of (index, row) tuples for a store-date
        target_reduction_units: Total units to reduce across all items
        severity_score: Weather severity score
        severity_category: Weather severity category
        target_reduction_pct: Target reduction percentage
        verbose: Print detailed output
        store_no: Store number for logging
        date_forecast: Forecast date for logging
        
    Returns:
        Tuple of (updated_rows, items_adjusted, total_cases_reduced)
    """
    # Prioritize items for reduction
    prioritized = prioritize_items_for_reduction(indexed_rows)
    
    # Track total reduction achieved
    total_reduced_units = 0
    total_cases_reduced = 0
    items_adjusted = 0
    items_with_adjustment = set()
    
    # Create lookup for easy row access
    row_lookup = {idx: row for idx, row, _ in prioritized}
    
    # Track reduction per item
    reduction_per_item = {idx: 0 for idx, _, _ in prioritized}
    
    # Iterative multi-pass reduction
    max_passes = 50  # Safety limit
    current_pass = 0
    
    while total_reduced_units < target_reduction_units and current_pass < max_passes:
        current_pass += 1
        made_reduction_this_pass = False
        
        for idx, row, priority in prioritized:
            if priority <= 0:
                continue  # Skip items that can't be reduced
                
            if total_reduced_units >= target_reduction_units:
                break
            
            # Get current state of this item
            forecast_qty = row.get('forecast_quantity', 0) or 0
            case_pack_size = row.get('case_pack_size', 6) or 6
            current_cases = forecast_qty / case_pack_size
            
            # Check if we can still reduce this item (keep minimum 1 case)
            if current_cases < 2:
                continue
            
            # Reduce by 1 case
            reduction_units = case_pack_size
            row['forecast_quantity'] = forecast_qty - reduction_units
            
            total_reduced_units += reduction_units
            total_cases_reduced += 1
            reduction_per_item[idx] += reduction_units
            items_with_adjustment.add(idx)
            made_reduction_this_pass = True
        
        if not made_reduction_this_pass:
            break  # No more reductions possible
    
    # GUARANTEE: If severity > 4 and no adjustments made, force minimum 1 item × 1 case
    if severity_score > 4 and total_cases_reduced == 0:
        # Find any item with at least 2 cases to reduce
        for idx, row, priority in prioritized:
            forecast_qty = row.get('forecast_quantity', 0) or 0
            case_pack_size = row.get('case_pack_size', 6) or 6
            current_cases = forecast_qty / case_pack_size
            
            if current_cases >= 2:
                # Force reduction of 1 case
                reduction_units = case_pack_size
                row['forecast_quantity'] = forecast_qty - reduction_units
                total_reduced_units += reduction_units
                total_cases_reduced += 1
                reduction_per_item[idx] += reduction_units
                items_with_adjustment.add(idx)
                
                if verbose:
                    print(f"    [GUARANTEE] Forced 1 case reduction on item (severity {severity_score:.1f} > 4)")
                break
    
    # Update adjustment tracking on all rows
    for idx, row, _ in prioritized:
        reduction = reduction_per_item.get(idx, 0)
        
        if reduction > 0:
            case_pack_size = row.get('case_pack_size', 6) or 6
            cases_reduced = reduction // case_pack_size
            
            # Store as NEGATIVE value for waterfall (reduction = decrease in forecast)
            row['weather_adjustment_qty'] = -reduction
            row['weather_adjusted'] = 1
            row['weather_adjustment_reason'] = (
                f"Weather severity {severity_score:.1f} ({severity_category}). "
                f"Reduced {reduction} units ({cases_reduced} case{'s' if cases_reduced > 1 else ''}) "
                f"based on {target_reduction_pct:.0%} expected sales impact."
            )
            
            # Recalculate shrink metrics after reduction
            recalculate_forecast_shrink_metrics(row)
            items_adjusted += 1
        else:
            # No adjustment for this item - explain why
            forecast_qty = row.get('forecast_qty_pre_weather', 0) or 0
            case_pack_size = row.get('case_pack_size', 6) or 6
            current_cases = forecast_qty / case_pack_size
            
            if severity_score >= 4:
                if current_cases < 2:
                    row['weather_adjustment_reason'] = (
                        f"Severity {severity_score:.1f} ({severity_category}). "
                        f"No reduction: Only {current_cases:.1f} cases - need minimum 2 cases to reduce."
                    )
                else:
                    row['weather_adjustment_reason'] = (
                        f"Severity {severity_score:.1f} ({severity_category}). "
                        f"Target reduction met by other items in store."
                    )
        
        # Update weather status indicator
        row['weather_status_indicator'] = build_weather_status_indicator(row)
    
    # Convert back to list format
    result_rows = [(idx, row_lookup[idx]) for idx, _, _ in prioritized]
    
    return result_rows, items_adjusted, total_cases_reduced


# =============================================================================
# MAIN ADJUSTMENT FUNCTIONS
# =============================================================================

def apply_weather_adjustments(
    forecast_results: List[dict],
    weather_data: Dict[Tuple[str, str], dict],
    severity_threshold: float = DEFAULT_SEVERITY_THRESHOLD,
    max_store_reduction_pct: float = DEFAULT_MAX_STORE_REDUCTION_PCT,
    verbose: bool = True,
    **kwargs  # Accept but ignore legacy parameters like min_shrink_headroom
) -> List[dict]:
    """
    Apply weather-based adjustments to forecast results at the STORE level.
    
    This is a store-level adjustment approach:
    1. Groups forecasts by store-date
    2. For stores with significant weather (severity >= threshold), calculates STORE-level target reduction
    3. Prioritizes items for reduction: non-hero items first, then by forecast coverage
    4. Applies iterative case-by-case reduction across prioritized items
    5. GUARANTEES minimum 1 item × 1 case reduction when severity > 4
    6. Respects case pack sizes (rounds to whole cases)
    7. Caps total reduction at max_store_reduction_pct
    
    Args:
        forecast_results: List of forecast result dictionaries
        weather_data: Weather data keyed by (store_no, date)
        severity_threshold: Minimum severity to trigger adjustments (default 4.0)
        max_store_reduction_pct: Maximum store-level reduction percentage (default 40%)
        verbose: Print detailed adjustment information
        
    Returns:
        Updated forecast results with weather adjustments applied
    """
    if verbose:
        print("\n" + "=" * 60)
        print("WEATHER ADJUSTMENT MODULE (Store-Level)")
        print("=" * 60)
        print(f"Severity threshold: {severity_threshold}")
        print(f"Max store reduction: {max_store_reduction_pct:.1%}")
        print("Guarantee: Min 1 item × 1 case when severity > 4")
    
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
        'total_rows': len(forecast_results),
        'stores_evaluated': 0,
        'stores_adjusted': 0,
        'stores_with_guarantee': 0,
        'items_adjusted': 0,
        'total_cases_reduced': 0,
        'total_units_reduced': 0,
    }
    
    adjusted_results = [None] * len(forecast_results)
    
    for key, indexed_rows in store_date_groups.items():
        store_no, date_forecast = key
        stats['stores_evaluated'] += 1
        
        # Get weather data
        weather_info = weather_data.get(key, {})
        severity_score = weather_info.get('severity_score', 0) or 0
        severity_category = weather_info.get('severity_category', 'MINIMAL') or 'MINIMAL'
        sales_impact_factor = weather_info.get('sales_impact_factor', 1.0) or 1.0
        
        # Initialize all rows with weather data and pre-adjustment values
        for idx, row in indexed_rows:
            row['weather_severity_score'] = severity_score
            row['weather_severity_category'] = severity_category
            row['weather_sales_impact_factor'] = sales_impact_factor
            row['forecast_qty_pre_weather'] = row.get('forecast_quantity', 0)
            row['weather_adjustment_qty'] = 0
            row['weather_adjustment_reason'] = ''
            row['weather_adjusted'] = 0
            
            # Add weather status indicator
            row['weather_status_indicator'] = build_weather_status_indicator(row)
            
            # Add sales trend
            row['sales_trend_4wk'] = build_sales_trend_string(
                row.get('w4_sold'), row.get('w3_sold'),
                row.get('w2_sold'), row.get('w1_sold')
            )
        
        # Skip if below severity threshold
        if severity_score < severity_threshold:
            for idx, row in indexed_rows:
                row['weather_adjustment_reason'] = f"Severity {severity_score:.1f} below threshold {severity_threshold}"
                adjusted_results[idx] = row
            continue
        
        # Calculate store-level totals and target reduction
        total_store_forecast = sum(row.get('forecast_quantity', 0) or 0 for _, row in indexed_rows)
        
        if total_store_forecast == 0:
            for idx, row in indexed_rows:
                adjusted_results[idx] = row
            continue
        
        # Target reduction based on weather impact
        weather_reduction_pct = 1.0 - sales_impact_factor
        target_reduction_pct = min(weather_reduction_pct, max_store_reduction_pct)
        target_reduction_units = total_store_forecast * target_reduction_pct
        
        if verbose and target_reduction_pct > 0:
            print(f"\n  Store {store_no} | {date_forecast}")
            print(f"    Severity: {severity_score:.1f} ({severity_category})")
            print(f"    Total store forecast: {total_store_forecast} units")
            print(f"    Weather impact: {weather_reduction_pct:.1%}, Target reduction: {target_reduction_units:.0f} units")
        
        # Apply store-level reduction using prioritized iterative approach
        updated_rows, items_adjusted, cases_reduced = apply_store_level_reduction(
            indexed_rows,
            target_reduction_units,
            severity_score,
            severity_category,
            target_reduction_pct,
            verbose=verbose,
            store_no=store_no,
            date_forecast=date_forecast
        )
        
        # Calculate actual reduction for stats
        total_reduced = sum(row.get('weather_adjustment_qty', 0) or 0 for _, row in updated_rows)
        
        # Store results
        for idx, row in updated_rows:
            adjusted_results[idx] = row
        
        # Update stats
        if items_adjusted > 0:
            stats['stores_adjusted'] += 1
            stats['items_adjusted'] += items_adjusted
            stats['total_units_reduced'] += total_reduced
            stats['total_cases_reduced'] += cases_reduced
            
            if verbose:
                print(f"    Reduced: {total_reduced} units ({cases_reduced} cases) across {items_adjusted} items")
        elif severity_score > 4 and cases_reduced > 0:
            # Guarantee was applied
            stats['stores_with_guarantee'] += 1
            stats['stores_adjusted'] += 1
            stats['items_adjusted'] += items_adjusted
            stats['total_units_reduced'] += total_reduced
            stats['total_cases_reduced'] += cases_reduced
            
            if verbose:
                print(f"    [GUARANTEE] Reduced: {total_reduced} units ({cases_reduced} cases)")
    
    # Print summary
    if verbose:
        print("\nAdjustment Summary:")
        print(f"  Total rows: {stats['total_rows']}")
        print(f"  Stores evaluated: {stats['stores_evaluated']}")
        print(f"  Stores adjusted: {stats['stores_adjusted']}")
        if stats.get('stores_with_guarantee', 0) > 0:
            print(f"  Stores with guarantee applied: {stats['stores_with_guarantee']}")
        print(f"  Items adjusted: {stats['items_adjusted']}")
        print(f"  Total units reduced: {stats['total_units_reduced']}")
        print(f"  Total cases reduced: {stats['total_cases_reduced']}")
    
    return adjusted_results


def get_weather_adjustment_summary(forecast_results: List[dict]) -> dict:
    """
    Generate summary statistics for weather adjustments.
    
    Args:
        forecast_results: List of adjusted forecast results
        
    Returns:
        Dictionary with adjustment statistics
    """
    summary = {
        'total_rows': len(forecast_results),
        'adjusted_rows': 0,
        'total_pre_weather_qty': 0,
        'total_post_weather_qty': 0,
        'total_reduction': 0,
        'by_severity': {},
        'by_store': {},
        'by_date': {},
    }
    
    for row in forecast_results:
        pre_qty = row.get('forecast_qty_pre_weather', 0) or 0
        post_qty = row.get('forecast_quantity', 0) or 0
        reduction = row.get('weather_adjustment_qty', 0) or 0
        severity = row.get('weather_severity_category', 'UNKNOWN') or 'UNKNOWN'
        store = str(row.get('store_no', 'UNKNOWN'))
        date = str(row.get('date_forecast', 'UNKNOWN'))
        
        summary['total_pre_weather_qty'] += pre_qty
        summary['total_post_weather_qty'] += post_qty
        summary['total_reduction'] += reduction
        
        if reduction > 0:
            summary['adjusted_rows'] += 1
        
        # By severity
        if severity not in summary['by_severity']:
            summary['by_severity'][severity] = {'count': 0, 'reduction': 0}
        summary['by_severity'][severity]['count'] += 1
        summary['by_severity'][severity]['reduction'] += reduction
        
        # By store
        if store not in summary['by_store']:
            summary['by_store'][store] = {'count': 0, 'reduction': 0}
        summary['by_store'][store]['reduction'] += reduction
        
        # By date
        if date not in summary['by_date']:
            summary['by_date'][date] = {'count': 0, 'reduction': 0}
        summary['by_date'][date]['reduction'] += reduction
    
    return summary


def print_weather_adjustment_report(summary: dict):
    """
    Print a formatted weather adjustment report.
    
    Args:
        summary: Summary dictionary from get_weather_adjustment_summary
    """
    print("\n" + "=" * 60)
    print("WEATHER ADJUSTMENT REPORT")
    print("=" * 60)
    
    print("\nOverall Summary:")
    print(f"  Total rows: {summary['total_rows']}")
    print(f"  Adjusted rows: {summary['adjusted_rows']}")
    print(f"  Pre-weather quantity: {summary['total_pre_weather_qty']}")
    print(f"  Post-weather quantity: {summary['total_post_weather_qty']}")
    print(f"  Total reduction: {summary['total_reduction']}")
    
    if summary['total_pre_weather_qty'] > 0:
        pct_reduction = summary['total_reduction'] / summary['total_pre_weather_qty'] * 100
        print(f"  Reduction percentage: {pct_reduction:.2f}%")
    
    print("\nBy Severity Category:")
    for category, data in sorted(summary['by_severity'].items()):
        print(f"  {category}: {data['count']} rows, {data['reduction']} units reduced")
    
    print("\nTop 5 Dates by Reduction:")
    sorted_dates = sorted(summary['by_date'].items(), 
                         key=lambda x: x[1]['reduction'], reverse=True)[:5]
    for date, data in sorted_dates:
        print(f"  {date}: {data['reduction']} units reduced")
    
    print("\nTop 5 Stores by Reduction:")
    sorted_stores = sorted(summary['by_store'].items(), 
                          key=lambda x: x[1]['reduction'], reverse=True)[:5]
    for store, data in sorted_stores:
        print(f"  Store {store}: {data['reduction']} units reduced")
