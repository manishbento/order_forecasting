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
from enum import Enum


# =============================================================================
# ADJUSTMENT TYPE ENUM
# =============================================================================

class AdjustmentType(Enum):
    """
    Defines all possible adjustment types for forecast modifications.
    Each type will be tracked separately in the waterfall breakdown.
    """
    PROMO = "promo"                          # Standard promotional uplifts
    HOLIDAY_INCREASE = "holiday_increase"    # Holiday-related demand increases
    CANNIBALISM = "cannibalism"              # Demand reduction due to competing products
    ADHOC_INCREASE = "adhoc_increase"        # One-time or temporary demand increases
    ADHOC_DECREASE = "adhoc_decrease"        # One-time or temporary demand decreases
    STORE_SPECIFIC = "store_specific"        # Store-level adjustments
    ITEM_SPECIFIC = "item_specific"          # Item-level adjustments
    REGIONAL = "regional"                    # Region-level adjustments


# =============================================================================
# ADJUSTMENT CONFIGURATIONS
# =============================================================================
# Each adjustment must specify:
# - type: AdjustmentType enum value
# - name: Descriptive name for the adjustment
# - regions: List of affected regions (or None for all)
# - start_date: Start date for the adjustment
# - end_date: End date for the adjustment
# - multiplier: Adjustment multiplier (>1.0 for increases, <1.0 for decreases)
# - Optional: stores, items, or other filter criteria

ADJUSTMENTS = [
    # ========== PROMOTIONAL ADJUSTMENTS ==========
    {
        'type': AdjustmentType.PROMO,
        'name': 'LA_August_2025',
        'regions': ['LA'],
        'start_date': datetime(2025, 8, 19),
        'end_date': datetime(2025, 8, 25),
        'multiplier': 1.05
    },
    {
        'type': AdjustmentType.PROMO,
        'name': 'BA_August_2025',
        'regions': ['BA'],
        'start_date': datetime(2025, 8, 25),
        'end_date': datetime(2025, 8, 31),
        'multiplier': 1.10
    },
    {
        'type': AdjustmentType.PROMO,
        'name': 'BA_National_Sept_2025',
        'regions': ['BA'],
        'start_date': datetime(2025, 9, 22),
        'end_date': datetime(2025, 9, 28),
        'multiplier': 1.05
    },
    
    # ========== HOLIDAY ADJUSTMENTS ==========
    {
        'type': AdjustmentType.HOLIDAY_INCREASE,
        'name': 'Other_Regions_Thanksgiving',
        'regions': ['LA', 'NE', 'TE'],
        'start_date': datetime(2025, 11, 24),
        'end_date': datetime(2025, 11, 26),
        'multiplier': 1.10
    },
    {
        'type': AdjustmentType.HOLIDAY_INCREASE,
        'name': 'SE_Thanksgiving',
        'regions': ['SE'],
        'start_date': datetime(2025, 11, 24),
        'end_date': datetime(2025, 11, 24),
        'multiplier': 1.05
    },
    
    # ========== CANNIBALISM ADJUSTMENTS ==========
    {
        'type': AdjustmentType.CANNIBALISM,
        'name': 'BA_SD_Thanksgiving_Cannibalism',
        'regions': ['BA', 'SD'],
        'start_date': datetime(2025, 11, 21),
        'end_date': datetime(2025, 11, 23),
        'multiplier': 0.88
    },
    {
        'type': AdjustmentType.CANNIBALISM,
        'name': 'BA_Platter_Cannibalism',
        'regions': ['BA'],
        'start_date': datetime(2025, 12, 4),
        'end_date': datetime(2025, 12, 7),
        'multiplier': 0.88
    },
    {
        'type': AdjustmentType.CANNIBALISM,
        'name': 'LA_SD_Platter_Cannibalism',
        'regions': ['LA', 'SD'],
        'start_date': datetime(2025, 12, 18),
        'end_date': datetime(2025, 12, 21),
        'multiplier': 0.87
    },
    
    # ========== ADHOC INCREASE ADJUSTMENTS ==========
    # Example: Store-specific temporary increase for high-volume stores
    {
        'type': AdjustmentType.ADHOC_INCREASE,
        'name': 'High_Volume_Stores_Group1',
        'regions': None,  # All regions
        'stores': [490, 674, 691, 738, 1375, 1653],
        'start_date': datetime(2025, 10, 30),
        'end_date': datetime(2025, 11, 2),
        'multiplier': 1.20
    },
    {
        'type': AdjustmentType.ADHOC_INCREASE,
        'name': 'High_Volume_Stores_Group2',
        'regions': None,  # All regions
        'stores': [465, 1058, 427, 481, 644, 436, 736, 1028, 1620, 431, 407, 1079],
        'start_date': datetime(2025, 10, 10),
        'end_date': datetime(2025, 10, 16),
        'multiplier': 1.10
    },
    
    # Example: Item-specific increase for new product launch
    {
        'type': AdjustmentType.ITEM_SPECIFIC,
        'name': 'New_Item_Launch_1984587',
        'regions': None,  # All regions
        'items': [1984587],
        'start_date': datetime(2025, 9, 1),
        'end_date': datetime(2025, 9, 21),
        'multiplier': 1.20
    },
    {
        'type': AdjustmentType.ITEM_SPECIFIC,
        'name': 'New_Item_Launch_1984587_Phase2',
        'regions': None,  # All regions
        'items': [1984587],
        'start_date': datetime(2025, 9, 22),
        'end_date': datetime(2025, 9, 28),
        'multiplier': 1.07
    },
    {
        'type': AdjustmentType.ITEM_SPECIFIC,
        'name': 'Item_1984587_BA_Boost',
        'regions': ['BA'],
        'items': [1984587],
        'start_date': datetime(2025, 11, 17),
        'end_date': datetime(2025, 11, 23),
        'multiplier': 1.10
    },
    
    # Example: Regional boost
    {
        'type': AdjustmentType.REGIONAL,
        'name': 'NE_Regional_Boost',
        'regions': ['NE'],
        'start_date': datetime(2025, 10, 20),
        'end_date': datetime(2025, 11, 2),
        'multiplier': 1.15
    },
    
    # ========== ADHOC DECREASE ADJUSTMENTS ==========
    # Example: Store-specific temporary decrease
    {
        'type': AdjustmentType.ADHOC_DECREASE,
        'name': 'Store_423_Decrease',
        'regions': None,  # All regions
        'stores': [423],
        'start_date': datetime(2025, 11, 3),
        'end_date': datetime(2025, 11, 9),
        'multiplier': 0.90
    },
    
    # Example: Item-specific decrease in specific region
    {
        'type': AdjustmentType.ITEM_SPECIFIC,
        'name': 'Salmon_Combo_TE_Boost',
        'regions': ['TE'],
        'items': [1713314],
        'start_date': datetime(2025, 10, 20),
        'end_date': datetime(2025, 11, 2),
        'multiplier': 1.30
    },
    {
        'type': AdjustmentType.ITEM_SPECIFIC,
        'name': 'Item_1896526_BA_Boost',
        'regions': ['BA'],
        'items': [1896526],
        'start_date': datetime(2025, 11, 17),
        'end_date': datetime(2025, 11, 23),
        'multiplier': 1.30
    },
]


# =============================================================================
# INSTRUCTIONS FOR ADDING NEW ADJUSTMENTS
# =============================================================================
"""
To add a new adjustment, simply append to the ADJUSTMENTS list above with:

1. 'type': Choose from AdjustmentType enum:
   - PROMO: Standard promotional uplifts
   - HOLIDAY_INCREASE: Holiday-related demand increases
   - CANNIBALISM: Demand reduction due to competing products
   - ADHOC_INCREASE: One-time or temporary demand increases
   - ADHOC_DECREASE: One-time or temporary demand decreases
   - STORE_SPECIFIC: Store-level adjustments
   - ITEM_SPECIFIC: Item-level adjustments
   - REGIONAL: Region-level adjustments

2. 'name': Descriptive name for tracking (will appear in exports)

3. 'regions': List of region codes ['BA', 'LA', etc.] or None for all regions

4. 'start_date': datetime(YYYY, MM, DD) - when adjustment begins

5. 'end_date': datetime(YYYY, MM, DD) - when adjustment ends

6. 'multiplier': Float value (>1.0 for increases, <1.0 for decreases)

7. Optional filters:
   - 'stores': List of store numbers [123, 456, etc.]
   - 'items': List of item numbers [789, 101112, etc.]

EXAMPLES:

# Simple region-wide promotion
{
    'type': AdjustmentType.PROMO,
    'name': 'Summer_Sale_2026',
    'regions': ['BA', 'LA'],
    'start_date': datetime(2026, 7, 1),
    'end_date': datetime(2026, 7, 7),
    'multiplier': 1.15
}

# Store-specific adjustment
{
    'type': AdjustmentType.STORE_SPECIFIC,
    'name': 'Grand_Opening_Store_999',
    'regions': None,
    'stores': [999],
    'start_date': datetime(2026, 8, 1),
    'end_date': datetime(2026, 8, 7),
    'multiplier': 1.50
}

# Item-specific cannibalism
{
    'type': AdjustmentType.CANNIBALISM,
    'name': 'Item_123_Competitor_Launch',
    'regions': ['SD', 'SE'],
    'items': [123456],
    'start_date': datetime(2026, 9, 1),
    'end_date': datetime(2026, 9, 30),
    'multiplier': 0.85
}

NO CODE CHANGES NEEDED - Just add your adjustment to the ADJUSTMENTS list!
The waterfall tracking will automatically capture your adjustment by type.
"""


# =============================================================================
# ADJUSTMENT FUNCTIONS
# =============================================================================

def initialize_adjustment_tracking_fields(row: dict) -> dict:
    """
    Initialize all adjustment tracking fields to default values.
    
    This ensures all possible adjustment fields exist in the row,
    making downstream aggregation easier.
    
    Args:
        row: Item-store data dictionary
        
    Returns:
        Row with initialized tracking fields
    """
    # Track adjustments by type
    for adj_type in AdjustmentType:
        prefix = adj_type.value
        row[f'{prefix}_adj_applied'] = 0
        row[f'{prefix}_adj_qty'] = 0.0
        row[f'{prefix}_adj_name'] = ''
        row[f'{prefix}_adj_multiplier'] = 1.0
    
    # Legacy fields for backward compatibility
    row['promo_uplift_applied'] = 0
    row['promo_uplift_qty'] = 0.0
    row['promo_uplift_name'] = ''
    row['promo_uplift_multiplier'] = 1.0
    row['holiday_adj_applied'] = 0
    row['holiday_adj_qty'] = 0.0
    row['holiday_adj_name'] = ''
    row['holiday_adj_multiplier'] = 1.0
    row['cannibalism_adj_applied'] = 0
    row['cannibalism_adj_qty'] = 0.0
    
    return row


def apply_adjustments(row: dict, current_date: datetime, 
                     adjustments_list: List[dict] = None) -> dict:
    """
    Apply all configured adjustments dynamically based on their type.
    
    This is the core adjustment engine that:
    1. Iterates through all adjustments in ADJUSTMENTS list
    2. Checks if the adjustment applies (region, date, store, item filters)
    3. Applies the multiplier to forecast_average
    4. Tracks the adjustment in type-specific fields
    5. Stops after first matching adjustment per type (priority order)
    
    Args:
        row: Item-store data dictionary with forecast_average
        current_date: Current forecast date
        adjustments_list: Optional custom adjustment list (defaults to ADJUSTMENTS)
        
    Returns:
        Updated row with adjustments applied and tracked
    """
    if adjustments_list is None:
        adjustments_list = ADJUSTMENTS
    
    # Initialize tracking fields
    row = initialize_adjustment_tracking_fields(row)
    
    # Track which adjustment types have been applied (one per type)
    applied_types = set()
    
    # Get row attributes for filtering
    region = row.get('region_code')
    store_no = int(row.get('store_no', 0))
    item_no = int(row.get('item_no', 0))
    
    # Track pre-adjustment value
    pre_adjustment = row.get('forecast_average', 0) or 0
    
    # Process each adjustment in order
    for adj in adjustments_list:
        adj_type = adj['type']
        
        # Skip if we've already applied an adjustment of this type
        if adj_type in applied_types:
            continue
        
        # Check if adjustment applies to this row
        if not _adjustment_matches(adj, region, store_no, item_no, current_date):
            continue
        
        # Apply the adjustment
        multiplier = adj['multiplier']
        row['forecast_average'] *= multiplier
        
        # Track the adjustment by type
        prefix = adj_type.value
        row[f'{prefix}_adj_applied'] = 1
        row[f'{prefix}_adj_name'] = adj['name']
        row[f'{prefix}_adj_multiplier'] = multiplier
        row[f'{prefix}_adj_qty'] = row['forecast_average'] - pre_adjustment
        
        # Update pre_adjustment for next adjustment
        pre_adjustment = row['forecast_average']
        
        # Mark this type as applied
        applied_types.add(adj_type)
        
        # Also populate legacy fields for backward compatibility
        if adj_type == AdjustmentType.PROMO:
            row['promo_uplift_applied'] = 1
            row['promo_uplift_name'] = adj['name']
            row['promo_uplift_multiplier'] = multiplier
            row['promo_uplift_qty'] = row[f'{prefix}_adj_qty']
        elif adj_type in (AdjustmentType.HOLIDAY_INCREASE, AdjustmentType.CANNIBALISM):
            row['holiday_adj_applied'] = 1
            row['holiday_adj_name'] = adj['name']
            row['holiday_adj_multiplier'] = multiplier
            row['holiday_adj_qty'] = row[f'{prefix}_adj_qty']
            if adj_type == AdjustmentType.CANNIBALISM:
                row['cannibalism_adj_applied'] = 1
                row['cannibalism_adj_qty'] = row[f'{prefix}_adj_qty']
    
    return row


def _adjustment_matches(adj: dict, region: str, store_no: int, 
                       item_no: int, current_date: datetime) -> bool:
    """
    Check if an adjustment applies to a specific row.
    
    Args:
        adj: Adjustment configuration dictionary
        region: Region code
        store_no: Store number
        item_no: Item number
        current_date: Current forecast date
        
    Returns:
        True if adjustment should be applied to this row
    """
    # Check date range
    if not (adj['start_date'] <= current_date <= adj['end_date']):
        return False
    
    # Check region filter (None means all regions)
    if 'regions' in adj and adj['regions'] is not None:
        if region not in adj['regions']:
            return False
    
    # Check store filter (if specified)
    if 'stores' in adj and adj['stores'] is not None:
        if store_no not in adj['stores']:
            return False
    
    # Check item filter (if specified)
    if 'items' in adj and adj['items'] is not None:
        if item_no not in adj['items']:
            return False
    
    return True


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
    2. All configured adjustments (promotions, holidays, cannibalism, adhoc, etc.)
    3. Store-specific adjustments
    4. Item-specific adjustments
    5. Weather-based adjustments
    
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
    
    # Apply all configured adjustments (promotions, holidays, cannibalism, etc.)
    row = apply_adjustments(row, current_date)
    
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
STORE_SHRINK_THRESHOLD = 0.15  # 15% - max acceptable forecast shrink at store level (reduced from 20%)
STORE_MIN_COVERAGE = 0.00      # 0% - minimum coverage required (trigger for increase)

# Reasonability thresholds - prevent over-forecasting beyond historical patterns
STORE_MAX_VS_HISTORICAL_THRESHOLD = 0.10  # 10% - max allowed above highest week in last 4 weeks
ITEM_HISTORICAL_CAP_ENABLED = True  # Prioritize items forecasted above their historical max


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


def calculate_store_historical_metrics(indexed_rows: List[Tuple[int, dict]]) -> dict:
    """
    Calculate store-level historical metrics for reasonability checks.
    
    Aggregates the last 4 weeks of sales at the store level to determine
    historical max/average/min for reasonability testing.
    
    Args:
        indexed_rows: List of (index, row) tuples for a store-date
        
    Returns:
        Dictionary with:
        - store_w1_sold: Total W1 sales across all items
        - store_w2_sold: Total W2 sales across all items
        - store_w3_sold: Total W3 sales across all items
        - store_w4_sold: Total W4 sales across all items
        - store_max_4w: Max of last 4 weeks total sales
        - store_avg_4w: Average of last 4 weeks total sales
        - store_min_4w: Min of last 4 weeks total sales
    """
    store_w1_sold = 0
    store_w2_sold = 0
    store_w3_sold = 0
    store_w4_sold = 0
    
    for _, row in indexed_rows:
        store_w1_sold += (row.get('w1_sold', 0) or 0)
        store_w2_sold += (row.get('w2_sold', 0) or 0)
        store_w3_sold += (row.get('w3_sold', 0) or 0)
        store_w4_sold += (row.get('w4_sold', 0) or 0)
    
    weekly_totals = [store_w1_sold, store_w2_sold, store_w3_sold, store_w4_sold]
    # Filter out zero weeks (could indicate data gaps)
    non_zero_weeks = [w for w in weekly_totals if w > 0]
    
    return {
        'store_w1_sold': store_w1_sold,
        'store_w2_sold': store_w2_sold,
        'store_w3_sold': store_w3_sold,
        'store_w4_sold': store_w4_sold,
        'store_max_4w': max(weekly_totals) if weekly_totals else 0,
        'store_avg_4w': sum(non_zero_weeks) / len(non_zero_weeks) if non_zero_weeks else 0,
        'store_min_4w': min(non_zero_weeks) if non_zero_weeks else 0,
    }


def calculate_item_historical_cap(row: dict) -> dict:
    """
    Calculate item-level historical cap metrics.
    
    Identifies if an item is being forecasted above its historical maximum,
    which indicates potential over-forecasting that should be prioritized for reduction.
    
    Args:
        row: Item-store data dictionary
        
    Returns:
        Dictionary with:
        - item_max_4w: Max sales in last 4 weeks
        - item_avg_4w: Average non-zero sales in last 4 weeks
        - forecast_vs_max_ratio: forecast_qty / max_4w (values > 1 = over historical max)
        - exceeds_historical_max: Boolean flag
    """
    w1_sold = row.get('w1_sold', 0) or 0
    w2_sold = row.get('w2_sold', 0) or 0
    w3_sold = row.get('w3_sold', 0) or 0
    w4_sold = row.get('w4_sold', 0) or 0
    forecast_qty = row.get('forecast_quantity', 0) or 0
    
    weekly_sales = [w1_sold, w2_sold, w3_sold, w4_sold]
    non_zero_weeks = [w for w in weekly_sales if w > 0]
    
    item_max_4w = max(weekly_sales) if weekly_sales else 0
    item_avg_4w = sum(non_zero_weeks) / len(non_zero_weeks) if non_zero_weeks else 0
    
    # Calculate how much forecast exceeds historical max
    if item_max_4w > 0:
        forecast_vs_max_ratio = forecast_qty / item_max_4w
    else:
        # No historical sales - can't determine if exceeding
        forecast_vs_max_ratio = 1.0
    
    return {
        'item_max_4w': item_max_4w,
        'item_avg_4w': item_avg_4w,
        'forecast_vs_max_ratio': forecast_vs_max_ratio,
        'exceeds_historical_max': forecast_vs_max_ratio > 1.0
    }


def apply_store_level_shrink_pass(
    forecast_results: List[dict],
    shrink_threshold: float = STORE_SHRINK_THRESHOLD,
    historical_threshold: float = STORE_MAX_VS_HISTORICAL_THRESHOLD,
    use_historical_cap: bool = ITEM_HISTORICAL_CAP_ENABLED,
    verbose: bool = True
) -> List[dict]:
    """
    Apply store-level shrink pass to reduce forecast with enhanced reasonability checks.
    
    This enhanced function:
    1. Groups forecasts by store-date
    2. Calculates store-level forecast shrink % = (forecast_qty - w1_sold) / forecast_qty
    3. Calculates store-level historical metrics (max/avg of last 4 weeks)
    4. Applies TWO controls:
       a) Shrink threshold: If shrink % > threshold (default 15%), reduce items
       b) Historical reasonability: If forecast > max_4w * (1 + historical_threshold), reduce
    5. PRIORITIZES items exceeding their historical max first (never sold this much)
    6. Then prioritizes by coverage (most over-forecasted)
    7. Maintains minimum 1 case per item
    
    Args:
        forecast_results: List of forecast result dictionaries
        shrink_threshold: Maximum acceptable store-level shrink percentage (default 0.15)
        historical_threshold: Max % allowed above store's historical max (default 0.10)
        use_historical_cap: Whether to prioritize items exceeding historical max
        verbose: Print detailed adjustment information
        
    Returns:
        Updated forecast results with store-level adjustments applied
    """
    if verbose:
        print("\n" + "=" * 60)
        print("STORE-LEVEL SHRINK PASS (ENHANCED)")
        print("=" * 60)
        print(f"Shrink threshold: {shrink_threshold:.0%}")
        print(f"Historical max threshold: +{historical_threshold:.0%}")
        print(f"Item historical cap enabled: {use_historical_cap}")
    
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
        'stores_adjusted_shrink': 0,
        'stores_adjusted_historical': 0,
        'items_adjusted': 0,
        'items_exceeding_historical': 0,
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
            row['store_level_decline_qty'] = 0.0  # Reduction due to shrink control (stored as negative)
            row['store_level_growth_qty'] = 0.0   # Increase due to coverage (stored as positive)
            row['store_level_adjustment_reason'] = ''
            row['store_level_adjusted'] = 0
            
            # Calculate item historical metrics
            item_hist = calculate_item_historical_cap(row)
            row['item_max_4w'] = item_hist['item_max_4w']
            row['item_avg_4w'] = item_hist['item_avg_4w']
            row['forecast_vs_max_ratio'] = item_hist['forecast_vs_max_ratio']
            row['exceeds_historical_max'] = item_hist['exceeds_historical_max']
        
        # Calculate initial store-level metrics
        total_forecast, total_sold, store_shrink_pct, min_coverage = calculate_store_level_metrics(indexed_rows)
        
        # Calculate store historical metrics
        store_hist = calculate_store_historical_metrics(indexed_rows)
        store_max_4w = store_hist['store_max_4w']
        store_avg_4w = store_hist['store_avg_4w']
        
        # Calculate historical reasonability: is forecast > max_4w * (1 + threshold)?
        historical_cap = store_max_4w * (1 + historical_threshold) if store_max_4w > 0 else float('inf')
        exceeds_historical = total_forecast > historical_cap
        historical_overage_pct = (total_forecast / store_max_4w - 1) if store_max_4w > 0 else 0
        
        # Store the metrics in rows
        for idx, row in indexed_rows:
            row['store_level_shrink_pct'] = store_shrink_pct
            row['store_level_coverage_pct'] = min_coverage
            row['store_max_4w'] = store_max_4w
            row['store_avg_4w'] = store_avg_4w
            row['store_historical_cap'] = historical_cap
            row['store_exceeds_historical'] = exceeds_historical
        
        # DUAL CHECK: Both shrink threshold AND historical reasonability
        needs_shrink_adjustment = store_shrink_pct > shrink_threshold
        needs_historical_adjustment = exceeds_historical
        
        # Skip if both checks pass
        if not needs_shrink_adjustment and not needs_historical_adjustment:
            for idx, row in indexed_rows:
                row['store_level_adjustment_reason'] = (
                    f"Within thresholds: Shrink {store_shrink_pct:.1%} <= {shrink_threshold:.0%}, "
                    f"Forecast {total_forecast:.0f} <= Historical cap {historical_cap:.0f}"
                )
                adjusted_results[idx] = row
            continue
        
        # Count items exceeding their historical max
        items_over_historical = sum(1 for _, r in indexed_rows if r.get('exceeds_historical_max', False))
        stats['items_exceeding_historical'] += items_over_historical
        
        if verbose:
            print(f"\n  Store {store_no} | {date_forecast}")
            print(f"    Initial shrink: {store_shrink_pct:.1%} (threshold: {shrink_threshold:.0%})")
            print(f"    Store forecast: {total_forecast:.0f}, W1 Sold: {total_sold:.0f}")
            print(f"    Historical - Max4W: {store_max_4w:.0f}, Avg4W: {store_avg_4w:.0f}")
            print(f"    Historical cap: {historical_cap:.0f} (+{historical_threshold:.0%})")
            if exceeds_historical:
                print(f"    ⚠️  EXCEEDS HISTORICAL by {historical_overage_pct:.1%}")
            if items_over_historical > 0:
                print(f"    ⚠️  {items_over_historical} items exceed their historical max")
        
        # Iteratively reduce items until BOTH checks pass
        iteration = 0
        max_iterations = 200  # Increased for larger stores
        total_reduced = 0
        
        while (store_shrink_pct > shrink_threshold or total_forecast > historical_cap) and iteration < max_iterations:
            iteration += 1
            
            # Find ALL items that can be reduced
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
                    
                    # Item-level historical ratio
                    item_max = row.get('item_max_4w', 0) or forecast_avg
                    forecast_vs_hist = forecast_qty / item_max if item_max > 0 else 1.0
                    exceeds_hist = row.get('exceeds_historical_max', False)
                    
                    # Item-level shrink for secondary sorting
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
                        'item_no': item_no,
                        'exceeds_historical': exceeds_hist,
                        'forecast_vs_hist': forecast_vs_hist
                    })
            
            # ENHANCED SORTING: 
            # 1. Items exceeding historical max FIRST (never sold this much before)
            # 2. Then by forecast_vs_hist ratio (how much over historical)
            # 3. Then by coverage (how much over forecast average)
            # 4. Finally by item shrink
            if use_historical_cap:
                reducible_items.sort(
                    key=lambda x: (
                        x['exceeds_historical'],  # True items first (True > False)
                        x['forecast_vs_hist'],    # Higher ratio = more over historical
                        x['coverage'],            # Higher coverage = more over-forecasted
                        x['shrink']               # Higher shrink
                    ),
                    reverse=True
                )
            else:
                # Original logic: just coverage and shrink
                reducible_items.sort(key=lambda x: (x['coverage'], x['shrink']), reverse=True)
            
            # If no items can be reduced, stop
            if not reducible_items:
                break
            
            # Reduce 1 case from the highest priority item
            top_item = reducible_items[0]
            case_size = top_item['case_pack_size']
            
            # Apply reduction
            top_item['row']['forecast_quantity'] -= case_size
            top_item['row']['store_level_adjustment_qty'] += case_size
            top_item['row']['store_level_decline_qty'] -= case_size  # Negative = decline/reduction
            top_item['row']['store_level_adjusted'] = 1
            
            total_reduced += case_size
            
            # Recalculate store-level metrics
            total_forecast, _, store_shrink_pct, _ = calculate_store_level_metrics(indexed_rows)
            
            # Build reason with context
            reason_parts = []
            if top_item['exceeds_historical']:
                reason_parts.append(f"Historical cap: {top_item['forecast_vs_hist']:.1f}x max")
            reason_parts.append(f"Coverage: {top_item['coverage']:.1f}x")
            reason_parts.append(f"Store shrink: {store_shrink_pct:.1%}")
            
            top_item['row']['store_level_adjustment_reason'] = (
                f"Store pass: reduced {top_item['row']['store_level_adjustment_qty']} units "
                f"({top_item['row']['store_level_adjustment_qty'] // case_size} cases). "
                + ", ".join(reason_parts)
            )
        
        # Update final metrics for all rows
        for idx, row in indexed_rows:
            row['store_level_shrink_pct'] = store_shrink_pct
            row['store_forecast_total'] = total_forecast
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
            if needs_shrink_adjustment:
                stats['stores_adjusted_shrink'] += 1
            if needs_historical_adjustment:
                stats['stores_adjusted_historical'] += 1
            if verbose:
                print(f"    Final shrink: {store_shrink_pct:.1%}")
                print(f"    Final forecast: {total_forecast:.0f} (cap: {historical_cap:.0f})")
                print(f"    Reduced: {total_reduced} units in {iteration} iterations")
                if store_shrink_pct > shrink_threshold:
                    print("    NOTE: Couldn't reach shrink target (floor constraints)")
                if total_forecast > historical_cap:
                    print("    NOTE: Couldn't reach historical cap (floor constraints)")
    
    # Print summary
    if verbose:
        print("\nStore-Level Shrink Pass Summary:")
        print(f"  Stores evaluated: {stats['stores_evaluated']}")
        print(f"  Stores adjusted (total): {stats['stores_adjusted']}")
        print(f"    - Due to shrink threshold: {stats['stores_adjusted_shrink']}")
        print(f"    - Due to historical cap: {stats['stores_adjusted_historical']}")
        print(f"  Items exceeding historical max: {stats['items_exceeding_historical']}")
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
        
        # Track adjustment - negative value indicates increase for legacy field
        current_adj = top_item['row'].get('store_level_adjustment_qty', 0) or 0
        top_item['row']['store_level_adjustment_qty'] = current_adj - case_size  # Negative = increase
        top_item['row']['store_level_growth_qty'] = (top_item['row'].get('store_level_growth_qty', 0) or 0) + case_size  # Positive = growth
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

