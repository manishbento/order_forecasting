# Adjustment System Enhancement - Summary

## What We Built

A **dynamic, type-based adjustment framework** that allows you to add new forecast adjustments without modifying core logic. All adjustments are automatically tracked in the waterfall breakdown.

## Key Changes

### 1. New AdjustmentType Enum (`forecasting/adjustments.py`)

Defined 8 adjustment types:
- `PROMO` - Standard promotional uplifts
- `HOLIDAY_INCREASE` - Holiday-related demand increases
- `CANNIBALISM` - Demand reduction due to competing products
- `ADHOC_INCREASE` - One-time or temporary demand increases
- `ADHOC_DECREASE` - One-time or temporary demand decreases
- `STORE_SPECIFIC` - Store-level adjustments
- `ITEM_SPECIFIC` - Item-level adjustments
- `REGIONAL` - Region-level adjustments

### 2. Unified ADJUSTMENTS List (`forecasting/adjustments.py`)

Replaced separate lists (`PROMOTIONS`, `THANKSGIVING_ADJUSTMENTS`, `DECEMBER_ADJUSTMENTS`) with a single `ADJUSTMENTS` list where each entry includes:
- `type`: AdjustmentType enum value
- `name`: Descriptive name
- `regions`: List of affected regions (or None for all)
- `start_date`: Start date
- `end_date`: End date
- `multiplier`: Adjustment factor
- Optional: `stores`, `items` filters

### 3. Dynamic apply_adjustments() Function (`forecasting/adjustments.py`)

New core function that:
- ✅ Iterates through all adjustments in order
- ✅ Checks if adjustment applies (region, date, store, item filters)
- ✅ Applies multiplier to forecast_average
- ✅ Tracks adjustment in type-specific fields
- ✅ Stops after first matching adjustment per type

### 4. Enhanced Waterfall Schema (`data/aggregates.py`)

Added new fields to `waterfall_aggregate` table:
- `promo_adj_qty` / `promo_adj_count`
- `holiday_increase_adj_qty` / `holiday_increase_adj_count`
- `cannibalism_adj_qty` / `cannibalism_adj_count`
- `adhoc_increase_adj_qty` / `adhoc_increase_adj_count`
- `adhoc_decrease_adj_qty` / `adhoc_decrease_adj_count`
- `store_specific_adj_qty` / `store_specific_adj_count`
- `item_specific_adj_qty` / `item_specific_adj_count`
- `regional_adj_qty` / `regional_adj_count`

### 5. Updated Aggregation Query (`data/aggregates.py`)

Modified `populate_waterfall_aggregate()` to SUM all new adjustment fields from `forecast_results` table.

### 6. Comprehensive Documentation

Created `docs/ADJUSTMENT_TYPES_GUIDE.md` with:
- Detailed explanation of each adjustment type
- Step-by-step guide for adding new adjustments
- Multiple examples (promotions, store-specific, item-specific, etc.)
- Filter logic explanation
- Best practices and troubleshooting

## Benefits

### ✅ Dynamic Configuration
Add new adjustments by simply appending to `ADJUSTMENTS` list - no code changes needed.

### ✅ Type Safety
Clear categorization ensures proper tracking and waterfall visibility.

### ✅ Flexible Filtering
Filter by region, store, item, or any combination.

### ✅ Complete Transparency
Every adjustment is tracked and visible in waterfall breakdown.

### ✅ Scalable
Easy to add new adjustment types in the future if needed.

### ✅ Backward Compatible
Legacy fields (`promo_uplift_qty`, `holiday_adj_qty`, `cannibalism_adj_qty`) maintained for compatibility.

## How to Use

### Adding a New Adjustment

Open `forecasting/adjustments.py` and add to the `ADJUSTMENTS` list:

```python
{
    'type': AdjustmentType.PROMO,
    'name': 'Summer_Sale_2026',
    'regions': ['BA', 'LA'],
    'start_date': datetime(2026, 7, 1),
    'end_date': datetime(2026, 7, 7),
    'multiplier': 1.15
}
```

That's it! The system will automatically:
1. Apply the adjustment during forecasting
2. Track it in forecast_results
3. Aggregate it in waterfall_aggregate
4. Include it in all exports

### Example Adjustments Already Configured

1. **Promotions**: LA August 2025, BA August/September 2025
2. **Holiday Increases**: Thanksgiving in LA/NE/TE/SE regions
3. **Cannibalism**: BA/SD Thanksgiving, BA/LA/SD December platter competition
4. **Adhoc Increases**: High-volume store groups, new item launches
5. **Regional**: NE regional boost
6. **Item-Specific**: Salmon combo TE boost, various item uplifts
7. **Store-Specific**: Store 423 temporary decrease

## Next Steps

### For Current Forecast Runs

No action needed - system works with existing adjustments converted to new format.

### For Future Adjustments

1. Open `forecasting/adjustments.py`
2. Choose appropriate `AdjustmentType`
3. Add entry to `ADJUSTMENTS` list
4. Run forecast - adjustment will be applied and tracked

### For Waterfall Exports

Next time exports are regenerated, they will include all adjustment type breakdowns in the waterfall.

## Files Modified

1. `forecasting/adjustments.py` - Core adjustment logic
2. `data/aggregates.py` - Waterfall schema and aggregation
3. `docs/ADJUSTMENT_TYPES_GUIDE.md` - Comprehensive guide (NEW)
4. `docs/ADJUSTMENT_SYSTEM_SUMMARY.md` - This summary (NEW)

## Testing Recommendations

1. ✅ Verify existing adjustments still work
2. ✅ Add a test adjustment and confirm it appears in forecast_results
3. ✅ Run aggregation and verify new fields populate
4. ✅ Check waterfall exports include new adjustment types
5. ✅ Validate waterfall math balances with new components

## Questions?

Refer to `docs/ADJUSTMENT_TYPES_GUIDE.md` for detailed examples and troubleshooting.
