# Adjustment Types Guide

## Overview

The forecasting system now uses a dynamic, type-based adjustment framework that allows you to add new adjustments without modifying core logic. All adjustments are automatically tracked in the waterfall breakdown.

## Adjustment Types

### Available Types

1. **PROMO** - Standard promotional uplifts
   - Regular sales events
   - Marketing campaigns
   - Temporary price reductions
   
2. **HOLIDAY_INCREASE** - Holiday-related demand increases
   - Thanksgiving surges
   - Christmas increases
   - Other holiday uplifts
   
3. **CANNIBALISM** - Demand reduction due to competing products
   - Product substitution effects
   - Market share losses
   - Internal competition
   
4. **ADHOC_INCREASE** - One-time or temporary demand increases
   - Store grand openings
   - Special events
   - Temporary capacity changes
   
5. **ADHOC_DECREASE** - One-time or temporary demand decreases
   - Store closures
   - Temporary capacity reductions
   - One-off events
   
6. **STORE_SPECIFIC** - Store-level adjustments
   - High-volume store boosts
   - Underperforming store reductions
   - Store-specific circumstances
   
7. **ITEM_SPECIFIC** - Item-level adjustments
   - New product launches
   - Product phase-outs
   - Item-specific promotions
   
8. **REGIONAL** - Region-level adjustments
   - Regional market dynamics
   - Geographic-specific events
   - Regional capacity changes

## How to Add New Adjustments

### Step 1: Choose Your Adjustment Type

Select the most appropriate `AdjustmentType` from the list above.

### Step 2: Add to ADJUSTMENTS List

Open `forecasting/adjustments.py` and add your adjustment to the `ADJUSTMENTS` list:

```python
{
    'type': AdjustmentType.PROMO,              # Required: Type from enum
    'name': 'Summer_Sale_2026',                # Required: Descriptive name
    'regions': ['BA', 'LA'],                   # Required: List or None for all
    'start_date': datetime(2026, 7, 1),        # Required: Start date
    'end_date': datetime(2026, 7, 7),          # Required: End date
    'multiplier': 1.15,                        # Required: Adjustment factor
    'stores': None,                            # Optional: Filter by stores
    'items': None,                             # Optional: Filter by items
}
```

### Step 3: That's It!

No code changes needed. The system will:
- ✅ Apply the adjustment during forecasting
- ✅ Track it in the forecast_results table
- ✅ Aggregate it in waterfall_aggregate table
- ✅ Include it in all exports (JSON, Excel, AI analysis)
- ✅ Display it in waterfall breakdowns

## Example Adjustments

### Region-Wide Promotion

```python
{
    'type': AdjustmentType.PROMO,
    'name': 'BA_Summer_Promotion_2026',
    'regions': ['BA'],
    'start_date': datetime(2026, 7, 1),
    'end_date': datetime(2026, 7, 7),
    'multiplier': 1.15  # 15% increase
}
```

### Store-Specific Adjustment

```python
{
    'type': AdjustmentType.STORE_SPECIFIC,
    'name': 'Grand_Opening_Store_999',
    'regions': None,  # All regions
    'stores': [999],  # Specific store only
    'start_date': datetime(2026, 8, 1),
    'end_date': datetime(2026, 8, 7),
    'multiplier': 1.50  # 50% increase for opening week
}
```

### Item-Specific Cannibalism

```python
{
    'type': AdjustmentType.CANNIBALISM,
    'name': 'Item_123_Competitor_Launch',
    'regions': ['SD', 'SE'],
    'items': [123456],  # Specific item affected
    'start_date': datetime(2026, 9, 1),
    'end_date': datetime(2026, 9, 30),
    'multiplier': 0.85  # 15% decrease due to competition
}
```

### Multi-Store Adhoc Increase

```python
{
    'type': AdjustmentType.ADHOC_INCREASE,
    'name': 'High_Volume_Event_Weekend',
    'regions': None,
    'stores': [101, 102, 103, 104, 105],  # Multiple stores
    'start_date': datetime(2026, 10, 15),
    'end_date': datetime(2026, 10, 17),
    'multiplier': 1.30  # 30% weekend boost
}
```

### Regional Holiday Adjustment

```python
{
    'type': AdjustmentType.HOLIDAY_INCREASE,
    'name': 'Christmas_Week_NE',
    'regions': ['NE'],
    'start_date': datetime(2026, 12, 22),
    'end_date': datetime(2026, 12, 24),
    'multiplier': 1.25  # 25% Christmas surge
}
```

## Waterfall Tracking

Each adjustment type is tracked separately in the waterfall breakdown:

### Database Fields

For each adjustment type, the system tracks:
- `{type}_adj_qty` - Total adjustment quantity
- `{type}_adj_count` - Number of items affected
- `{type}_adj_name` - Name of the adjustment applied
- `{type}_adj_multiplier` - Multiplier used

### Example Waterfall

```
Starting Point: 5,000 units
+ Baseline Uplift: +200 units
+ Promo Adjustment: +150 units (BA_Summer_Promotion)
+ Holiday Increase: +300 units (Christmas_Week_NE)
- Cannibalism: -100 units (Item_123_Competitor_Launch)
+ Store Specific: +75 units (Grand_Opening_Store_999)
+ Cover: +250 units
+ Rounding: +5 units
- Store Pass: -50 units
- Weather: -80 units
= Final Forecast: 5,750 units
```

## Priority and Conflicts

### Adjustment Priority

Adjustments are applied in the order they appear in the `ADJUSTMENTS` list.

### One Adjustment Per Type

Only **one** adjustment of each type can apply to a given item/store/date:
- ✅ Can have: PROMO + HOLIDAY_INCREASE + CANNIBALISM
- ❌ Cannot have: Two PROMO adjustments on same item
- The **first matching** adjustment of each type wins

### Combining Adjustments

Multiple adjustment types can apply to the same forecast:

```python
# Item 12345, Store 101, BA region, Dec 20, 2026:
# 1. PROMO: 1.10x (regional promotion)
# 2. ITEM_SPECIFIC: 1.20x (new product launch)
# 3. CANNIBALISM: 0.90x (competing product)
# Net effect: 1.10 × 1.20 × 0.90 = 1.188x = 18.8% increase
```

## Filter Logic

### Region Filter
- `regions: ['BA', 'LA']` - Only applies to BA and LA
- `regions: None` - Applies to ALL regions

### Store Filter
- `stores: [101, 102, 103]` - Only these stores
- `stores: None` - All stores (if region matches)

### Item Filter
- `items: [123, 456]` - Only these items
- `items: None` - All items (if region/store matches)

### Combined Filters

```python
{
    'type': AdjustmentType.ITEM_SPECIFIC,
    'name': 'Specific_Item_Specific_Stores_Specific_Region',
    'regions': ['BA'],           # Only BA region
    'stores': [101, 102],        # Only stores 101 and 102
    'items': [123456],           # Only item 123456
    'start_date': datetime(2026, 1, 1),
    'end_date': datetime(2026, 1, 7),
    'multiplier': 1.50
}
# This applies ONLY to item 123456 in stores 101 and 102 in BA region
```

## Multiplier Guidelines

### Increases
- `1.05` = 5% increase
- `1.10` = 10% increase
- `1.20` = 20% increase
- `1.50` = 50% increase
- `2.00` = 100% increase (double)

### Decreases
- `0.95` = 5% decrease
- `0.90` = 10% decrease
- `0.85` = 15% decrease
- `0.70` = 30% decrease
- `0.50` = 50% decrease (half)

## Best Practices

### 1. Use Descriptive Names
✅ Good: `'BA_Summer_Promo_July2026'`
❌ Bad: `'Adjustment_1'`

### 2. Choose Appropriate Types
- Use `PROMO` for planned promotional events
- Use `ADHOC_*` for one-time unexpected changes
- Use `CANNIBALISM` for negative competitive impacts
- Use `HOLIDAY_INCREASE` for seasonal surges

### 3. Document Complex Adjustments
Add comments above complex adjustments:

```python
# Competitor opened new store near stores 101-105
# Expecting 15% cannibalism for 30 days
{
    'type': AdjustmentType.CANNIBALISM,
    'name': 'Competitor_Opening_Impact',
    'regions': None,
    'stores': [101, 102, 103, 104, 105],
    'start_date': datetime(2026, 3, 1),
    'end_date': datetime(2026, 3, 30),
    'multiplier': 0.85
}
```

### 4. Review Adjustment Order
- Similar adjustments should be grouped together
- Most general adjustments first, most specific last
- Document any dependencies

### 5. Archive Old Adjustments
Move completed adjustments to a separate section or file to keep the active list clean.

## Troubleshooting

### Adjustment Not Applied

**Check:**
1. Date range includes forecast date
2. Region code matches exactly (case-sensitive)
3. Store/item numbers are correct
4. Another adjustment of same type already matched (only first wins)

### Unexpected Results

**Debug:**
1. Query `forecast_results` table for the specific item/store/date
2. Check `{type}_adj_applied`, `{type}_adj_qty`, `{type}_adj_name` fields
3. Review adjustment order in ADJUSTMENTS list
4. Verify multiplier calculation

### Waterfall Not Showing Adjustment

**Verify:**
1. Adjustment is in `forecast_results` table
2. Aggregation query in `data/aggregates.py` includes the field
3. Export queries include the adjustment type
4. Re-run aggregation and export

## Migration from Old System

### Before (Old System)
- Separate lists: `PROMOTIONS`, `THANKSGIVING_ADJUSTMENTS`, `DECEMBER_ADJUSTMENTS`
- Manual tracking in individual functions
- Limited waterfall visibility

### After (New System)
- Single `ADJUSTMENTS` list
- Automatic tracking by type
- Complete waterfall transparency
- Easy to add new types

### Backward Compatibility
The system maintains these legacy fields for compatibility:
- `promo_uplift_applied`, `promo_uplift_qty`
- `holiday_adj_applied`, `holiday_adj_qty`
- `cannibalism_adj_applied`, `cannibalism_adj_qty`

## Summary

✅ **Dynamic** - Add adjustments without code changes
✅ **Typed** - Clear categorization for analysis
✅ **Tracked** - Full waterfall visibility
✅ **Flexible** - Filter by region, store, item
✅ **Scalable** - Easy to extend with new types

Just add your adjustment to the `ADJUSTMENTS` list and the system handles the rest!
