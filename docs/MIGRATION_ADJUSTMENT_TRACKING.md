# Migration Guide: Adjustment Type Tracking

## Current Status

✅ **Code Updated** - New adjustment system is ready to use
⚠️ **Database Schema** - New fields not yet in `forecast_results` table
📋 **Action Required** - Run forecast to populate new fields

## What Happened

The aggregation query tried to read new adjustment fields (`promo_adj_qty`, `holiday_increase_adj_qty`, etc.) from the `forecast_results` table, but these fields don't exist yet because:

1. The forecast was run with the OLD adjustment system
2. The NEW adjustment system hasn't created these fields yet
3. The fields will be created when you run the forecast engine

## Temporary Fix Applied

Modified `data/aggregates.py` to use default values (0) for new adjustment fields until the forecast is re-run:

```python
# Instead of reading from forecast_results:
# SUM(COALESCE(fr.promo_adj_qty, 0)) AS promo_adj_qty,

# Using temporary default:
0 AS promo_adj_qty,
```

## To Complete Migration

### Step 1: Run Forecast
```bash
python main.py
```

This will:
- Use the new `apply_adjustments()` function
- Create all new adjustment type fields in `forecast_results`
- Populate fields like `promo_adj_qty`, `cannibalism_adj_qty`, etc.

### Step 2: Update Aggregation Query

Once forecast runs successfully, update `data/aggregates.py` line 264-296 to change from:

```python
# CURRENT (Temporary):
0 AS promo_adj_qty,
0 AS promo_adj_count,
```

To:

```python
# AFTER FORECAST RUN:
SUM(COALESCE(fr.promo_adj_qty, 0)) AS promo_adj_qty,
SUM(CASE WHEN fr.promo_adj_applied = 1 THEN 1 ELSE 0 END) AS promo_adj_count,
```

Repeat for all 8 adjustment types:
- `promo_adj_qty` / `promo_adj_count`
- `holiday_increase_adj_qty` / `holiday_increase_adj_count`
- `cannibalism_adj_qty` / `cannibalism_adj_count`
- `adhoc_increase_adj_qty` / `adhoc_increase_adj_count`
- `adhoc_decrease_adj_qty` / `adhoc_decrease_adj_count`
- `store_specific_adj_qty` / `store_specific_adj_count`
- `item_specific_adj_qty` / `item_specific_adj_count`
- `regional_adj_qty` / `regional_adj_count`

### Step 3: Verify

After updating the query:
1. Run aggregation: `populate_waterfall_aggregate()`
2. Check `waterfall_aggregate` table has non-zero adjustment values
3. Verify exports show adjustment breakdowns

## Verification Query

After running forecast, check if fields exist:

```sql
-- Check forecast_results schema
PRAGMA table_info(forecast_results);

-- Check if adjustment fields exist and have data
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN promo_adj_applied = 1 THEN 1 ELSE 0 END) as promo_count,
    SUM(promo_adj_qty) as total_promo_qty,
    SUM(CASE WHEN cannibalism_adj_applied = 1 THEN 1 ELSE 0 END) as cannibalism_count,
    SUM(cannibalism_adj_qty) as total_cannibalism_qty
FROM forecast_results
WHERE date_forecast >= '2025-12-18';
```

Expected result: Non-zero counts for adjustments that should be active.

## Timeline

1. **Now**: Aggregation uses 0 for new fields (won't break, but won't show new adjustments)
2. **After forecast run**: New fields will exist in `forecast_results`
3. **After query update**: Aggregation will pull actual adjustment values
4. **After export**: Waterfall will show complete breakdown by adjustment type

## Backward Compatibility

The system maintains these legacy fields:
- `promo_uplift_qty` - Still populated for compatibility
- `holiday_adj_qty` - Still populated for compatibility
- `cannibalism_adj_qty` - Used by both old and new system

So existing exports will continue working while you migrate.

## Need Help?

The modified fields are in:
- `data/aggregates.py` lines 264-296
- Look for comment: `-- ===== NEW: Adjustment Type Tracking =====`

Once forecast runs, simply change `0 AS field_name` to `SUM(COALESCE(fr.field_name, 0)) AS field_name`
