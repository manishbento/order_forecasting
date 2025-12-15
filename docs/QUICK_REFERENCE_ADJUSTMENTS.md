# Quick Reference: Adding New Adjustments

## 🚀 3-Step Process

### 1️⃣ Choose Type
```python
from enum import Enum

class AdjustmentType(Enum):
    PROMO               # Promotions
    HOLIDAY_INCREASE    # Holiday uplifts
    CANNIBALISM         # Competition/substitution
    ADHOC_INCREASE      # One-time increases
    ADHOC_DECREASE      # One-time decreases
    STORE_SPECIFIC      # Store-level
    ITEM_SPECIFIC       # Item-level
    REGIONAL            # Region-level
```

### 2️⃣ Add to ADJUSTMENTS List
```python
# Open: forecasting/adjustments.py
# Find: ADJUSTMENTS = [...]
# Add:

{
    'type': AdjustmentType.PROMO,           # Required
    'name': 'Your_Adjustment_Name',         # Required
    'regions': ['BA', 'LA'],                # Required (or None)
    'start_date': datetime(2026, 1, 1),     # Required
    'end_date': datetime(2026, 1, 7),       # Required
    'multiplier': 1.15,                     # Required
    'stores': [101, 102],                   # Optional
    'items': [123, 456],                    # Optional
}
```

### 3️⃣ Run Forecast
✅ Done! Your adjustment is now:
- Applied during forecasting
- Tracked in database
- Visible in waterfall
- Included in exports

---

## 📋 Common Patterns

### Region-Wide Promotion
```python
{
    'type': AdjustmentType.PROMO,
    'name': 'Summer_Sale_BA',
    'regions': ['BA'],
    'start_date': datetime(2026, 7, 1),
    'end_date': datetime(2026, 7, 7),
    'multiplier': 1.15  # 15% increase
}
```

### Specific Stores Only
```python
{
    'type': AdjustmentType.STORE_SPECIFIC,
    'name': 'High_Volume_Weekend',
    'regions': None,  # All regions
    'stores': [101, 102, 103],
    'start_date': datetime(2026, 8, 15),
    'end_date': datetime(2026, 8, 17),
    'multiplier': 1.25  # 25% increase
}
```

### Specific Item + Region
```python
{
    'type': AdjustmentType.ITEM_SPECIFIC,
    'name': 'Item_123_LA_Launch',
    'regions': ['LA'],
    'items': [123456],
    'start_date': datetime(2026, 9, 1),
    'end_date': datetime(2026, 9, 30),
    'multiplier': 1.50  # 50% launch boost
}
```

### Cannibalism Effect
```python
{
    'type': AdjustmentType.CANNIBALISM,
    'name': 'Competitor_Impact',
    'regions': ['SD', 'SE'],
    'start_date': datetime(2026, 10, 1),
    'end_date': datetime(2026, 10, 31),
    'multiplier': 0.85  # 15% decrease
}
```

---

## 🎯 Multiplier Guide

| Multiplier | Effect |
|------------|--------|
| 2.00 | +100% (double) |
| 1.50 | +50% |
| 1.25 | +25% |
| 1.15 | +15% |
| 1.10 | +10% |
| 1.05 | +5% |
| 1.00 | No change |
| 0.95 | -5% |
| 0.90 | -10% |
| 0.85 | -15% |
| 0.75 | -25% |
| 0.50 | -50% (half) |

---

## 🔍 Filter Logic

### All Filters Must Match
```python
{
    'regions': ['BA'],      # ✓ Must be BA
    'stores': [101, 102],   # ✓ AND must be store 101 or 102
    'items': [123],         # ✓ AND must be item 123
    # Result: Only item 123 in stores 101/102 in BA
}
```

### None = No Filter
```python
{
    'regions': None,  # All regions
    'stores': None,   # All stores
    'items': [123],   # Only item 123
    # Result: Item 123 everywhere
}
```

---

## ⚠️ Important Rules

1. **One Per Type**: Only ONE adjustment of each type applies per item/store/date
2. **Order Matters**: First matching adjustment wins (within same type)
3. **Types Stack**: Different types multiply together
   - PROMO (1.10x) + CANNIBALISM (0.90x) = 0.99x net

---

## 📁 File Locations

- **Add Adjustments**: `forecasting/adjustments.py`
- **Schema Definition**: `data/aggregates.py`
- **Full Guide**: `docs/ADJUSTMENT_TYPES_GUIDE.md`
- **Summary**: `docs/ADJUSTMENT_SYSTEM_SUMMARY.md`

---

## 🐛 Troubleshooting

### Not Applied?
- ✓ Check date range
- ✓ Check region spelling (case-sensitive)
- ✓ Check store/item numbers
- ✓ Check if another adjustment of same type already matched

### Need Help?
See `docs/ADJUSTMENT_TYPES_GUIDE.md` for detailed examples
