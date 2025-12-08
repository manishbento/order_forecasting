# Excel Export Documentation

## Overview
The system generates two types of Excel exports for stakeholder review:
1. **Regional Summary** - Executive-level aggregated view
2. **PO Source Export** - Line-item detail for orders

---

## 1. Regional Summary Report
**File:** `Costco_{REGION}_Summary_{START}_{END}.xlsx`

### Sheets

#### 📊 Daily Summary
Aggregated metrics by forecast date for the entire region.

| Column | Description |
|--------|-------------|
| Forecast Date | Date being forecasted |
| Day Name | Day of week |
| Store Count | Number of stores |
| Line Count | Total item-store combinations |
| **Fcst Pre-Store Adj** | Forecast before store-level adjustments |
| **Store Level Adj** | Units adjusted by store pass |
| **Fcst Pre-Weather** | Forecast before weather adjustment |
| **Total Fcst Qty** | Final forecast quantity |
| **Weather Adj** | Units reduced due to weather |
| Fcst Average | Expected sales (4-week average) |
| **Shipped Trend** | W4→W3→W2→W1 shipped quantities |
| **Sold Trend** | W4→W3→W2→W1 sold quantities |
| Exp Shrink (Avg) % | (Forecast - Avg Sales) / Forecast |
| Exp Shrink (LW) % | (Forecast - LW Sold) / Forecast |
| LW Shrink % | Actual shrink from last week |
| Weather Counts | SEVERE/HIGH/MODERATE store counts |
| Delta from LW | Forecast vs last week shipped |

#### 🏬 Store Summary
Daily breakdown by individual store with weather indicators.

| Column | Description |
|--------|-------------|
| Forecast Date | Date being forecasted |
| Store # / Name | Store identifier |
| Item Count | Products forecasted |
| Total Fcst Qty | Final forecast quantity |
| Weather Adj | Units reduced for this store |
| Weather Severity | Max severity score (0-10) |
| Weather Indicator | ☀️⛈️❄️🔴 visual indicator |
| Shipped/Sold Trends | 4-week historical patterns |
| Expected Shrink % | Projected waste percentages |

#### 🌧️ Weather Impact
Weather details by store and date.

| Column | Description |
|--------|-------------|
| Date / Store | Location and time |
| Conditions | "Rain, Partially cloudy" |
| Temp Min/Max | Temperature range (°F) |
| Precip (in) | Expected precipitation |
| Snow (in) | New snowfall |
| Snow Depth | Existing accumulation |
| Wind Speed/Gust | Wind conditions |
| **Severity Score** | 0-10 composite score |
| **Severity Category** | MINIMAL/LOW/MODERATE/HIGH/SEVERE |
| **Items Adjusted** | Count of items reduced |
| **Total Adj Qty** | Units reduced |

#### 📋 Item Details
Full item/store granular view.

| Column | Description |
|--------|-------------|
| All date/store/item info | Full identification |
| Case Pack | Units per case |
| Fcst Pre-Store Adj | Before store pass |
| Store Adj Qty | Store-level reduction |
| Fcst Pre-Weather | Before weather |
| **Fcst Final** | Final order quantity |
| Fcst Cases | Final in cases |
| **Weather Adj** | Units reduced by weather |
| W1-W4 Ship/Sold | Historical 4-week data |
| Expected Shrink % | Projected waste |
| Weather Indicator | Visual status |

---

## 2. PO Source Export
**File:** `Costco_{REGION}_PO_{START}_{END}_SOURCE_WEATHER.xlsx`

### Sheets
One worksheet per day (MON, TUE, WED, etc.)

### Columns

| Column | Description |
|--------|-------------|
| Fiscal Week # | Week number |
| Date | Forecast date |
| Day Name | Day of week |
| Region | Region code |
| Warehouse # | Store number |
| Warehouse Name | Store name |
| Item # | Product code |
| Item Description | Product name |
| **PO Qty (Units)** | Final order quantity |
| **PO Qty (Cases)** | Order in cases |
| **Weather Status** | 🌧️ Rain: 0.5" \| Severity: 6.2 🟠 \| ⬇️ Adjusted: -12 units |
| **Weather Adj Qty** | Units reduced (highlighted orange/red) |
| LW Shrink % | Last week actual shrink |
| **Shipped Qty Trend** | 48 > 42 > 36 > 30 (W4→W1) |
| **Sold Qty Trend** | 45 > 40 > 35 > 28 (W4→W1) |

### Conditional Formatting
- **Weather Status**: 🔴 Red background if Adj ≥ 10 units
- **Weather Status**: 🟠 Orange background if Adj > 0
- **Weather Status**: 🟢 Green background if no adjustment

---

## Key Metrics Explained

### Shipped vs Sold Trends
- **Shipped**: What we sent to stores (W4→W3→W2→W1)
- **Sold**: What customers bought
- **Pattern**: Shows demand trajectory (increasing/decreasing)

### Expected Shrink %
- **From Avg**: (Forecast - 4wk Avg Sales) / Forecast
- **From LW**: (Forecast - Last Week Sold) / Forecast
- **Interpretation**: Higher % = more expected waste

### Weather Adjustment
- Only applies when severity ≥ 4.0
- Proportional reduction across all items
- Minimum 1 case always maintained
- See [Weather Adjustment Logic](WEATHER_ADJUSTMENT_LOGIC.md) for details

---

## Color Coding

| Color | Meaning |
|-------|---------|
| 🟢 Green | Good - No weather adjustment |
| 🟡 Yellow | Warning - Minor weather impact |
| 🟠 Orange | Caution - Moderate weather reduction |
| 🔴 Red | Alert - Significant weather reduction |

### Shrink % Thresholds
| Range | Color | Interpretation |
|-------|-------|----------------|
| < 10% | Green | Low expected waste |
| 10-25% | Yellow | Normal range |
| 25-40% | Orange | Higher than typical |
| > 40% | Red | Review forecast |
