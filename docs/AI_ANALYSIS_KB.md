# Costco Order Forecasting - AI Context Guide

## Overview

This system forecasts daily order quantities for perishable food items at Costco stores. The goal is to minimize shrink (waste) while avoiding sold-out situations.

## Key Concepts

### Data Hierarchy
- **Region** (`rgn`): Geographic grouping (BA=Bay Area, LA=Los Angeles, SD=San Diego, NE=Northeast, SE=Southeast, TE=Texas)
- **Store** (`sid`): Individual Costco warehouse, identified by store number
- **Item** (`iid`): Product SKU, identified by item number
- **Date** (`dt`): Forecast date

### Core Metrics

| Abbrev | Full Name | Description |
|--------|-----------|-------------|
| `fcst` | Forecast Quantity | Final order quantity to ship |
| `lw_ship` / `w1r` | Last Week Shipped | Units received last week (same day) |
| `lw_sold` / `w1s` | Last Week Sold | Units sold last week (same day) |
| `delta` | Delta from LW | fcst - lw_ship (change in order) |
| `delta_pct` | Delta % | Percentage change from last week |
| `shrk` / `shrink` | Shrink | Unsold units (shipped - sold) |
| `shrk_pct` | Shrink % | Shrink as percentage of shipped |

### Historical Data (4-Week Lookback)
- `w1s`, `w2s`, `w3s`, `w4s`: Sold quantities (W1=most recent)
- `w1r`, `w2r`, `w3r`, `w4r`: Received/shipped quantities
- `ema`: Exponential Moving Average (weighted: 60% W1, 20% W2, 10% W3, 10% W4)
- `avg`: Simple average of non-zero weeks
- `vel`: Sales velocity (trend slope)

## Forecast Waterfall

The forecast is built in stages. Each component adds or removes quantity:

```
LW Sold → Baseline Selection → EMA Uplift → Decline Adj → High Shrink Adj 
       → Base Cover → Rounding → Safety Stock → Store Pass → Weather Adj → FINAL
```

### Waterfall Components

| Component | Abbrev | Description | Direction |
|-----------|--------|-------------|-----------|
| **Baseline Source** | `bl_src` | Starting point selection | - |
| `lw_sales` | | Uses last week sold (LW >= EMA) | - |
| `ema` | | Uses EMA (LW < EMA, demand growing) | - |
| `average` | | Uses average (no LW shipments) | - |
| `minimum_case` | | Minimum 1 case (no recent sales) | - |
| **EMA Uplift** | `ema_up` | Added when LW < EMA (trending up) | + |
| **Decline Adjustment** | `decline` / `dec_adj` | Added for items with WoW decline pattern | + |
| **High Shrink Adj** | `shrk_adj` | Reduced for items with consistent high shrink | - |
| **Base Cover** | `cover` | Safety buffer for demand variability | + |
| `cover_so` | | Extra cover for sold-out items | + |
| **Rounding** | `rnd_net` | Adjustment to case pack size | ± |
| `rnd_dir` | | Direction: 'up', 'down', 'none' | |
| **Safety Stock** | `safety` | Additional buffer for volatility | + |
| **Store Pass** | `store_pass` / `st_adj` | Store-level shrink control | ± |
| `store_grow` | | Added when coverage too low | + |
| `store_decline` | | Removed for shrink control | - |
| **Weather** | `weather` / `wx_adj` | Reduction for severe weather | - |

## AI Analysis Levels

### 1. Executive Level (`ai_executive_*.json`)
- Overall forecast metrics across all regions
- Waterfall breakdown showing what's driving changes
- Regional comparison
- Daily trends

**Key questions to answer:**
- Is the overall forecast up or down vs last week? By how much?
- Which waterfall components are the largest contributors?
- Are any regions outliers?
- Any concerning weather impacts?

### 2. Regional Level (`ai_regional_*.json`)
- Daily waterfall breakdown per region
- Top stores by volume
- Top items by volume
- Anomaly detection (stores with large changes)
- Weather impact summary

**Key questions to answer:**
- Which days have the biggest changes?
- Which stores/items are driving regional changes?
- Are there anomalies that need review?

### 3. Store Detail Level (`ai_store_*.json`)
- Item-level forecast with full waterfall
- Historical 4-week data
- Items flagged for review (large adjustments)

**Key questions to answer:**
- Does the item-level forecast make sense given history?
- Are the waterfall adjustments appropriate?
- Any items that should be manually adjusted?

## Validation Guidelines

When reviewing forecasts, consider:

1. **Trend alignment**: Does forecast follow sales velocity direction?
2. **Shrink history**: High shrink items should trend down
3. **Sold-out history**: Sold-out items (`so_lw=1`) may need increase
4. **Weather appropriateness**: Severe weather should reduce perishable orders
5. **Outlier reasonability**: Large delta_pct (>20%) needs justification

## Flag Meanings

| Flag | Values | Meaning |
|------|--------|---------|
| `ema_up_f` | 0/1 | EMA uplift was applied |
| `dec_f` | 0/1 | Decline adjustment applied |
| `shrk_f` | 0/1 | High shrink adjustment applied |
| `st_adj_f` | 0/1 | Store-level adjustment applied |
| `wx_f` | 0/1 | Weather adjustment applied |
| `so_lw` | 0/1 | Item was sold out last week |

## Common Adjustment Reasons

### Store-Level (`st_adj_rsn`)
- `shrink_control`: Reduced due to high store shrink
- `coverage_add`: Increased due to low coverage

### Weather (`wx_rsn`)
- Typically describes weather condition causing reduction

## Data Efficiency Notes

- All exports use IDs only (no names) to minimize tokens
- Abbreviated field names used throughout
- Compact JSON format (no whitespace)
- Pre-aggregated metrics to avoid recalculation

## Output File Structure

```
output/ai_analysis/
├── ai_executive_{start}_{end}.json          # ~3KB - All regions summary
├── ai_regional_{rgn}_{start}_{end}.json     # ~8KB each - Per region detail
└── store_detail/
    ├── ai_stores_{rgn}_{start}_{end}.json   # Store index per region
    └── ai_store_{rgn}_{sid}_{start}_{end}.json  # Per store detail
```

## Usage with LangChain/LangGraph

### Loading Exports

```python
import json

# Executive level (start here)
with open('output/ai_analysis/ai_executive_2025-12-15_2025-12-17.json') as f:
    executive = json.load(f)

# Regional drill-down
with open('output/ai_analysis/ai_regional_BA_2025-12-15_2025-12-17.json') as f:
    regional = json.load(f)

# Store detail for validation
with open('output/ai_analysis/store_detail/ai_store_BA_1017_2025-12-15_2025-12-17.json') as f:
    store = json.load(f)
```

### Suggested Analysis Flow

1. **Executive Summary** → Identify overall trends and outliers
2. **Regional Drill-down** → Investigate regions with large deltas
3. **Store Detail** → Validate specific forecasts at item level

### Key Fields for Quick Analysis

| Level | Key Fields |
|-------|------------|
| Executive | `waterfall`, `by_region[].delta_pct` |
| Regional | `anomalies`, `top_stores[].delta_pct` |
| Store | `review[]`, `items[].delta` |
