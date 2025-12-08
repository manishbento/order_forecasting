# Weather-Based Forecast Adjustment Logic

## Overview
Weather adjustments reduce forecasted quantities when severe weather is expected to reduce store traffic. **Adjustments only trigger when severity score ≥ 4.0** (configurable threshold).

---

## Severity Score Calculation (0-10 Scale)

### Primary Factors (Actual Precipitation Only)

| Factor | Thresholds | Severity Contribution |
|--------|-----------|----------------------|
| **Rain** (effective = rain × prob%) | 0.1" light, 0.25" moderate, 0.5" heavy, 1.0" extreme | 0-10 scale |
| **Snow** (new snowfall) | 1" light, 3" moderate, 6" heavy, 12" blizzard | 0-10 scale |
| **Snow Depth** (existing) | 4" +2 sev, 8" +3 sev, 12" +4 sev | Adds to snow severity |

### Composite Score Formula
```
base_score = max(rain_severity, snow_severity)

compounding_bonus = 0
if precip_severity >= 2:
    if snow > rain: bonus += snow_severity × 0.25 (max 2.0)
if precip_cover >= 75%: bonus += precip_severity × 0.20 (max 1.5)
if precip_cover >= 50%: bonus += precip_severity × 0.15 (max 1.0)

composite = base_score + compounding_bonus (capped at 10)
```

### Severity Categories
| Score | Category | Description |
|-------|----------|-------------|
| 0-1.5 | MINIMAL | Clear/light conditions |
| 1.5-3 | LOW | Light rain/snow, no adjustment |
| 3-5 | MODERATE | Steady rain, snow depth 4-8" |
| 5-7 | HIGH | Heavy precip, snow depth 8-12" |
| 7+ | SEVERE | Dangerous conditions, blizzard |

---

## Sales Impact Factor

Converts severity to a reduction multiplier:

| Severity Score | Sales Impact Factor | Forecast Reduction |
|----------------|--------------------|--------------------|
| ≤2 | 1.00 | 0% (no adjustment) |
| 3 | 0.975 | 2.5% |
| 4 | 0.95 | 5% |
| 5 | 0.90 | 10% |
| 6 | 0.85 | 15% |
| 7 | 0.775 | 22.5% |
| 8 | 0.70 | 30% |
| 9 | 0.60 | 40% |
| 10 | 0.50 | 50% (max) |

---

## Adjustment Examples

### Example 1: Heavy Snow + Existing Depth
**Store 1101 on Dec 13** - Minnesota
- Snow: 1.7", Snow Depth: 12.7"
- Severity: **8.4** (SEVERE)
- Sales Impact: **66%** → 34% reduction

| Item | Base Forecast | After Weather Adj |
|------|--------------|-------------------|
| Croissants (case=12) | 48 units | 36 units (-12) |
| Bagels (case=6) | 24 units | 18 units (-6) |

### Example 2: Moderate Snow Conditions
**Store 391 on Dec 12** - Wisconsin
- Snow: 1.4", Snow Depth: 9.0"
- Severity: **6.8** (HIGH)
- Sales Impact: **79%** → 21% reduction

| Item | Base Forecast | After Weather Adj |
|------|--------------|-------------------|
| Muffins (case=6) | 30 units | 24 units (-6) |
| Danish (case=12) | 24 units | 24 units (kept min 1 case) |

### Example 3: Light Rain (No Adjustment)
**Store 1106 on Dec 9** - Texas
- Rain: 0.18", Snow: 0"
- Severity: **3.7** (MODERATE - below threshold)
- Sales Impact: 100% → **No adjustment**

### Example 4: Snow Depth Only (No Active Precip)
**Store 1020 on Dec 8** - Colorado
- Rain: 0", Snow: 0", Snow Depth: 4.4"
- Severity: **3.8** (MODERATE - below threshold)
- Sales Impact: 100% → **No adjustment**

This is intentional - existing snow depth without active precipitation doesn't significantly impact shopping if roads are cleared.

---

## Why Threshold = 4.0?

Based on data analysis of 351 store-days:

| Threshold | Store-Days Affected | Typical Conditions |
|-----------|--------------------|--------------------|
| ≥4.0 | 9.4% | 8"+ snow depth OR 0.5"+ heavy rain |
| ≥3.0 | 12.3% | 4-6" snow depth, often clear skies |
| ≥2.0 | 17.4% | Light rain, minor snow |

**Threshold 4.0 is appropriate because:**
1. Captures genuinely impactful weather (8"+ snow, heavy rain storms)
2. Avoids over-adjusting for cold-region stores with persistent but manageable snow
3. Minimum 1 case always maintained (protects against stockouts)

---

## Adjustment Constraints

1. **Minimum Quantity**: Always keep at least 1 case per item
2. **Case Rounding**: Reductions rounded to whole cases
3. **Max Store Reduction**: 40% cap regardless of severity
4. **Proportional**: All items reduced by same percentage (respecting minimums)
