# Weather Severity Score Logic v2

## Core Principle
The severity score (0-10) should reflect **how likely customers are to avoid going to the store** due to weather conditions. This directly impacts expected sales.

## What Prevents Customers from Shopping?

### Primary Factors (Direct Impact on Travel)
These are the main reasons customers stay home:

| Factor | Why It Matters | Impact Level |
|--------|----------------|--------------|
| **Heavy Rain** | Difficult/unpleasant to drive, walk, load groceries | Moderate-High |
| **Active Snow** | Dangerous roads, slow travel, accumulation | High |
| **Existing Snow Depth** | Roads may be icy/unplowed, parking lots hazardous | Moderate-High |
| **Ice/Freezing Rain** | Most dangerous road condition | Very High |
| **Poor Visibility** | Fog, heavy precip makes driving dangerous | Moderate-High |
| **High Winds** | Difficulty with carts, doors, walking; downed trees | Moderate |
| **Severe Storms** | Thunderstorms, tornadoes, hurricanes | High-Critical |

### Secondary Factors (Compound the Primary)
These make bad weather worse but aren't standalone deterrents:

| Factor | Effect |
|--------|--------|
| **Wind + Rain/Snow** | Driving rain, blowing snow - worse conditions |
| **Duration (precip_cover)** | All-day rain vs brief shower |
| **Cloud Cover** | Generally doesn't prevent shopping alone |
| **Temperature** | People shop in cold/hot - minor impact unless extreme |

## Severity Score Design (0-10 Scale)

### Score Interpretation
| Score | Category | Expected Sales Impact | Customer Behavior |
|-------|----------|----------------------|-------------------|
| 0-2 | MINIMAL | 0-3% reduction | Normal shopping |
| 2-4 | LOW | 3-7% reduction | Some may delay trips |
| 4-6 | MODERATE | 7-15% reduction | Many avoid unnecessary trips |
| 6-8 | HIGH | 15-30% reduction | Only essential trips |
| 8-10 | SEVERE | 30-50% reduction | Most stay home |

### Component Scoring

#### 1. Rain Severity (0-10)
Based on actual precipitation amount (inches):
```
Effective Rain = Actual Precip × (PrecipProb / 100)

0.0-0.1" : 0-2 (trace to light drizzle)
0.1-0.25": 2-4 (light rain)
0.25-0.5": 4-6 (moderate rain)
0.5-1.0" : 6-8 (heavy rain)
1.0+"    : 8-10 (extreme rain)
```

#### 2. Snow Severity (0-10)
Combines NEW snowfall + EXISTING snow depth:
```
New Snow Impact:
  0-1"  : 0-2 (dusting)
  1-3"  : 2-4 (light accumulation)
  3-6"  : 4-6 (moderate accumulation)  
  6-12" : 6-8 (heavy accumulation)
  12+"  : 8-10 (blizzard conditions)

Existing Snow Depth Bonus (adds to new snow impact):
  0-2"  : +0-1 (minimal ground cover)
  2-4"  : +1-2 (light accumulation on ground)
  4-8"  : +2-3 (moderate - roads may be slick)
  8-12" : +3-4 (heavy - parking lots likely bad)
  12+"  : +4-5 (severe - travel hazardous)

Total Snow Severity = min(10, New Snow Score + Depth Bonus)
```

#### 3. Wind Severity (0-10)
```
0-15 mph  : 0-1 (calm to breezy)
15-25 mph : 1-3 (windy - some difficulty)
25-40 mph : 3-6 (high winds - cart/door issues)
40-58 mph : 6-8 (storm force - dangerous)
58+ mph   : 8-10 (hurricane force)
```

#### 4. Visibility Severity (0-10)
```
10+ miles : 0 (clear)
5-10 miles: 0-2 (slightly reduced)
1-5 miles : 2-5 (noticeably reduced)
0.25-1 mi : 5-8 (low - driving difficult)
<0.25 mi  : 8-10 (fog/blizzard - dangerous)
```

#### 5. Severe Risk (from API, 0-100)
```
0-30  : 0 severity (normal weather)
30-50 : 2-4 severity (moderate storm risk)
50-70 : 4-6 severity (significant storm risk)
70-90 : 6-8 severity (high storm risk)
90+   : 8-10 severity (severe storm likely)
```

### Composite Score Calculation

```python
def calculate_composite_severity():
    # Step 1: Calculate base severity from precipitation
    precip_base = max(rain_severity, snow_severity)
    
    # Step 2: Include severe weather risk
    base_score = max(precip_base, severe_risk_severity)
    
    # Step 3: Add compounding effects (only if there's precipitation)
    compound = 0.0
    
    if precip_base >= 2:  # Has meaningful precipitation
        # Wind makes rain/snow worse
        if wind_severity >= 3:
            compound += min(1.5, wind_severity * 0.3)
        
        # Poor visibility with precipitation is dangerous
        if visibility_severity >= 3:
            compound += min(1.5, visibility_severity * 0.3)
        
        # Snow inherently worse than rain (accumulation, ice)
        if snow_severity > rain_severity:
            compound += min(1.0, snow_severity * 0.15)
        
        # Extended duration (all-day rain/snow)
        if precip_cover >= 50:  # >50% of hours have precip
            compound += min(1.0, precip_base * 0.15)
    
    # Step 4: Ice/freezing conditions are worst
    if has_ice_conditions:
        compound += 2.0  # Ice is extremely dangerous
    
    final_score = min(10.0, base_score + compound)
    return final_score
```

## Example Scenarios

| Scenario | Rain | Snow | Depth | Wind | Vis | Score | Category |
|----------|------|------|-------|------|-----|-------|----------|
| Clear sunny day | 0 | 0 | 0 | 5 | 10 | **0** | MINIMAL |
| Light drizzle | 0.1" | 0 | 0 | 10 | 8 | **2** | MINIMAL |
| Steady rain | 0.4" | 0 | 0 | 15 | 6 | **5** | MODERATE |
| Heavy rain + wind | 0.7" | 0 | 0 | 30 | 4 | **7** | HIGH |
| Light snow | 0 | 2" | 0 | 10 | 5 | **3** | LOW |
| Moderate snow | 0 | 4" | 2" | 20 | 3 | **6** | HIGH |
| Heavy snow + depth | 0 | 6" | 6" | 25 | 2 | **8** | SEVERE |
| Blizzard | 0 | 10" | 8" | 40 | 0.5 | **10** | SEVERE |
| Freezing rain | 0.3" | 0 | 0 | 15 | 5 | **7** | HIGH |
| Fog only | 0 | 0 | 0 | 5 | 0.25 | **3** | LOW |

## Temperature Philosophy

**Temperature alone does NOT significantly impact shopping:**
- People shop in 20°F weather (they dress warm)
- People shop in 95°F weather (car AC, store AC)

**When temperature matters:**
- Combined with precipitation (rain at 33°F = possible ice)
- Extreme cold (<0°F) makes car trips unpleasant
- Extreme heat (>105°F) discourages outdoor activity

**Implementation:** Temperature contributes only 0-2 points, and only at extremes.

## Sales Impact Factor

The severity score maps to expected sales reduction:

```python
def get_sales_impact_factor(severity_score):
    """
    Returns multiplier for expected sales.
    Example: 0.85 means expect 85% of normal sales (15% reduction)
    """
    if severity_score >= 8:
        return 0.55  # Severe: 45% reduction
    elif severity_score >= 6:
        return 0.75  # High: 25% reduction  
    elif severity_score >= 4:
        return 0.88  # Moderate: 12% reduction
    elif severity_score >= 2:
        return 0.95  # Low: 5% reduction
    else:
        return 1.00  # Minimal: no reduction
```

## Threshold for Adjustments

**Recommended threshold: 4.0**

- Below 4: Normal weather variation, no adjustment needed
- 4-6: Moderate impact, reduce forecast proportionally
- 6-8: High impact, more aggressive reduction
- 8+: Severe, maximum reduction applied

When severity >= 4:
- Calculate store-level reduction target
- Guarantee minimum 1 item × 1 case reduction (ensures some action taken)
- Prioritize non-hero items, then by forecast coverage
