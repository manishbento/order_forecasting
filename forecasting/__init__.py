"""
Forecasting Package
===================
Contains core forecasting logic, adjustments, and rounding rules.

Modules:
- engine: Core forecasting calculations (velocity, EMA, base forecast)
- adjustments: Business rule adjustments (promotions, holidays)
- rounding: Case pack rounding and safety stock
- weather_adjustment: Weather-based forecast adjustments
"""

from .engine import (
    calculate_base_forecast,
    apply_decline_adjustment,
    apply_high_shrink_adjustment
)
from .adjustments import apply_all_adjustments
from .rounding import apply_all_rounding, calculate_safety_stock
from .weather_adjustment import (
    apply_weather_adjustments,
    get_weather_adjustment_summary,
    print_weather_adjustment_report
)

__all__ = [
    'calculate_base_forecast',
    'apply_decline_adjustment',
    'apply_high_shrink_adjustment',
    'apply_all_adjustments',
    'apply_all_rounding',
    'calculate_safety_stock',
    'apply_weather_adjustments',
    'get_weather_adjustment_summary',
    'print_weather_adjustment_report'
]
