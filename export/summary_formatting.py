"""
Summary Formatting Module
=========================
Formatting helpers, colors, and icon definitions for regional summary reports.

This module contains all formatting-related functions and constants
used for generating styled Excel reports.
"""

# =============================================================================
# WEATHER INDICATOR ICONS
# =============================================================================

WEATHER_ICONS = {
    'clear': '☀️',
    'partly_cloudy': '⛅',
    'cloudy': '☁️',
    'rain_light': '🌦️',
    'rain': '🌧️',
    'rain_heavy': '⛈️',
    'thunderstorm': '🌩️',
    'snow_light': '🌨️',
    'snow': '❄️',
    'snow_heavy': '🌨️❄️',
    'fog': '🌫️',
    'wind': '💨',
    'extreme_cold': '🥶',
    'extreme_heat': '🥵',
    'severe': '⚠️',
    'unknown': '❓'
}

SEVERITY_ICONS = {
    'MINIMAL': '✅',
    'LOW': '🟢',
    'MODERATE': '🟡',
    'HIGH': '🟠',
    'SEVERE': '🔴'
}

# =============================================================================
# COLOR PALETTES FOR CONDITIONAL FORMATTING
# =============================================================================

COLORS = {
    'header_bg': '#2F5496',           # Dark blue
    'header_font': '#FFFFFF',          # White
    'subheader_bg': '#5B9BD5',        # Medium blue
    'subheader_font': '#FFFFFF',       # White
    'section_bg': '#D6DCE5',          # Light gray-blue
    'total_bg': '#E2EFDA',            # Light green
    'alt_row_bg': '#F2F2F2',          # Light gray for alternating rows
    
    # Conditional formatting colors
    'good_bg': '#C6EFCE',             # Light green
    'good_font': '#006100',           # Dark green
    'warning_bg': '#FFEB9C',          # Light yellow/orange
    'warning_font': '#9C6500',        # Dark orange
    'bad_bg': '#FFC7CE',              # Light red
    'bad_font': '#9C0006',            # Dark red
    'neutral_bg': '#F2F2F2',          # Light gray
    
    # Weather severity colors
    'severe_bg': '#FF6B6B',           # Red
    'high_bg': '#FFA94D',             # Orange
    'moderate_bg': '#FFE066',         # Yellow
    'low_bg': '#69DB7C',              # Green
    'minimal_bg': '#A9E34B',          # Light green
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_weather_indicator_icon(condition: str, severity_category: str = None,
                                snow_amount: float = 0, rain_amount: float = 0,
                                temp_min: float = None, temp_max: float = None,
                                wind_speed: float = 0, severity_score: float = 0) -> str:
    """
    Get weather indicator icon based on conditions.
    
    Args:
        condition: Weather condition string
        severity_category: Severity category (SEVERE, HIGH, MODERATE, LOW, MINIMAL)
        snow_amount: Snow amount in inches
        rain_amount: Rain amount in inches
        temp_min: Minimum temperature
        temp_max: Maximum temperature
        wind_speed: Wind speed in mph
        severity_score: Overall severity score
        
    Returns:
        Weather indicator icon string
    """
    if not condition:
        return SEVERITY_ICONS.get(severity_category, '❓')
    
    condition_lower = condition.lower()
    
    # Severe conditions
    if severity_score >= 7 or severity_category == 'SEVERE':
        return WEATHER_ICONS['severe']
    
    # Snow
    if snow_amount >= 6 or 'blizzard' in condition_lower:
        return WEATHER_ICONS['snow_heavy']
    if snow_amount >= 2 or 'snow' in condition_lower:
        return WEATHER_ICONS['snow']
    if snow_amount > 0:
        return WEATHER_ICONS['snow_light']
    
    # Rain/storms
    if 'thunder' in condition_lower or 'storm' in condition_lower:
        return WEATHER_ICONS['thunderstorm']
    if rain_amount >= 1:
        return WEATHER_ICONS['rain_heavy']
    if rain_amount >= 0.25 or 'rain' in condition_lower:
        return WEATHER_ICONS['rain']
    if rain_amount > 0 or 'drizzle' in condition_lower or 'shower' in condition_lower:
        return WEATHER_ICONS['rain_light']
    
    # Fog
    if 'fog' in condition_lower or 'mist' in condition_lower:
        return WEATHER_ICONS['fog']
    
    # Wind
    if wind_speed >= 30:
        return WEATHER_ICONS['wind']
    
    # Temperature extremes
    if temp_min is not None and temp_min < 20:
        return WEATHER_ICONS['extreme_cold']
    if temp_max is not None and temp_max > 100:
        return WEATHER_ICONS['extreme_heat']
    
    # Clear/cloudy
    if 'clear' in condition_lower or 'sunny' in condition_lower:
        return WEATHER_ICONS['clear']
    if 'partly' in condition_lower or 'partial' in condition_lower:
        return WEATHER_ICONS['partly_cloudy']
    if 'cloud' in condition_lower or 'overcast' in condition_lower:
        return WEATHER_ICONS['cloudy']
    
    return SEVERITY_ICONS.get(severity_category, WEATHER_ICONS['clear'])


def create_summary_formats(wb):
    """
    Create all custom formats for the summary workbook.
    
    Args:
        wb: xlsxwriter Workbook object
        
    Returns:
        Dictionary of format objects
    """
    formats = {}
    
    # Title format
    formats['title'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 16,
        'bold': True,
        'font_color': COLORS['header_font'],
        'bg_color': COLORS['header_bg'],
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })
    
    # Subtitle format
    formats['subtitle'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 12,
        'bold': True,
        'font_color': COLORS['subheader_font'],
        'bg_color': COLORS['subheader_bg'],
        'align': 'center',
        'valign': 'vcenter',
        'border': 1
    })
    
    # Section header
    formats['section'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 11,
        'bold': True,
        'bg_color': COLORS['section_bg'],
        'border': 1
    })
    
    # Column headers
    formats['col_header'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'font_size': 10,
        'bold': True,
        'font_color': COLORS['header_font'],
        'bg_color': COLORS['header_bg'],
        'align': 'center',
        'valign': 'vcenter',
        'text_wrap': True,
        'border': 1
    })
    
    # Number formats
    formats['number'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0',
        'align': 'right',
        'border': 1
    })
    
    formats['number_bold'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0',
        'align': 'right',
        'bold': True,
        'bg_color': COLORS['total_bg'],
        'border': 1
    })
    
    formats['decimal'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0.0',
        'align': 'right',
        'border': 1
    })
    
    formats['decimal2'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0.00',
        'align': 'right',
        'border': 1
    })
    
    formats['decimal3'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '#,##0.000',
        'align': 'right',
        'border': 1
    })
    
    formats['currency'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '$#,##0.00',
        'align': 'right',
        'border': 1
    })
    
    # Percentage formats
    formats['pct'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'border': 1
    })
    
    formats['pct_bold'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bold': True,
        'bg_color': COLORS['total_bg'],
        'border': 1
    })
    
    # Conditional percentage formats
    formats['pct_good'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bg_color': COLORS['good_bg'],
        'font_color': COLORS['good_font'],
        'border': 1
    })
    
    formats['pct_warning'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bg_color': COLORS['warning_bg'],
        'font_color': COLORS['warning_font'],
        'border': 1
    })
    
    formats['pct_bad'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bg_color': COLORS['bad_bg'],
        'font_color': COLORS['bad_font'],
        'border': 1
    })
    
    # Text formats
    formats['text'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'left',
        'border': 1
    })
    
    formats['text_center'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'border': 1
    })
    
    formats['text_bold'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'bold': True,
        'align': 'left',
        'bg_color': COLORS['total_bg'],
        'border': 1
    })
    
    # Date format
    formats['date'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': 'yyyy-mm-dd',
        'align': 'center',
        'border': 1
    })
    
    # Weather severity formats
    formats['severity_severe'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['severe_bg'],
        'font_color': 'white',
        'bold': True,
        'border': 1
    })
    
    formats['severity_high'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['high_bg'],
        'font_color': 'white',
        'border': 1
    })
    
    formats['severity_moderate'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['moderate_bg'],
        'border': 1
    })
    
    formats['severity_low'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['low_bg'],
        'border': 1
    })
    
    formats['severity_minimal'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'bg_color': COLORS['minimal_bg'],
        'border': 1
    })
    
    # Trend string format
    formats['trend'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'align': 'center',
        'font_size': 10,
        'border': 1
    })
    
    # Growth positive/negative formats
    formats['growth_positive'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '+0.0%',
        'align': 'center',
        'bg_color': COLORS['warning_bg'],
        'font_color': COLORS['warning_font'],
        'border': 1
    })
    
    formats['growth_negative'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'bg_color': COLORS['good_bg'],
        'font_color': COLORS['good_font'],
        'border': 1
    })
    
    formats['growth_neutral'] = wb.add_format({
        'font_name': 'Aptos Narrow',
        'num_format': '0.0%',
        'align': 'center',
        'border': 1
    })
    
    return formats


def get_severity_format(formats: dict, severity_score: float, category: str = None):
    """
    Get appropriate format based on weather severity.
    
    Args:
        formats: Dictionary of format objects
        severity_score: Weather severity score (0-10)
        category: Severity category string
        
    Returns:
        Format object
    """
    if category:
        category = category.upper()
        if category == 'SEVERE':
            return formats['severity_severe']
        elif category == 'HIGH':
            return formats['severity_high']
        elif category == 'MODERATE':
            return formats['severity_moderate']
        elif category == 'LOW':
            return formats['severity_low']
        else:
            return formats['severity_minimal']
    
    if severity_score >= 8:
        return formats['severity_severe']
    elif severity_score >= 6:
        return formats['severity_high']
    elif severity_score >= 4:
        return formats['severity_moderate']
    elif severity_score >= 2:
        return formats['severity_low']
    else:
        return formats['severity_minimal']


def get_shrink_pct_format(formats: dict, shrink_pct: float):
    """
    Get appropriate format based on shrink percentage.
    
    Args:
        formats: Dictionary of format objects
        shrink_pct: Shrink percentage (as decimal, e.g., 0.15 = 15%)
        
    Returns:
        Format object
    """
    if shrink_pct is None:
        return formats['pct']
    if shrink_pct <= 0.05:  # 5% or less = good
        return formats['pct_good']
    elif shrink_pct <= 0.15:  # 5-15% = warning
        return formats['pct_warning']
    else:  # Over 15% = bad
        return formats['pct_bad']


def get_growth_pct_format(formats: dict, growth_pct: float):
    """
    Get appropriate format based on growth percentage.
    Positive growth (forecast > historical) is highlighted as a warning.
    
    Args:
        formats: Dictionary of format objects
        growth_pct: Growth percentage (as decimal, e.g., 0.05 = 5%)
        
    Returns:
        Format object
    """
    if growth_pct is None:
        return formats['pct']
    if growth_pct > 0.02:  # More than 2% growth - warning (may cause shrink)
        return formats['growth_positive']
    elif growth_pct < -0.02:  # More than 2% decline - good (conservative)
        return formats['growth_negative']
    else:
        return formats['growth_neutral']


def build_sales_trend_string(w4: int, w3: int, w2: int, w1: int) -> str:
    """
    Build sales trend string: W4 > W3 > W2 > W1.
    
    Args:
        w4, w3, w2, w1: Weekly sales values
        
    Returns:
        Formatted trend string
    """
    def fmt(val):
        if val is None:
            return "-"
        return f"{int(val):,}"
    
    return f"{fmt(w4)} > {fmt(w3)} > {fmt(w2)} > {fmt(w1)}"
