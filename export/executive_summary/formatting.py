"""
Executive Summary Formatting Module
===================================
Excel formatting, colors, and styling for executive summary reports.

This module contains all formatting-related functions and constants
used for generating professional styled Excel reports.
"""

# =============================================================================
# COLOR PALETTE - Professional Executive Style
# =============================================================================

COLORS = {
    # Header colors
    'header_primary': '#1F4E79',      # Dark blue (main headers)
    'header_secondary': '#2E75B6',    # Medium blue (sub-headers)
    'header_font': '#FFFFFF',          # White text on headers
    
    # Section colors
    'section_bg': '#D6DCE5',          # Light gray-blue for sections
    'total_row_bg': '#BDD7EE',        # Light blue for totals
    'alt_row_bg': '#F2F2F2',          # Light gray for alternating rows
    
    # Status colors
    'positive_bg': '#C6EFCE',         # Light green
    'positive_font': '#006100',       # Dark green
    'negative_bg': '#FFC7CE',         # Light red
    'negative_font': '#9C0006',       # Dark red
    'warning_bg': '#FFEB9C',          # Light yellow/orange
    'warning_font': '#9C5700',        # Dark orange
    'neutral_bg': '#F2F2F2',          # Light gray
    'neutral_font': '#000000',        # Black
    
    # Weather severity colors
    'severe_bg': '#FF6B6B',           # Red
    'severe_font': '#FFFFFF',
    'high_bg': '#FFA94D',             # Orange
    'high_font': '#000000',
    'moderate_bg': '#FFE066',         # Yellow
    'moderate_font': '#000000',
    'low_bg': '#69DB7C',              # Green
    'low_font': '#000000',
    'minimal_bg': '#A9E34B',          # Light green
    'minimal_font': '#000000',
    
    # Waterfall specific colors
    'baseline_bg': '#5B9BD5',         # Blue
    'adjustment_up_bg': '#70AD47',    # Green (increases)
    'adjustment_down_bg': '#ED7D31',  # Orange (decreases)
    'final_bg': '#4472C4',            # Dark blue
}

# Weather icons for display
SEVERITY_ICONS = {
    'SEVERE': '🔴',
    'HIGH': '🟠',
    'MODERATE': '🟡',
    'LOW': '🟢',
    'MINIMAL': '✅',
    None: '✅'
}


def create_executive_formats(wb):
    """
    Create all custom formats for the executive summary workbook.
    
    Args:
        wb: xlsxwriter workbook object
        
    Returns:
        Dictionary of format objects
    """
    formats = {}
    
    # ==========================================================================
    # Title and Header Formats
    # ==========================================================================
    formats['title'] = wb.add_format({
        'bold': True,
        'font_size': 18,
        'font_color': COLORS['header_primary'],
        'align': 'left',
        'valign': 'vcenter',
    })
    
    formats['subtitle'] = wb.add_format({
        'bold': True,
        'font_size': 12,
        'font_color': COLORS['header_secondary'],
        'align': 'left',
        'valign': 'vcenter',
    })
    
    formats['section_title'] = wb.add_format({
        'bold': True,
        'font_size': 14,
        'font_color': COLORS['header_primary'],
        'align': 'left',
        'valign': 'vcenter',
        'bottom': 2,
        'bottom_color': COLORS['header_primary'],
    })
    
    # Primary header (dark blue background)
    formats['header_primary'] = wb.add_format({
        'bold': True,
        'font_size': 10,
        'font_color': COLORS['header_font'],
        'bg_color': COLORS['header_primary'],
        'align': 'center',
        'valign': 'vcenter',
        'text_wrap': True,
        'border': 1,
        'border_color': '#FFFFFF',
    })
    
    # Secondary header (medium blue background)
    formats['header_secondary'] = wb.add_format({
        'bold': True,
        'font_size': 10,
        'font_color': COLORS['header_font'],
        'bg_color': COLORS['header_secondary'],
        'align': 'center',
        'valign': 'vcenter',
        'text_wrap': True,
        'border': 1,
        'border_color': '#FFFFFF',
    })
    
    # Region/group header
    formats['region_header'] = wb.add_format({
        'bold': True,
        'font_size': 11,
        'font_color': '#000000',
        'bg_color': COLORS['section_bg'],
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
    })
    
    # ==========================================================================
    # Data Cell Formats
    # ==========================================================================
    
    # Date format
    formats['date'] = wb.add_format({
        'num_format': 'yyyy-mm-dd',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Text centered
    formats['text_center'] = wb.add_format({
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Text left
    formats['text_left'] = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Number format (with comma separator)
    formats['number'] = wb.add_format({
        'num_format': '#,##0',
        'align': 'right',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Decimal format
    formats['decimal'] = wb.add_format({
        'num_format': '#,##0.00',
        'align': 'right',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Percentage format
    formats['percent'] = wb.add_format({
        'num_format': '0.0%',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # ==========================================================================
    # Conditional Percentage Formats
    # ==========================================================================
    
    # Positive percentage (green)
    formats['percent_positive'] = wb.add_format({
        'num_format': '+0.0%',
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['positive_font'],
        'bg_color': COLORS['positive_bg'],
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Negative percentage (red)
    formats['percent_negative'] = wb.add_format({
        'num_format': '0.0%',
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['negative_font'],
        'bg_color': COLORS['negative_bg'],
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Neutral percentage
    formats['percent_neutral'] = wb.add_format({
        'num_format': '0.0%',
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # Warning percentage (yellow/orange)
    formats['percent_warning'] = wb.add_format({
        'num_format': '0.0%',
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['warning_font'],
        'bg_color': COLORS['warning_bg'],
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # ==========================================================================
    # Total Row Formats
    # ==========================================================================
    
    formats['total_label'] = wb.add_format({
        'bold': True,
        'font_size': 10,
        'align': 'left',
        'valign': 'vcenter',
        'bg_color': COLORS['total_row_bg'],
        'border': 1,
    })
    
    formats['total_number'] = wb.add_format({
        'bold': True,
        'num_format': '#,##0',
        'align': 'right',
        'valign': 'vcenter',
        'bg_color': COLORS['total_row_bg'],
        'border': 1,
    })
    
    formats['total_percent'] = wb.add_format({
        'bold': True,
        'num_format': '0.0%',
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': COLORS['total_row_bg'],
        'border': 1,
    })
    
    # ==========================================================================
    # Waterfall Specific Formats
    # ==========================================================================
    
    # Starting point (blue)
    formats['waterfall_start'] = wb.add_format({
        'bold': True,
        'num_format': '#,##0',
        'align': 'right',
        'valign': 'vcenter',
        'font_color': '#FFFFFF',
        'bg_color': COLORS['baseline_bg'],
        'border': 1,
    })
    
    # Adjustment increase (green)
    formats['waterfall_increase'] = wb.add_format({
        'num_format': '+#,##0',
        'align': 'right',
        'valign': 'vcenter',
        'font_color': COLORS['positive_font'],
        'bg_color': COLORS['positive_bg'],
        'border': 1,
    })
    
    # Adjustment decrease (red)
    formats['waterfall_decrease'] = wb.add_format({
        'num_format': '#,##0',
        'align': 'right',
        'valign': 'vcenter',
        'font_color': COLORS['negative_font'],
        'bg_color': COLORS['negative_bg'],
        'border': 1,
    })
    
    # Final total (dark blue)
    formats['waterfall_final'] = wb.add_format({
        'bold': True,
        'num_format': '#,##0',
        'align': 'right',
        'valign': 'vcenter',
        'font_color': '#FFFFFF',
        'bg_color': COLORS['final_bg'],
        'border': 1,
    })
    
    # Combined quantity and percentage format
    formats['qty_pct_cell'] = wb.add_format({
        'align': 'center',
        'valign': 'vcenter',
        'border': 1,
        'border_color': '#D0D0D0',
    })
    
    # ==========================================================================
    # Weather Severity Formats
    # ==========================================================================
    
    formats['severity_severe'] = wb.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['severe_font'],
        'bg_color': COLORS['severe_bg'],
        'border': 1,
    })
    
    formats['severity_high'] = wb.add_format({
        'bold': True,
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['high_font'],
        'bg_color': COLORS['high_bg'],
        'border': 1,
    })
    
    formats['severity_moderate'] = wb.add_format({
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['moderate_font'],
        'bg_color': COLORS['moderate_bg'],
        'border': 1,
    })
    
    formats['severity_low'] = wb.add_format({
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['low_font'],
        'bg_color': COLORS['low_bg'],
        'border': 1,
    })
    
    formats['severity_minimal'] = wb.add_format({
        'align': 'center',
        'valign': 'vcenter',
        'font_color': COLORS['minimal_font'],
        'bg_color': COLORS['minimal_bg'],
        'border': 1,
    })
    
    return formats


def get_delta_format(formats: dict, value: float, threshold: float = 0.0) -> object:
    """
    Get appropriate format for a delta/change value.
    
    Args:
        formats: Dictionary of format objects
        value: The delta value
        threshold: Threshold for neutral (default 0)
        
    Returns:
        Appropriate format object
    """
    if value is None:
        return formats['percent_neutral']
    
    if value > threshold:
        return formats['percent_positive']
    elif value < -threshold:
        return formats['percent_negative']
    else:
        return formats['percent_neutral']


def get_shrink_format(formats: dict, value: float) -> object:
    """
    Get appropriate format for shrink percentage.
    
    Good shrink: 0-8%
    Warning shrink: 8-15%
    Bad shrink: >15%
    
    Args:
        formats: Dictionary of format objects
        value: The shrink percentage value
        
    Returns:
        Appropriate format object
    """
    if value is None:
        return formats['percent_neutral']
    
    if value <= 0.08:
        return formats['percent_positive']
    elif value <= 0.15:
        return formats['percent_warning']
    else:
        return formats['percent_negative']


def get_waterfall_format(formats: dict, value: float) -> object:
    """
    Get appropriate format for waterfall adjustment values.
    
    Args:
        formats: Dictionary of format objects
        value: The adjustment value
        
    Returns:
        Appropriate format object
    """
    if value is None or value == 0:
        return formats['number']
    
    if value > 0:
        return formats['waterfall_increase']
    else:
        return formats['waterfall_decrease']


def get_severity_format(formats: dict, category: str) -> object:
    """
    Get appropriate format for weather severity category.
    
    Args:
        formats: Dictionary of format objects
        category: Weather severity category
        
    Returns:
        Appropriate format object
    """
    category_map = {
        'SEVERE': formats['severity_severe'],
        'HIGH': formats['severity_high'],
        'MODERATE': formats['severity_moderate'],
        'LOW': formats['severity_low'],
        'MINIMAL': formats['severity_minimal'],
    }
    return category_map.get(category, formats['severity_minimal'])


def format_qty_with_pct(qty: float, pct: float) -> str:
    """
    Format quantity with percentage in a single cell.
    Shows proper sign for adjustments.
    
    Example: "+1,234 (+5.2%)" for increases, "-500 (-2.1%)" for decreases
    
    Args:
        qty: Quantity value (can be positive or negative)
        pct: Percentage value (as decimal, e.g., 0.052 or -0.021)
        
    Returns:
        Formatted string with proper signs
    """
    if qty is None or qty == 0:
        return "0 (0.0%)"
    
    # Format with sign
    if qty > 0:
        qty_str = f"+{qty:,.0f}"
        pct_str = f"+{pct * 100:.1f}%"
    else:
        qty_str = f"{qty:,.0f}"  # Negative sign is automatic
        pct_str = f"{pct * 100:.1f}%"  # Negative sign is automatic
    
    return f"{qty_str} ({pct_str})"


def format_delta_with_pct(delta: float, pct: float) -> str:
    """
    Format delta with percentage for display.
    
    Example: "+1,234 (+5.2%)" or "-1,234 (-5.2%)"
    
    Args:
        delta: Delta quantity value
        pct: Percentage value (as decimal)
        
    Returns:
        Formatted string
    """
    if delta >= 0:
        return f"+{delta:,.0f} (+{pct * 100:.1f}%)"
    else:
        return f"{delta:,.0f} ({pct * 100:.1f}%)"
