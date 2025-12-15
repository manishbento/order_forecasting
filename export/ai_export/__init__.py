"""
AI Export Module
================
Exports forecast data in compact, token-efficient formats for LLM consumption.

This module provides three levels of analysis:
1. Executive Summary - High-level insights across all regions
2. Regional Analysis - Region-level drivers and trends
3. Store Detail - Store/item level for validation

All exports use:
- IDs only (no names) to minimize tokens
- Abbreviated field names
- Compact JSON structure
- Pre-computed metrics and insights
"""

from .executive import export_executive_ai
from .regional import export_regional_ai
from .store_detail import export_store_detail_ai
from .export_all import export_all_ai_data

__all__ = [
    'export_executive_ai',
    'export_regional_ai', 
    'export_store_detail_ai',
    'export_all_ai_data',
]
