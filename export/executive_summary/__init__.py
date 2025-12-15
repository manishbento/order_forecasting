"""
Executive Summary Export Package
================================
Creates high-level executive summary reports for stakeholder communication.

This package generates a professional Excel workbook with:
1. Regional Summary - Compact overview by region/date with key metrics
2. Waterfall Analysis - Breakdown of adjustments from baseline to final forecast
3. Weather Impact Summary - Weather severity and adjustment summary by region

Modules:
- queries: SQL queries for executive summary data
- formatting: Excel formatting definitions
- writers: Worksheet writing functions
- export: Main export functions
"""

from .export import (
    export_executive_summary,
    export_all_executive_summaries,
)

__all__ = [
    'export_executive_summary',
    'export_all_executive_summaries',
]
