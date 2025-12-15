"""
Export All AI Data
===================
Convenience function to export all AI analysis data at once.
"""

import os
from typing import List

from config import settings
from .executive import export_executive_ai
from .regional import export_regional_ai
from .store_detail import export_store_detail_ai


def export_all_ai_data(conn, regions: List[str],
                       start_date: str, end_date: str,
                       output_dir: str = None,
                       stores_per_region: int = 5) -> dict:
    """
    Export all AI analysis data (executive, regional, store detail).
    
    This is the main entry point for generating all AI-consumable exports.
    
    Args:
        conn: DuckDB connection
        regions: List of region codes
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Base output directory
        stores_per_region: Number of store detail files per region
        
    Returns:
        Dictionary with paths to all exported files
    """
    if output_dir is None:
        output_dir = os.path.join(settings.OUTPUT_DIR, 'ai_analysis')
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n" + "="*60)
    print("EXPORTING AI ANALYSIS DATA")
    print("="*60)
    
    result = {
        "executive": None,
        "regional": [],
        "store_detail": []
    }
    
    # Executive summary (all regions combined)
    print("\n1. Executive Summary (all regions)...")
    result["executive"] = export_executive_ai(
        conn, regions, start_date, end_date, output_dir
    )
    
    # Regional analysis (one per region)
    print("\n2. Regional Analysis...")
    result["regional"] = export_regional_ai(
        conn, regions, start_date, end_date, output_dir
    )
    
    # Store detail (top N stores per region)
    print("\n3. Store Detail Analysis...")
    store_detail_dir = os.path.join(output_dir, 'store_detail')
    result["store_detail"] = export_store_detail_ai(
        conn, regions, start_date, end_date,
        output_dir=store_detail_dir,
        stores_per_region=stores_per_region,
        include_store_index=True
    )
    
    # Print summary
    print("\n" + "-"*60)
    print("AI Export Summary:")
    print("  Executive file: 1")
    print(f"  Regional files: {len(result['regional'])}")
    print(f"  Store files: {len(result['store_detail'])}")
    print(f"  Output directory: {output_dir}")
    print("="*60 + "\n")
    
    return result
