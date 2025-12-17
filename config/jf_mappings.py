"""
JF Export Mappings Configuration
================================
Defines the mappings for exporting forecasts in JF format.

This module contains:
1. Store to JF# (Customer Number) mapping - defines which stores to export
   and their corresponding customer numbers
2. Item to UPC mapping - converts internal item_no to UPC codes

Only stores defined in JF_STORE_MAPPING will be included in the export.
"""

# =============================================================================
# STORE TO JF# (CUSTOMER NUMBER) MAPPING
# =============================================================================
# Maps store_no to jf# (customer number)
# Only stores listed here will be included in the JF export.
# Format: store_no: jf_customer_number
#
# Example:
#   455: 14852  means store 455 will be exported with customer number 14852
#
# Add or remove stores as needed to control which stores are exported.

JF_STORE_MAPPING = {
    # San Diego Region stores -> JF Customer Numbers
    # Store No: Customer Number (JF#)
    455: 14852,
    456: 14844,
    457: 14851,
    458: 14847,
    459: 14842,
    460: 14845,
    461: 14840,
    462: 14846,
    463: 14839,
    464: 14837,
    465: 14848,
    466: 14841,
    467: 14849,
    468: 14843,
    469: 14853,
    470: 14838,
    471: 14850,
    472: 14836,
}


# =============================================================================
# ITEM TO UPC MAPPING
# =============================================================================
# Maps internal item_no to UPC codes for export
# Format: item_no: upc_code
#
# UPC codes are typically 12-13 digit numbers used for product identification

JF_ITEM_UPC_MAPPING = {
    # Item No: UPC Code
    # Example items - update with actual mappings
    1896526: '639123300652',   # California Combo Avocado
    1896534: '639123300737',   # Example item 2
    1896542: '639123301086',   # Example item 3
    # Add more item -> UPC mappings as needed
}


# =============================================================================
# EXPORT CONFIGURATION
# =============================================================================
# Region code for JF exports (only this region will be processed)
JF_EXPORT_REGION = 'SD'

# Unit of measure for all exports
JF_UNIT_OF_MEASURE = 'EA'

# Date format for the export file name (YYYY-MM-DD)
JF_FILENAME_DATE_FORMAT = '%Y-%m-%d'

# Date format for the date fields in the CSV (MM/DD/YYYY)
JF_CSV_DATE_FORMAT = '%m/%d/%Y'


def get_jf_customer_number(store_no: int) -> int | None:
    """
    Get the JF customer number for a store.
    
    Args:
        store_no: Internal store number
        
    Returns:
        JF customer number if store is mapped, None otherwise
    """
    return JF_STORE_MAPPING.get(store_no)


def get_upc_code(item_no: int) -> str | None:
    """
    Get the UPC code for an item.
    
    Args:
        item_no: Internal item number
        
    Returns:
        UPC code string if item is mapped, None otherwise
    """
    upc = JF_ITEM_UPC_MAPPING.get(item_no)
    return str(upc) if upc is not None else None


def get_mapped_stores() -> list[int]:
    """
    Get list of all stores that have JF mappings.
    
    Returns:
        List of store numbers that will be exported
    """
    return list(JF_STORE_MAPPING.keys())


def get_mapped_items() -> list[int]:
    """
    Get list of all items that have UPC mappings.
    
    Returns:
        List of item numbers that have UPC mappings
    """
    return list(JF_ITEM_UPC_MAPPING.keys())
