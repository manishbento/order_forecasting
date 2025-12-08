# Configuration Files Directory

Place your `Configuration.xlsx` file here with the following worksheets:

## Required Worksheets

### config_active
Active items configuration by region.
| Column | Type | Description |
|--------|------|-------------|
| item_no | INT | Item number |
| item_desc | VARCHAR | Item description |
| region_code | VARCHAR | Region code (BA, LA, SD, NE, SE, TE) |
| active_date | DATE | Start date for item |
| active_end_date | DATE | End date for item |

### config_substitute
Item substitution mappings for discontinued items.
| Column | Type | Description |
|--------|------|-------------|
| item_no | INT | Original item number |
| region_code | VARCHAR | Region code |
| sub_item_no | INT | Replacement item number |
| sub_item_desc | VARCHAR | Replacement item description |
| effective_date | DATE | Substitution start date |
| effective_end_date | DATE | Substitution end date |

## Optional Worksheets
Additional configuration sheets can be added and will be loaded as DuckDB tables.
