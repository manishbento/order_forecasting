"""
Database Migrations Utility
===========================
Utility functions for managing database schema updates.

Run this module to apply pending schema changes to the forecast_results table.
Make sure no other processes have a lock on the database before running.
"""

import duckdb
from config import settings


def get_current_columns(conn, table_name: str = 'forecast_results') -> set:
    """Get current column names for a table."""
    cols = conn.sql(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
    """).fetchall()
    return set(c[0] for c in cols)


def add_missing_weather_columns(db_path: str = None, dry_run: bool = False):
    """
    Add missing weather-related columns to forecast_results table.
    
    Args:
        db_path: Path to DuckDB database file
        dry_run: If True, only print what would be done without making changes
        
    Returns:
        List of columns that were added (or would be added if dry_run)
    """
    db_path = db_path or settings.SHRINK_DB_PATH
    
    # Define all weather columns that should exist
    weather_columns = [
        # Core weather data
        ('weather_day_condition', 'VARCHAR'),
        ('weather_day_low_rain', 'REAL'),
        ('weather_day_medium_rain', 'REAL'),
        ('weather_day_high_rain', 'REAL'),
        ('weather_total_rain_expected', 'REAL'),
        ('weather_latitude', 'REAL'),
        ('weather_longitude', 'REAL'),
        ('weather_resolved_address', 'VARCHAR'),
        ('weather_timezone', 'VARCHAR'),
        
        # Severity metrics
        ('weather_severity_score', 'REAL'),
        ('weather_severity_category', 'VARCHAR'),
        ('weather_sales_impact_factor', 'REAL'),
        ('weather_rain_severity', 'REAL'),
        ('weather_snow_severity', 'REAL'),
        ('weather_wind_severity', 'REAL'),
        ('weather_visibility_severity', 'REAL'),
        ('weather_temp_severity', 'REAL'),
        
        # Weather variables
        ('weather_snow_amount', 'REAL'),
        ('weather_snow_depth', 'REAL'),
        ('weather_wind_speed', 'REAL'),
        ('weather_wind_gust', 'REAL'),
        ('weather_temp_max', 'REAL'),
        ('weather_temp_min', 'REAL'),
        ('weather_visibility', 'REAL'),
        ('weather_severe_risk', 'REAL'),
        ('weather_precip_probability', 'REAL'),
        ('weather_precip_cover', 'REAL'),
        ('weather_humidity', 'REAL'),
        ('weather_cloud_cover', 'REAL'),
        
        # Adjustment tracking
        ('weather_adjustment_qty', 'REAL'),
        ('weather_adjustment_reason', 'VARCHAR'),
        ('weather_adjusted', 'INTEGER'),
        ('weather_status_indicator', 'VARCHAR'),
        ('forecast_qty_pre_weather', 'REAL'),
    ]
    
    added_columns = []
    
    try:
        if dry_run:
            conn = duckdb.connect(db_path, read_only=True)
        else:
            conn = duckdb.connect(db_path)
        
        current_cols = get_current_columns(conn)
        
        for col_name, col_type in weather_columns:
            if col_name not in current_cols:
                if dry_run:
                    print(f"[DRY RUN] Would add column: {col_name} ({col_type})")
                    added_columns.append(col_name)
                else:
                    try:
                        conn.execute(f'ALTER TABLE forecast_results ADD COLUMN {col_name} {col_type}')
                        print(f"Added column: {col_name} ({col_type})")
                        added_columns.append(col_name)
                    except Exception as e:
                        if 'already exists' not in str(e).lower():
                            print(f"Error adding {col_name}: {e}")
        
        conn.close()
        
        if added_columns:
            print(f"\n{'Would add' if dry_run else 'Added'} {len(added_columns)} columns")
        else:
            print("\nNo columns need to be added - schema is up to date")
            
        return added_columns
        
    except Exception as e:
        print(f"Error: {e}")
        if "lock" in str(e).lower():
            print("\nDatabase is locked by another process.")
            print("Please close any notebooks or scripts using the database and try again.")
        return []


def verify_schema(db_path: str = None):
    """
    Verify that all required weather columns exist in forecast_results.
    
    Args:
        db_path: Path to DuckDB database file
        
    Returns:
        Tuple of (missing_columns, existing_columns)
    """
    db_path = db_path or settings.SHRINK_DB_PATH
    
    required_weather_cols = {
        'weather_snow_depth',
        'weather_precip_probability', 
        'weather_precip_cover',
        'weather_humidity',
        'weather_cloud_cover',
        'weather_severity_score',
        'weather_severity_category',
        'weather_sales_impact_factor',
        'weather_rain_severity',
        'weather_snow_severity',
        'weather_wind_severity',
        'weather_visibility_severity',
        'weather_temp_severity',
    }
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        current_cols = get_current_columns(conn)
        conn.close()
        
        weather_cols = {c for c in current_cols if c.startswith('weather_')}
        missing = required_weather_cols - weather_cols
        
        print(f"Current weather columns: {len(weather_cols)}")
        print(f"Required weather columns: {len(required_weather_cols)}")
        print(f"Missing weather columns: {len(missing)}")
        
        if missing:
            print("\nMissing columns:")
            for col in sorted(missing):
                print(f"  - {col}")
        else:
            print("\n✅ All required weather columns are present!")
        
        return missing, weather_cols
        
    except Exception as e:
        print(f"Error: {e}")
        return set(), set()


if __name__ == '__main__':
    import sys
    
    print("=" * 60)
    print("Database Schema Migration Utility")
    print("=" * 60)
    print()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--dry-run':
        print("DRY RUN MODE - No changes will be made\n")
        add_missing_weather_columns(dry_run=True)
    elif len(sys.argv) > 1 and sys.argv[1] == '--verify':
        verify_schema()
    else:
        print("This will modify the database schema.")
        print("Run with --dry-run to preview changes")
        print("Run with --verify to check current schema")
        print()
        response = input("Proceed with migration? (yes/no): ")
        if response.lower() == 'yes':
            add_missing_weather_columns()
        else:
            print("Migration cancelled")
