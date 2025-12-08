"""
Data Loader Module
==================
Handles loading data from Fabric Datalake and local sources into DuckDB.

This module is responsible for:
- Connecting to Microsoft Fabric Datalake
- Loading historical shrink/sales data
- Loading configuration files (Excel)
- Creating local DuckDB tables for fast querying
"""

import os
import duckdb
import polars as pl
import pandas as pd

from config import settings
from utils.fabric_lakehouse import FabricDatalake


class DataLoader:
    """
    Handles all data loading operations for the forecasting system.
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize the DataLoader.
        
        Args:
            db_path: Path to DuckDB database. Uses settings default if None.
        """
        self.db_path = db_path or settings.SHRINK_DB_PATH
        self.conn = None
        
    def connect(self):
        """Establish connection to DuckDB."""
        if self.conn is None:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.conn = duckdb.connect(self.db_path)
            print(f"Connected to DuckDB at {self.db_path}")
        return self.conn
    
    def disconnect(self):
        """Close DuckDB connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            print("DuckDB connection closed.")
    
    def get_connection(self):
        """Get active connection, connecting if necessary."""
        if self.conn is None:
            self.connect()
        return self.conn
    
    def load_shrink_data(self, start_date: str = None, end_date: str = None):
        """
        Load shrink/sales data from Fabric Datalake.
        
        Args:
            start_date: Start date for data range (YYYY-MM-DD)
            end_date: End date for data range (YYYY-MM-DD)
        """
        start_date = start_date or settings.START_DATE
        end_date = end_date or settings.END_DATE
        
        print(f"Loading shrink data from Fabric Datalake... Range: {start_date} to {end_date}")
        
        # Connect to Fabric
        fabric = FabricDatalake()
        fabric_conn = fabric.get_connection()
        
        # Load data using Polars for efficiency
        query = f"""
            SELECT * 
            FROM BNTO_LH_300_GOLD.dbo.rpt_costco_us_shrink rpt 
            WHERE rpt.date_posting >= '{start_date}' 
            AND rpt.date_posting <= '{end_date}'
        """
        
        df = pl.read_database(query=query, connection=fabric_conn)
        
        # Convert to Arrow for DuckDB
        arrow_table = df.to_arrow()
        
        # Get DuckDB connection
        conn = self.get_connection()
        
        # Create table from Arrow
        conn.sql("DROP TABLE IF EXISTS shrink")
        conn.sql("CREATE TABLE shrink AS SELECT * FROM arrow_table")
        
        row_count = conn.sql("SELECT COUNT(*) FROM shrink").fetchone()[0]
        print(f"Loaded {row_count} rows into 'shrink' table.")
        
        # Disconnect from Fabric
        fabric.disconnect()
        
        return row_count
    
    def load_configuration(self, config_path: str = None):
        """
        Load configuration from Excel file into DuckDB tables.
        
        Each worksheet becomes a separate table with the sheet name as table name.
        
        Args:
            config_path: Path to Configuration.xlsx file
        """
        config_path = config_path or settings.CONFIG_FILE_PATH
        
        if not os.path.exists(config_path):
            print(f"Warning: Configuration file not found at {config_path}")
            return
        
        print(f"Loading configuration from {config_path}...")
        
        conn = self.get_connection()
        
        # Read Excel file
        xls = pd.ExcelFile(config_path)
        
        for sheet_name in xls.sheet_names:
            print(f"  Loading sheet: {sheet_name}")
            df = pd.read_excel(xls, sheet_name=sheet_name)
            
            # Clean sheet name for table name (remove spaces, special chars)
            table_name = sheet_name.replace(' ', '_').replace('-', '_')
            
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        print("Configuration loaded successfully.")
    
    def load_store_data(self):
        """
        Load store master data from Fabric Datalake.
        
        Returns store information needed for weather data lookups.
        """
        print("Loading store data from Fabric Datalake...")
        
        fabric = FabricDatalake()
        fabric_conn = fabric.get_connection()
        
        query = """
            SELECT
                code,
                unique_key,
                name,
                address,
                city,
                postal_code,
                region,
                store_no
            FROM
                BNTO_LH_300_GOLD.dbo.dim_customers
            WHERE
                entity_code = 'BNI'
                AND dim_banner = 'COSTCO'
        """
        
        df = pl.read_database(query=query, connection=fabric_conn)
        arrow_table = df.to_arrow()
        
        conn = self.get_connection()
        conn.sql("DROP TABLE IF EXISTS stores")
        conn.sql("CREATE TABLE stores AS SELECT * FROM arrow_table")
        
        row_count = conn.sql("SELECT COUNT(*) FROM stores").fetchone()[0]
        print(f"Loaded {row_count} stores.")
        
        fabric.disconnect()
        
        return row_count
    
    def get_stores_df(self) -> pd.DataFrame:
        """
        Get stores data as a pandas DataFrame.
        
        Returns:
            DataFrame with store information
        """
        conn = self.get_connection()
        try:
            return conn.sql("SELECT postal_code, store_no FROM stores").df()
        except Exception as e:
            print(f"Error fetching stores: {e}")
            return pd.DataFrame()
    
    def load_weather_data(self) -> pd.DataFrame:
        """
        Load weather data from OpenWeatherMap database.
        
        Returns:
            DataFrame with weather information or None if database doesn't exist
        """
        weather_db_path = os.path.join(settings.DATA_STORE_DIR, "openweathermap.db")
        
        if not os.path.exists(weather_db_path):
            print(f"Warning: openweathermap.db not found at {weather_db_path}")
            print("Weather features will be unavailable.")
            return None
        
        try:
            weather_conn = duckdb.connect(weather_db_path, read_only=True)
            
            weather_df = weather_conn.execute("""
                SELECT 
                    store_no,
                    date,
                    severity_score,
                    total_rain_expected,
                    total_snow_expected,
                    temp_max,
                    temp_min,
                    humidity,
                    wind_speed,
                    pop_probability,
                    alert_tags,
                    day_condition,
                    day_low_rain,
                    day_medium_rain,
                    day_high_rain
                FROM weather
                ORDER BY store_no, date
            """).df()
            
            weather_conn.close()
            print(f"Loaded {len(weather_df)} weather records from OpenWeatherMap")
            return weather_df
            
        except Exception as e:
            print(f"Error loading weather data: {e}")
            return None
    
    def execute_query(self, query: str):
        """
        Execute a query and return results.
        
        Args:
            query: SQL query string
            
        Returns:
            Query results
        """
        conn = self.get_connection()
        return conn.execute(query).fetchall()
    
    def fetch_df(self, query: str) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            pandas DataFrame with results
        """
        conn = self.get_connection()
        return conn.sql(query).df()


def create_data_loader(db_path: str = None) -> DataLoader:
    """
    Factory function to create a DataLoader instance.
    
    Args:
        db_path: Optional path to DuckDB database
        
    Returns:
        Configured DataLoader instance
    """
    return DataLoader(db_path)
