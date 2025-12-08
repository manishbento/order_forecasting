"""
Microsoft Fabric Lakehouse Connection Utility
=============================================
Provides connection to Microsoft Fabric Lakehouse SQL Endpoint
using Service Principal authentication.

This class handles automatic token acquisition and refresh through
the ODBC driver.
"""

import os
import pyodbc
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class FabricDatalake:
    """
    Connect to and query a Microsoft Fabric Lakehouse SQL Endpoint.
    
    Uses Service Principal authentication with automatic token handling
    by the ODBC driver.
    
    Usage:
        fabric = FabricDatalake()
        conn = fabric.get_connection()
        # Use connection for queries
        fabric.disconnect()
    """
    
    def __init__(self, client_id: str = None, client_secret: str = None,
                 tenant_id: str = None, sql_endpoint: str = None, 
                 database: str = None):
        """
        Initialize Fabric Lakehouse connection.
        
        Args:
            client_id: Azure AD application client ID
            client_secret: Azure AD application client secret
            tenant_id: Azure AD tenant ID
            sql_endpoint: Fabric SQL endpoint URL
            database: Database name
        """
        # Use environment variables (no hardcoded defaults)
        self.client_id = client_id or os.environ.get('FABRIC_CLIENT_ID')
        self.client_secret = client_secret or os.environ.get('FABRIC_CLIENT_SECRET')
        self.tenant_id = tenant_id or os.environ.get('FABRIC_TENANT_ID')
        
        self.sql_endpoint = sql_endpoint or \
            'koj73uk36z4u5jlkoc2fsgwuqq-6rdov3bdqjjute3kn2jtfurh2q.datawarehouse.fabric.microsoft.com'
        self.database = database or 'BNTO_LH_300_GOLD'
        
        # Build connection string
        self.connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.sql_endpoint},1433;"
            f"Database={self.database};"
            f"UID={self.client_id};"
            f"PWD={self.client_secret};"
            f"Authentication=ActiveDirectoryServicePrincipal;"
            f"Encrypt=Yes;"
            f"TrustServerCertificate=No;"
            f"Connection Timeout=30;"
        )
        
        self.conn = None
    
    def connect(self):
        """Establish connection to the database."""
        if self.conn is None:
            try:
                self.conn = pyodbc.connect(self.connection_string, autocommit=True)
                print("Connection to Fabric Lakehouse established successfully.")
            except pyodbc.Error as ex:
                print(f"Error connecting to database: {ex}")
                raise
    
    def disconnect(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("Fabric Lakehouse connection closed.")
    
    def get_connection(self):
        """Get active connection, creating one if necessary."""
        if self.conn is None:
            self.connect()
        return self.conn
    
    def fetch_data(self, query: str, params: tuple = None):
        """
        Execute SQL query and fetch all results.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Tuple of (success, column_names, records)
        """
        self.get_connection()
        
        try:
            with self.conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                columns = [column[0] for column in cursor.description]
                records = cursor.fetchall()
                records = [tuple(row) for row in records]
                return True, columns, records
        except pyodbc.Error as ex:
            print(f"Error executing query: {ex}")
            return False, [], []
