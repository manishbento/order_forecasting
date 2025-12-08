"""
Microsoft Fabric Warehouse Connection Utility
=============================================
Provides connection to Microsoft Fabric Warehouse with token-based
authentication and DataFrame append capabilities.
"""

import os
import struct
import json
import time
from itertools import chain, repeat

import pyodbc
import pandas as pd
from azure.identity import ClientSecretCredential
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class FabricDatalakeWH:
    """
    Connect to Microsoft Fabric Warehouse with token-based authentication.
    
    This class handles:
    - Token acquisition and refresh
    - Connection management
    - DataFrame insertion
    
    Usage:
        wh = FabricDatalakeWH()
        wh.execute_query("SELECT 1")
        wh.append_df(df, 'table_name')
        wh.disconnect()
    """
    
    def __init__(self, client_id: str = None, client_secret: str = None,
                 tenant_id: str = None, sql_endpoint: str = None,
                 database: str = None, token_path: str = None):
        """
        Initialize Fabric Warehouse connection.
        
        Args:
            client_id: Azure AD application client ID
            client_secret: Azure AD application client secret
            tenant_id: Azure AD tenant ID
            sql_endpoint: Fabric SQL endpoint URL
            database: Database name
            token_path: Path to store token cache
        """
        # Use environment variables (no hardcoded defaults)
        self.client_id = client_id or os.environ.get('FABRIC_CLIENT_ID')
        self.client_secret = client_secret or os.environ.get('FABRIC_CLIENT_SECRET')
        self.tenant_id = tenant_id or os.environ.get('FABRIC_TENANT_ID')
        
        self.sql_endpoint = sql_endpoint or \
            'koj73uk36z4u5jlkoc2fsgwuqq-6rdov3bdqjjute3kn2jtfurh2q.datawarehouse.fabric.microsoft.com'
        self.database = database or 'BNTO_WH_300_GOLD'
        
        # Token management
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.token_path = token_path or os.path.join(base_dir, 'data_store', 'fabric_token.txt')
        
        # Ensure token directory exists
        os.makedirs(os.path.dirname(self.token_path), exist_ok=True)
        
        # Initialize credential
        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        self.resource_url = 'https://database.windows.net/.default'
        
        # Load or get token
        self.token_object, self.token_expiry = self._load_token()
        
        # Connection string
        self.connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={self.sql_endpoint},1433;"
            f"Database={self.database};"
            f"Encrypt=Yes;"
            f"TrustServerCertificate=No"
        )
        
        self.conn = None
    
    def _load_token(self):
        """Load token from cache or get new one."""
        try:
            with open(self.token_path, 'r') as file:
                token_data = json.load(file)
                token_expiry = token_data['expiry']
                if time.time() < token_expiry:
                    return token_data['token'], token_expiry
        except (FileNotFoundError, KeyError, json.JSONDecodeError):
            pass
        return self._get_new_token()
    
    def _get_new_token(self):
        """Get new token from Azure AD."""
        token_object = self.credential.get_token(self.resource_url)
        print(f'New token expires on {token_object.expires_on}')
        token_expiry = token_object.expires_on
        self._save_token(token_object.token, token_expiry)
        return token_object.token, token_expiry
    
    def _save_token(self, token, expiry):
        """Save token to cache file."""
        with open(self.token_path, 'w') as file:
            json.dump({'token': token, 'expiry': expiry}, file)
    
    def _get_token_bytes(self, force: bool = False):
        """Get token bytes for ODBC connection."""
        if (time.time() >= self.token_expiry) or force:
            self.token_object, self.token_expiry = self._get_new_token()
        else:
            print('Using existing token!')
        
        token_as_bytes = bytes(self.token_object, "UTF-8")
        encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
        token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
        return token_bytes
    
    def connect(self):
        """Establish connection to the database."""
        if self.conn is None:
            token_bytes = self._get_token_bytes(force=False)
            attrs_before = {1256: token_bytes}
            try:
                self.conn = pyodbc.connect(self.connection_string, attrs_before=attrs_before)
            except pyodbc.InterfaceError:
                token_bytes = self._get_token_bytes(force=True)
                attrs_before = {1256: token_bytes}
                self.conn = pyodbc.connect(self.connection_string, attrs_before=attrs_before)
    
    def disconnect(self):
        """Close the database connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            print("Fabric Warehouse connection closed.")
    
    def get_connection(self):
        """Get active connection, creating one if necessary."""
        self.connect()
        return self.conn
    
    def execute_query(self, query: str, params: tuple = None):
        """
        Execute a query (no results returned).
        
        Args:
            query: SQL query string
            params: Optional query parameters
        """
        self.connect()
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        self.conn.commit()
        cursor.close()
    
    def fetch_data(self, query: str, params: tuple = None):
        """
        Execute query and fetch results.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Tuple of (success, column_names, records)
        """
        self.connect()
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        records = cursor.fetchall()
        cursor.close()
        return True, columns, records
    
    def append_df(self, df: pd.DataFrame, table_name: str, 
                  if_exists: str = 'append'):
        """
        Insert DataFrame into Fabric Warehouse table.
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            if_exists: 'fail', 'replace', or 'append'
        """
        print(f"Inserting DataFrame into Warehouse table: {table_name}")
        
        try:
            engine = create_engine(
                "mssql+pyodbc://",
                creator=self.get_connection
            )
            
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=20
            )
            
            print(f"✅ Successfully inserted DataFrame into: {table_name}")
        except Exception as e:
            print(f"❌ Error inserting into Warehouse: {e}")
