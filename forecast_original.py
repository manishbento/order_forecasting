import os
# Add the root directory to sys.path
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import sys
import os
import polars as pl
import duckdb
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd
import math
from openpyxl import Workbook, load_workbook
from pynotebooks.utils.xl.writer import XLWriter
from decimal import Decimal
from copy import deepcopy

# Initialize FabricLakehouse Connecton
from pynotebooks.utils.lh2 import FabricDatalake
from pynotebooks.utils.wh import FabricDatalakeWH
conn_dd = duckdb.connect("costco_order_forecasting/shrink_data.db")

import concurrent.futures

# Append to Fabric Warehouse
wh = FabricDatalakeWH()

# FORECAST DATE RANGE
FORECAST_START_DATE_V = datetime(2025, 11, 10)
FORECAST_END_DATE_V = datetime(2025, 11, 12)
FORECAST_START_DATE = FORECAST_START_DATE_V.strftime('%Y-%m-%d')
FORECAST_END_DATE = FORECAST_END_DATE_V.strftime('%Y-%m-%d')

# Date Values
END_DATE_V = FORECAST_END_DATE_V
START_DATE_V = FORECAST_START_DATE_V - timedelta(days=45)

# Date as strings for SQL queries
START_DATE = START_DATE_V.strftime('%Y-%m-%d')
END_DATE = END_DATE_V.strftime('%Y-%m-%d')

# EXCEPTIONAL DAYS TO BE EXCLUDED
EXCEPTIONAL_DAYS = [
    '2025-07-01',  # Independence Day Holiday
    '2025-07-02',  # Independence Day Holiday
    '2025-07-03',  # Independence Day Holiday
    '2025-07-04',  # Independence Day Holiday
    '2025-09-01',  # Labor Day Holiday
    '2025-09-02',  # Next Day of Holiday
    '2025-09-24',  # Not Available Yet
]

NON_HERO_ITEMS: list[int] = [
    1942690, 
    1940912,
    1816554, # Maki Trio,
    1713314 # Salmon Combo
]

INACTIVE_STORES = [
    129 # Santa Clara
]

# Define a 'k' factor based on the Optimal Service Level.
# A ~60% service level corresponds to a k-factor of ~0.25.
# This represents the financial balance between waste and stockouts.
LOAD_DATA = True

param_sets = [
    {
        'BASE_COVER': 0.05, # Base Coverage on top of the Average Sales Forecast
        'BASE_COVER_SOLD_OUT': 0.06, # Base Coverage on top of the Average Sales Forecast if the last week was sold out
        'K_FACTOR': 0.25, # Service Level Factor for Safety Stock Calculation
        'CASE_SIZE': 6, # Case Pack Size for Rounding
        'WEEK_WEIGHTS': (0.6, 0.2, 0.1, 0.1), # Weights for the 4 weeks of sales data (W1, W2, W3, W4)
        'HIGH_SHRINK_THRESHOLD': 0.15, # Threshold for High Shrink Adjustment
        'ROUND_DOWN_SHRINK_THRESHOLD': 0.00 # Threshold for Rounding Down Adjustment
    }
]

# Configuration of Behaviour
EXPORT_JSON = True
PUSH_FABRIC = False
GENERATE_FILE_OUTPUTS = True
SCENARIO_TESTING = False
FOOD_COST_COMPONENT = 0.31

# Parameter Testing
if SCENARIO_TESTING:
    # create sets of scenarios to test on
    base_covers = [0.05]
    base_cover_shrink = [0.06]
    k_factors = [0.25]
    case_sizes = [4, 6]
    # different weights for different weeks
    week_weights = [
        (0.6, 0.2, 0.1, 0.1),
    ]
    high_shrink_thresholds = [0.15]
    round_down_shrink_thresholds = [0.00]

    # Light Scenarios
    # base_covers = [0.07, 0.09]
    base_cover_shrink = [0.09]
    k_factors = [0.25]
    # case_sizes = [6]
    # week_weights = [(0.4, 0.3, 0.2, 0.1)]
    high_shrink_thresholds = [0.20]
    round_down_shrink_thresholds = [0.15]

    # Create all combinations of parameters
    from itertools import product
    param_sets = []
    for base_cover, base_cover_shrink, k_factor, case_size, week_weight, high_shrink_threshold, round_down_shrink_threshold in product(base_covers, base_cover_shrink, k_factors, case_sizes, week_weights, high_shrink_thresholds, round_down_shrink_thresholds):
        param_sets.append({
            'BASE_COVER': base_cover,
            'BASE_COVER_SOLD_OUT': base_cover_shrink,
            'K_FACTOR': k_factor,
            'CASE_SIZE': case_size,
            'WEEK_WEIGHTS': week_weight,
            'HIGH_SHRINK_THRESHOLD': high_shrink_threshold,
            'ROUND_DOWN_SHRINK_THRESHOLD': round_down_shrink_threshold
        })
    

def create_forecast_results_table(con, force=True):
    if force:
        con.query('''
        DROP TABLE IF EXISTS forecast_results;
        ''')

    """
    Creates the forecast_results table in the DuckDB instance if it doesn't already exist.
    This function defines the schema for the final output table.

    Args:
        con: A DuckDB connection object.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS forecast_results (
        -- Identifiers from the base query
        item_no BIGINT,
        item_desc VARCHAR,
        store_no BIGINT,
        region_code VARCHAR,
        date_forecast DATE,
        date_base_w1 DATE,
        date_base_w2 DATE,
        date_base_w3 DATE,
        date_base_w4 DATE,

        -- Parameters (save to database too)
        base_cover DOUBLE,
        base_cover_sold_out DOUBLE,
        base_cover_applied DOUBLE,
        k_factor DOUBLE,

        -- Base data from the query
        case_pack_size INTEGER,
        w1_cost_unit DOUBLE,
        w1_price_unit DOUBLE,
        avg_four_week_sales DOUBLE,
        w1_shipped BIGINT,
        w2_shipped BIGINT,
        w3_shipped BIGINT,
        w4_shipped BIGINT,
        w1_sold BIGINT,
        w2_sold BIGINT,
        w3_sold BIGINT,
        w4_sold BIGINT,
        w1_shrink_p DOUBLE,
        w2_shrink_p DOUBLE,
        w3_shrink_p DOUBLE,
        w4_shrink_p DOUBLE,
        store_w1_received BIGINT,
        store_w1_sold BIGINT,
        store_w1_shrink BIGINT,
        store_w1_shrink_p DOUBLE,
        store_w2_received BIGINT,
        store_w2_sold BIGINT,
        store_w2_shrink BIGINT,
        store_w2_shrink_p DOUBLE,
        store_w3_received BIGINT,
        store_w3_sold BIGINT,
        store_w3_shrink BIGINT,
        store_w3_shrink_p DOUBLE,
        store_w4_received BIGINT,
        store_w4_sold BIGINT,
        store_w4_shrink BIGINT,
        store_w4_shrink_p DOUBLE,

        -- add result where available
        result_shipped BIGINT,
        result_sold BIGINT,
        result_shrink_p DOUBLE,
        result_store_received BIGINT,
        result_store_sold BIGINT,
        result_store_shrink BIGINT,
        result_store_shrink_p DOUBLE,
        result_price_unit DOUBLE,

        -- Newly calculated fields
        sales_velocity DOUBLE,
        sales_volatility DOUBLE,
        average_sold DOUBLE,
        ema DOUBLE,
        forecast_average DOUBLE,
        forecast_average_w_cover DOUBLE,
        round_up_quantity DOUBLE,
        round_up_final DOUBLE,
        forecast_quantity DOUBLE,
        delta_from_last_week DOUBLE,
        impact_of_rounding DOUBLE,
        forecast_safety_stock DOUBLE,
        forecast_safety_stock_applied BIGINT,

        -- Forecasting Fields
        forecast_shrink_last_week_sales DOUBLE,
        forecast_shrink_average DOUBLE,

        -- Parameters
        forecast_w1_weight DOUBLE,
        forecast_w2_weight DOUBLE,
        forecast_w3_weight DOUBLE,
        forecast_w4_weight DOUBLE,
        forecast_high_shrink_threshold DOUBLE,
        forecast_round_down_shrink_threshold DOUBLE,

        -- AI Forecasting Fields
        ai_forecast DOUBLE,
        ai_difference DOUBLE,
        ai_reasoning VARCHAR,

        -- Compute the Results
        result_forecast_case_pack_size BIGINT,
        result_forecast_shrink BIGINT,
        result_forecast_shrink_p DOUBLE,
        result_forecast_sold_out INTEGER,
        result_sales_amount DOUBLE,
        result_forecast_shrink_cost DOUBLE,
        result_forecast_lost_sales DOUBLE,
        result_forecast_margin_amount DOUBLE
    );
    """  # END:
    try:
        conn_dd.execute(create_table_query)
        print("Table 'forecast_results' created or already exists.")
    except Exception as e:
        print(f"An error occurred during table creation: {e}")

def create_fabric_table(force=False):

    # Drop table if forced
    if force:
        wh.execute_query("DROP TABLE IF EXISTS costco_us_forecast;")
        print("Dropped existing table 'costco_us_forecast' in Fabric Warehouse.")

    # create the table in Fabric Warehouse
    query = """
        IF NOT EXISTS (SELECT * FROM BNTO_WH_300_GOLD.sys.tables WHERE name = 'costco_us_forecast' AND type = 'U')
        BEGIN
            CREATE TABLE costco_us_forecast (
                item_no BIGINT,
                item_desc VARCHAR(1000),
                store_no BIGINT,
                region_code VARCHAR(50),
                base_cover FLOAT,
                base_cover_sold_out FLOAT,
                base_cover_applied FLOAT,
                k_factor FLOAT,
                date_forecast DATE,
                date_base_w1 DATE,
                date_base_w2 DATE,
                date_base_w3 DATE,
                date_base_w4 DATE,
                case_pack_size INT,
                w1_cost_unit FLOAT,
                w1_price_unit FLOAT,
                avg_four_week_sales FLOAT,
                w1_shipped BIGINT,
                w2_shipped BIGINT,
                w3_shipped BIGINT,
                w4_shipped BIGINT,
                w1_sold BIGINT,
                w2_sold BIGINT,
                w3_sold BIGINT,
                w4_sold BIGINT,
                w1_shrink_p FLOAT,
                w2_shrink_p FLOAT,
                w3_shrink_p FLOAT,
                w4_shrink_p FLOAT,
                result_price_unit FLOAT,
                result_shipped BIGINT,
                result_sold BIGINT,
                result_shrink_p FLOAT,
                result_store_received BIGINT,
                result_store_sold BIGINT,
                result_store_shrink BIGINT,
                result_store_shrink_p FLOAT,
                store_w1_received BIGINT,
                store_w1_sold BIGINT,
                store_w1_shrink BIGINT,
                store_w1_shrink_p FLOAT,
                store_w2_received BIGINT,
                store_w2_sold BIGINT,
                store_w2_shrink BIGINT,
                store_w2_shrink_p FLOAT,
                store_w3_received BIGINT,
                store_w3_sold BIGINT,
                store_w3_shrink BIGINT,
                store_w3_shrink_p FLOAT,
                store_w4_received BIGINT,
                store_w4_sold BIGINT,
                store_w4_shrink BIGINT,
                store_w4_shrink_p FLOAT,
                sales_velocity FLOAT,
                sales_volatility FLOAT,
                average_sold FLOAT,
                ema FLOAT,
                forecast_average FLOAT,
                forecast_average_w_cover FLOAT,
                round_up_quantity FLOAT,
                round_up_final FLOAT,
                forecast_quantity FLOAT,
                delta_from_last_week FLOAT,
                impact_of_rounding FLOAT,
                forecast_safety_stock FLOAT,
                forecast_safety_stock_applied BIGINT,
                forecast_shrink_last_week_sales FLOAT,
                forecast_shrink_average FLOAT,
                ai_forecast FLOAT,
                ai_difference FLOAT,
                ai_reasoning VARCHAR(MAX),
                result_forecast_shrink BIGINT,
                result_forecast_shrink_p FLOAT,
                result_forecast_sold_out INT,
                result_sales_amount FLOAT,
                result_forecast_shrink_cost FLOAT,
                result_forecast_lost_sales FLOAT,
                result_forecast_margin_amount FLOAT,
                forecast_w1_weight FLOAT,
                forecast_w2_weight FLOAT,
                forecast_w3_weight FLOAT,
                forecast_w4_weight FLOAT,
                forecast_high_shrink_threshold FLOAT,
                forecast_round_down_shrink_threshold FLOAT,
                result_forecast_case_pack_size BIGINT
            );
        END
    """
    wh.execute_query(query)
    print("Table 'costco_us_forecast' created in Fabric Warehouse.")

create_forecast_results_table(conn_dd, force=True)
# create_fabric_table(force=True)

def load_data():
    print("Loading data from Fabric Datalake... Range:", START_DATE, "to", END_DATE)
    conn = FabricDatalake().get_connection()
    df = pl.read_database(
        query=f"SELECT * FROM BNTO_LH_300_GOLD.dbo.rpt_costco_us_shrink rpt WHERE rpt.date_posting >= '{START_DATE}' AND rpt.date_posting <= '{END_DATE}'",
        connection=conn
    )
    my_arrow = df.to_arrow()
    conn_dd.sql("DROP TABLE IF EXISTS shrink")
    conn_dd.sql("CREATE TABLE shrink AS SELECT * FROM my_arrow")

if LOAD_DATA:
    load_data()

# read excel file called Configuration.xlsx and read the data from all the worksheets into new tables in the duckdb database. Use the sheet names as the table names and all fields
file_path = 'costco_order_forecasting/Configuration.xlsx'
def read_excel_to_duckdb(file_path):
    import pandas as pd
    xls = pd.ExcelFile(file_path)
    for sheet_name in xls.sheet_names:
        print(f"Reading sheet: {sheet_name}")
        df = pd.read_excel(xls, sheet_name=sheet_name)
        conn_dd.execute(f"DROP TABLE IF EXISTS {sheet_name}")
        conn_dd.execute(f"CREATE TABLE {sheet_name} AS SELECT * FROM df")

read_excel_to_duckdb(file_path)

query_forecast_base = '''
    WITH params AS (
        SELECT
            '{region}' AS region_code,
            CAST('{forecast_date}' AS DATE) AS date_forecast,
            CAST('{w1}' AS DATE) AS date_w1,
            CAST('{w2}' AS DATE) AS date_w2,
            CAST('{w3}' AS DATE) AS date_w3,
            CAST('{w4}' AS DATE) AS date_w4
    ),
    -- Create a clean sales history, replacing old SKUs with new ones from the substitute table
    clean_shrink AS (
        SELECT
            COALESCE(CAST(sub.sub_item_no AS BIGINT), s.item_no) AS item_no,
            COALESCE(CAST(sub.sub_item_desc AS VARCHAR), s.item_desc) AS item_desc,
            s.* EXCLUDE (item_no, item_desc) -- Exclude original item_no to prevent ambiguity
        FROM main.shrink s
        CROSS JOIN params p
        LEFT JOIN main.config_substitute sub ON s.item_no = sub.item_no AND s.region_code = sub.region_code AND sub.effective_date <= p.date_forecast AND sub.effective_end_date > p.date_forecast
    ),
    -- Create the set of all item/store combinations to forecast for
    item_store_universe AS (
        SELECT DISTINCT
            ca.item_no,
            ca.item_desc,
            s.store_no
            -- Configuration is now handled in the final SELECT
        FROM config_active ca
        JOIN clean_shrink s ON ca.item_no = s.item_no
        CROSS JOIN params p
        WHERE ca.region_code = p.region_code
        AND s.region_code = p.region_code
        AND ca.active_date <= p.date_forecast
        AND ca.active_end_date >= p.date_forecast
    ),
    -- Calculate store-level aggregates for each of the 4 weeks using the clean data
    store_weekly_aggs AS (
        SELECT
            s.store_no,
            -- W1, W2, W3, W4 Store Aggregates...
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_received END) AS result_store_received,
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_sold END) AS result_store_sold,
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_shrink END) AS result_store_shrink,

            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_received END) AS store_w1_received,
            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_sold END) AS store_w1_sold,
            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_shrink END) AS store_w1_shrink,
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_received END) AS store_w2_received,
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_sold END) AS store_w2_sold,
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_shrink END) AS store_w2_shrink,
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_received END) AS store_w3_received,
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_sold END) AS store_w3_sold,
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_shrink END) AS store_w3_shrink,
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_received END) AS store_w4_received,
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_sold END) AS store_w4_sold,
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_shrink END) AS store_w4_shrink,
            -- Store-level shrink percentages...
            SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_shrink END) / NULLIF(SUM(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_received END), 0) AS result_store_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_shrink END) / NULLIF(SUM(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_received END), 0) AS store_w1_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_shrink END) / NULLIF(SUM(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_received END), 0) AS store_w2_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_shrink END) / NULLIF(SUM(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_received END), 0) AS store_w3_shrink_p,
            SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_shrink END) / NULLIF(SUM(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_received END), 0) AS store_w4_shrink_p
        FROM
            clean_shrink s
        CROSS JOIN params p
        WHERE
            s.date_posting IN (p.date_w1, p.date_w2, p.date_w3, p.date_w4, p.date_forecast)
            AND s.region_code = p.region_code
        GROUP BY
            s.store_no
    )
    -- Final SELECT statement to join all data sources
    SELECT
        -- Identifiers
        u.item_no, u.item_desc, u.store_no,
        p.region_code, p.date_forecast, p.date_w1 AS date_base_w1, p.date_w2 AS date_base_w2, p.date_w3 AS date_base_w3, p.date_w4 AS date_base_w4,

        -- Dynamic Case Pack Size Calculation
        CASE WHEN u.item_desc LIKE '%PLATTER%' THEN 3 ELSE 6 END AS case_pack_size,

        -- Sourcing cost and price from the most recent week's data
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.cost_unit END) AS w1_cost_unit,
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.price_unit END) AS w1_price_unit,

        -- Pre-Calculated Metrics
        (COALESCE(MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_sold END), 0) + COALESCE(MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_sold END), 0) + COALESCE(MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_sold END), 0) + COALESCE(MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_sold END), 0)) / 4.0 AS avg_four_week_sales,

        -- Item-level weekly data
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.price_unit END) as result_price_unit,
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_received END) AS w1_shipped, 
        MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_received END) AS w2_shipped, 
        MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_received END) AS w3_shipped, 
        MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_received END) AS w4_shipped,
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.quantity_sold END) AS w1_sold, 
        MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.quantity_sold END) AS w2_sold, 
        MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.quantity_sold END) AS w3_sold, 
        MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.quantity_sold END) AS w4_sold,
        MAX(CASE WHEN s.date_posting = p.date_w1 THEN s.shrink_percentage END) AS w1_shrink_p, 
        MAX(CASE WHEN s.date_posting = p.date_w2 THEN s.shrink_percentage END) AS w2_shrink_p, 
        MAX(CASE WHEN s.date_posting = p.date_w3 THEN s.shrink_percentage END) AS w3_shrink_p, 
        MAX(CASE WHEN s.date_posting = p.date_w4 THEN s.shrink_percentage END) AS w4_shrink_p,
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_received END) AS result_shipped,
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.quantity_sold END) AS result_sold,
        MAX(CASE WHEN s.date_posting = p.date_forecast THEN s.shrink_percentage END) AS result_shrink_p,

        -- Store-level weekly data
        sa.store_w1_received,
        sa.store_w1_sold,
        sa.store_w1_shrink,
        sa.store_w1_shrink_p,
        sa.store_w2_received,
        sa.store_w2_sold,
        sa.store_w2_shrink,
        sa.store_w2_shrink_p,
        sa.store_w3_received,
        sa.store_w3_sold,
        sa.store_w3_shrink,
        sa.store_w3_shrink_p,
        sa.store_w4_received,
        sa.store_w4_sold,
        sa.store_w4_shrink,
        sa.store_w4_shrink_p,
        sa.result_store_received,
        sa.result_store_sold,
        sa.result_store_shrink,
        sa.result_store_shrink_p

    FROM item_store_universe u
    CROSS JOIN params p
    -- Join to get item-level data
    LEFT JOIN clean_shrink s ON u.item_no = s.item_no AND u.store_no = s.store_no AND s.date_posting IN (p.date_w1, p.date_w2, p.date_w3, p.date_w4, p.date_forecast)
    -- Join to get store-level aggregate data
    LEFT JOIN store_weekly_aggs sa ON u.store_no = sa.store_no

    GROUP BY ALL
    ORDER BY
        u.store_no,
        u.item_no;
'''

def get_forecast_data(region_code, forecast_date, w_dates):
    query = query_forecast_base.format(
        region=region_code,
        forecast_date=forecast_date,
        w1=w_dates[0],
        w2=w_dates[1],
        w3=w_dates[2],
        w4=w_dates[3]
    )
    # print(query)
    result = conn_dd.execute(query).fetchall()
    
    # Get column names
    column_names = [desc[0] for desc in conn_dd.description]
    
    # Zip column names with data
    result_dicts = [dict(zip(column_names, row)) for row in result]
    
    return result_dicts

def calculate_sales_velocity(w4_sold, w3_sold, w2_sold, w1_sold):
    """
    Calculates the sales velocity trend using the slope of a linear regression line.

    Args:
        w4_sold (float): Sales from four weeks ago.
        w3_sold (float): Sales from three weeks ago.
        w2_sold (float): Sales from two weeks ago.
        w1_sold (float): Sales from the most recent week.

    Returns:
        float: The slope of the line of best fit, representing the weekly sales velocity.
               Returns 0.0 if there are fewer than two data points.
    """
    # Create the time points (x-axis) and sales data (y-axis).
    # We treat the weeks as points in time relative to the forecast date.
    weeks = np.array([-3, -2, -1, 0])
    sales = np.array([w4_sold, w3_sold, w2_sold, w1_sold], dtype=float)

    # Ensure we only use valid (non-null/NaN) data points for the calculation.
    valid_points = ~np.isnan(sales)
    
    # We need at least two data points to calculate a trend.
    if np.sum(valid_points) < 2:
        return 0.0

    # numpy.polyfit calculates the coefficients of a polynomial fit.
    # For a line (degree 1), the first coefficient [0] is the slope.
    slope = np.polyfit(weeks[valid_points], sales[valid_points], 1)[0]
    
    return slope

region_codes = ['BA', 'LA', 'SD', 'NE', 'SE', 'TE']
# region_codes = ['NE']

def save_to_database(result_set):
    # print('# of lines (result_set):', len(result_set))
    for d in result_set:

        if not d:
            print("Skipping save for an empty result set.")
            continue

        # Convert the list of processed rows to a Pandas DataFrame
        df_to_append = pd.DataFrame(d)

        # Ensure DataFrame columns match the table schema to prevent errors
        # This is a good practice, though not strictly necessary if the dict keys are perfect
        table_cols_query = "SELECT name FROM pragma_table_info('forecast_results');"
        table_cols = [col[0] for col in conn_dd.execute(table_cols_query).fetchall()]
        df_to_append = df_to_append[table_cols]

        # Append the DataFrame to the DuckDB table
        try:
            conn_dd.append('forecast_results', df_to_append)
            # print(f"Successfully appended {len(df_to_append)} rows to 'forecast_results'.")
        except Exception as e:
            print(f"An error occurred while appending data: {e}")

        if PUSH_FABRIC:
            fabric_table_name = 'costco_us_forecast'
            try:
                wh.append_df(df_to_append, fabric_table_name)
                print(f"Successfully appended {len(df_to_append)} rows to '{fabric_table_name}' in Fabric Warehouse.")
            except Exception as e:
                print(f"An error occurred while appending data to Fabric Warehouse: {e}")

result_set = []
def process_forecast(index, total, data, BASE_COVER, BASE_COVER_SOLD_OUT, K_FACTOR, CASE_SIZE, WEEK_WEIGHTS, HIGH_SHRINK_THRESHOLD, ROUND_DOWN_SHRINK_THRESHOLD):
    if index % 10 == 0:
        print(f"{index} of {total} {index/total*100:.2f}% Using parameters: Base Cover: {BASE_COVER}, Base Cover Sold Out: {BASE_COVER_SOLD_OUT}, K Factor: {K_FACTOR}, Case Size: {CASE_SIZE} | Week Weights: {WEEK_WEIGHTS}, High Shrink Threshold: {HIGH_SHRINK_THRESHOLD}, Round Down Shrink Threshold: {ROUND_DOWN_SHRINK_THRESHOLD}")
    # I will now loop over the data and do my forecast.
    w1_weight, w2_weight, w3_weight, w4_weight = WEEK_WEIGHTS
    forecast_set = []
    for i, row in enumerate(data, 1):
        if i < 100000:
            # Calculate sales velocity
            w4_sold = 0 if row.get('w4_sold', 0) is None else row.get('w4_sold', 0)
            w3_sold = 0 if row.get('w3_sold', 0) is None else row.get('w3_sold', 0)
            w2_sold = 0 if row.get('w2_sold', 0) is None else row.get('w2_sold', 0)
            w1_sold = 0 if row.get('w1_sold', 0) is None else row.get('w1_sold', 0)

            if row['region_code'] == 'TE':
                BASE_COVER = 0.1  # add 2% more base cover for Texas region

            # parameters
            row['base_cover'] = BASE_COVER

            row['base_cover_sold_out'] = BASE_COVER_SOLD_OUT
            row['k_factor'] = K_FACTOR

            row['forecast_w1_weight'] = w1_weight
            row['forecast_w2_weight'] = w2_weight
            row['forecast_w3_weight'] = w3_weight
            row['forecast_w4_weight'] = w4_weight
            row['forecast_high_shrink_threshold'] = HIGH_SHRINK_THRESHOLD
            row['forecast_round_down_shrink_threshold'] = ROUND_DOWN_SHRINK_THRESHOLD
            row['case_pack_size'] = CASE_SIZE

            sales_velocity = float(calculate_sales_velocity(w4_sold, w3_sold, w2_sold, w1_sold))  # BEGIN:
            row['sales_velocity'] = sales_velocity

            # calculate the average sold quantity, but don't average in if the week didn't have any sales
            try:
                total_sold = w4_sold + w3_sold + w2_sold + w1_sold
                weeks_with_sales = sum(1 for x in [w4_sold, w3_sold, w2_sold, w1_sold] if x > 0)
                if weeks_with_sales > 0:
                    average_sold = total_sold / weeks_with_sales
                else:
                    average_sold = 0
                row['average_sold'] = average_sold
            except Exception as e:
                print(e)
                print(row)

            # Calculate the Exponential Moving Average using the w1 to w4 sold quantities
            weights = np.array([w1_weight, w2_weight, w3_weight, w4_weight], dtype=float)
            sold_quantities = np.array([w1_sold, w2_sold, w3_sold, w4_sold], dtype=float)
            # Ensure we only use valid (non-null/NaN) data points for the calculation.
            valid_sold_quantities = ~np.isnan(sold_quantities)
            if np.sum(valid_sold_quantities) > 0:
                ema = np.average(sold_quantities[valid_sold_quantities], weights=weights[valid_sold_quantities])
            else:
                ema = 0.0

            # Calculate the sales volatility as the standard deviation of the sold quantities
            sales_volatility = np.std(sold_quantities[valid_sold_quantities]) if np.sum(valid_sold_quantities) > 1 else 0.0

            row['ema'] = ema
            row['sales_volatility'] = sales_volatility

            # Ideal Safety Stock to meet the service level
            # Check if the W1 Sold is the highest of the four weeks. If yes, we don't need safety stock

            # if w1_sold is not None and w2_sold is not None and w3_sold is not None and w4_sold is not None and w1_sold >= w2_sold and w1_sold >= w3_sold and w1_sold >= w4_sold:
            #     forecast_safety_stock = 0
            # else:
            #     # only calculate if its a hero item
            #     if int(row['item_no']) not in NON_HERO_ITEMS:
            #         forecast_safety_stock = math.floor(K_FACTOR * sales_volatility)
            #     else:
            #         forecast_safety_stock = 0

            forecast_safety_stock = 0

            row['forecast_safety_stock'] = forecast_safety_stock

            # If the W1 store sales is less than 95% of the W2 store sales, then use the max of w1_sold, ema, w2_sold * 70% + w1_sold * 30% as the average to be used for forecasting
            # if row['store_w1_sold'] < 0.95 * row['store_w2_sold']:
            #     row['forecast_average'] = max(w1_sold, ema, (0.7 * w2_sold + 0.3 * w1_sold))
            # elif row['store_w1_sold'] > 1.05 * row['store_w2_sold']:
            #     row['forecast_average'] = max(w1_sold, ema, (0.7 * w1_sold + 0.1 * w2_sold + 0.1 * w3_sold + 0.1 * w4_sold))
            # else:
            #     row['forecast_average'] = max(w1_sold, ema, w1_sold * 0.6 + w2_sold * 0.2 + w3_sold * 0.1 + w4_sold * 0.1)
            # row['forecast_average'] = max(w1_sold, ema, average_sold)
            row['forecast_average'] = max(w1_sold, ema)

            if row['w1_shipped'] is None or row['w1_shipped'] == 0:
                row['forecast_average'] = max(average_sold, ema)

            # See if the sales has declined more than 30% from W2 to W1, if yes then we ignore the W1 sold and use a weighted average of W2, W3, W4
            if row['w1_sold'] is not None and row['w2_sold'] is not None and row['w2_sold'] > 0:
                decline_percentage = (row['w2_sold'] - row['w1_sold']) / row['w2_sold']
                decline_percentage_store = (row['store_w2_sold'] - row['store_w1_sold']) / row['store_w2_sold'] if row['store_w2_sold'] and row['store_w2_sold'] > 0 else 0
                if decline_percentage >= .15 and decline_percentage_store >= .15:
                    # decline is more than the threshold, so we round down the forecast
                    row['forecast_average'] = max(row['forecast_average'], w2_sold * 0.5 + w3_sold * 0.4 + w4_sold * 0.1, ema)
            
            # special rule for LA starting 08/19
            if region == 'LA' and current_date >= datetime(2025, 8, 19) and current_date <= datetime(2025, 8, 25):
                row['forecast_average'] = row['forecast_average'] * (1 + 0.05)

            # Promotion for Bay Area from 08/25 to 08/31 (was requested 15% but we are using 10% for now)
            if region == 'BA' and current_date >= datetime(2025,8,25) and current_date <= datetime(2025, 8, 31):
                row['forecast_average'] = row['forecast_average'] * 1.1

            # Promotion for Bay Area from 09/22 to 09/28 (was requested 15% but we are using 4% for now)
            if region == 'BA' and current_date >= datetime(2025,9,22) and current_date <= datetime(2025, 9, 28):
                # National Promotion - Suggested 5-7%
                row['forecast_average'] = row['forecast_average'] * 1.05
        
            # use custom logic if last two weeks have had consistently higher shrink > High Shrink Threshold
            if row['w1_shrink_p'] is not None and row['w2_shrink_p'] is not None:
                if row['w1_shrink_p'] >= HIGH_SHRINK_THRESHOLD and row['w2_shrink_p'] >= HIGH_SHRINK_THRESHOLD:
                    # If the last two weeks had high shrink, use a conservative estimate
                    row['forecast_average'] = max(w1_sold, ema)

            if int(row['store_no']) in [490,674,691,738,1375,1653] and current_date >= datetime(2025,10,30) and current_date <= datetime(2025,11,2):
                # test higher percentages for a week.
                if row['w1_sold'] is not None and row['w1_shrink_p'] is not None:
                    row['forecast_average'] = row['forecast_average'] * 1.20

            if int(row['store_no']) in [465,1058,427,481,644,436,736,1028,1620,431,407,1079] and current_date >= datetime(2025,10,10) and current_date <= datetime(2025,10,16):
                # test higher percentages for a week.
                if row['w1_sold'] is not None and row['w1_shrink_p'] is not None:
                    row['forecast_average'] = row['forecast_average'] * 1.1

            if int(row['store_no']) in [423] and current_date >= datetime(2025,11,3) and current_date <= datetime(2025,11,9):
                row['forecast_average'] = row['forecast_average'] * 0.9

            # initial launch
            if int(row['item_no']) in [1984587] and current_date <= datetime(2025, 9, 21):
                row['forecast_average'] = row['forecast_average'] * 1.2

            if int(row['item_no']) in [1984587] and current_date >= datetime(2025, 9, 22) and current_date <= datetime(2025, 9, 28):
                row['forecast_average'] = row['forecast_average'] * 1.07

            if int(row['item_no']) in [1713314] and region == 'TE' and current_date >= datetime(2025, 10, 20) and current_date <= datetime(2025, 11, 2):
                row['forecast_average'] = row['forecast_average'] * 1.30

            if region == 'NE' and current_date >= datetime(2025, 10, 20) and current_date <= datetime(2025, 11, 2):
                row['forecast_average'] = row['forecast_average'] * 1.15
                
            # Add cover, and use math.ceil to round up the average forecast to the nearest integer
            # If the last week was sold out (w1_shipped == w1_sold) then use the BASE_COVER_SOLD_OUT instead of BASE_COVER
            if row['w1_shipped'] is not None and row['w1_sold'] is not None and row['w1_shipped'] == row['w1_sold']:
                row['base_cover_quantity'] = row['forecast_average'] * (BASE_COVER_SOLD_OUT)
                row['base_cover_applied'] = BASE_COVER_SOLD_OUT
            else:
                row['base_cover_quantity'] = row['forecast_average'] * (BASE_COVER)
                row['base_cover_applied'] = BASE_COVER

            # The quantities are supposed to be rounded up to the case pack size which is in the variable case_pack_size, identify the round_up quantity in a new field
            # case_pack_size = row.get('case_pack_size', 6)  # Default to 1 if not found
            case_pack_size = CASE_SIZE
            row['result_forecast_case_pack_size'] = case_pack_size  # Store the case pack size for reference

            # Add Cover
            row['forecast_average_w_cover'] = row['forecast_average'] + row['base_cover_quantity']
            
            # Forecast Quantity
            row['forecast_quantity'] = math.ceil(row['forecast_average_w_cover'] / case_pack_size) * case_pack_size

            row['round_up_quantity'] = row['forecast_quantity'] - row['forecast_average_w_cover']
            row['round_up_final'] = row['round_up_quantity'] # For consistency with old schema

            # # Calculate the round up quantity
            # round_up_quantity_base = int(np.ceil(row['forecast_average'] / case_pack_size)) * case_pack_size - row['forecast_average']
            # if (round_up_quantity_base > row['base_cover_quantity']):
            #     # if the round up is effectively more than the required base cover, we don't need to apply base cover at all
            #     row['forecast_average_w_cover'] = row['forecast_average']
            #     row['base_cover_applied'] = 0
            #     row['base_cover_quantity'] = 0
            #     round_up_quantity = round_up_quantity_base
            #     round_up_covers_base = True
            # else:
            #     # round up quantity is not enough to cover the base cover, so we need to compute round up quantity based on the forecast_average_w_cover
            #     row['forecast_average_w_cover'] = row['forecast_average'] + row['base_cover_quantity']
            #     round_up_quantity = int(np.ceil(row['forecast_average_w_cover'] / case_pack_size)) * case_pack_size - row['forecast_average_w_cover']
            #     round_up_covers_base = False

            # row['round_up_quantity'] = round_up_quantity  # Store the quantity to be added for rounding up

            round_up_quantity = row['round_up_quantity']
            
            # if the w1_shrink is greater than 0.1 then add the round up quantity to the forecast_average_w_cover
            # if the w1_shrink is less than 0.1 and the round up quantity is less or equal to 2 then round it down instead of rounding up, but ensure that at least 18 units are being sent before rounding down
            # if the item_no is 1940912 then always round down instead of rounding down (limited to -2 for round down)
            # save the variable as forecast_quantity

            # if int(row['item_no']) in NON_HERO_ITEMS:
            #     print(row['item_no'], row['item_desc'], row['w1_shrink_p'], round_up_quantity, row['forecast_average_w_cover'], case_pack_size)

            if row['w1_shrink_p'] is not None:
                if round_up_quantity >= (case_pack_size - 1): #int(row['item_no']) in NON_HERO_ITEMS and 
                    # print("Special Non-Hero Item Round Down Applied:", row['item_no'], row['item_desc'], row['w1_shrink_p'], round_up_quantity, row['forecast_average_w_cover'], case_pack_size)
                    round_up_revised = - (case_pack_size - round_up_quantity)
                elif round_up_quantity >= (case_pack_size - 2) and row['w1_shrink_p'] >= (ROUND_DOWN_SHRINK_THRESHOLD + 0.03): #int(row['item_no']) in NON_HERO_ITEMS and 
                    round_up_revised = - (case_pack_size - round_up_quantity)
                else:
                    round_up_revised = round_up_quantity
            else:
                round_up_revised = round_up_quantity

            # No Round Downs
            if int(row['store_no']) in [1653,691,1375,674,490,738,465,1058,427,481,644,436,736,1028,1620,431,407,1079] or row['region_code'] == 'NE':
                round_up_revised = round_up_quantity

            # if the round_down causes the sales to go below the average, we don't allow that
            # if int(row['store_no']) == 471:
            #     print(row['item_no'], row['item_desc'], row['w1_shrink_p'], round_up_quantity, round_up_revised, row['forecast_average_w_cover'], row['forecast_average'], case_pack_size)
            if (row['forecast_average_w_cover'] + round_up_revised) < row['forecast_average']:
                print('Not Possible to Round Down Below Average :', row['forecast_average'], " : ", row['forecast_average_w_cover'], " : ",  round_up_revised)
                round_up_revised = round_up_quantity

            row['round_up_final'] = round_up_revised

            # Compute the final Forecast Quantity
            row['forecast_quantity'] = row['forecast_average_w_cover'] + row['round_up_final']

            # Compute the impact of rounding to the forecasted_quantity
            # This is the difference between the forecasted quantity and the rounded up quantity
            # This is only relevant if the forecasted quantity is greater than the rounded up quantity
            # If the forecasted quantity is less than the rounded up quantity, then the impact is 0
            # This is saved in a new field called impact_of_rounding
            if row['forecast_quantity'] > row['forecast_average_w_cover']:
                row['impact_of_rounding'] = row['forecast_quantity'] - row['forecast_average_w_cover']
            else:
                row['impact_of_rounding'] = 0

            # Buffer Adjustment - Add a Case if Safety Stock is not Met
            if int(row['item_no']) not in NON_HERO_ITEMS and row['forecast_safety_stock'] > 0 and row['w1_sold'] is not None and row['w4_sold'] is not None and row['w1_sold'] < average_sold and row['w1_sold'] < row['w4_sold']:
                if row['round_up_final'] < row['forecast_safety_stock']:
                    # apply safety stock (rounded up to the case_pack_size)
                    row['forecast_safety_stock_applied'] = case_pack_size
                    row['forecast_quantity'] += row['forecast_safety_stock_applied']
                else:
                    row['forecast_safety_stock_applied'] = 0
            else:
                row['forecast_safety_stock_applied'] = 0

            # This is the final check.
            # Here we check if the rounding up has caused the forecast quantity to be higher than the base cover or the base cover sold out, and if it is higher we try to reduce it to bring it closer to the base cover
            effective_cover = row['forecast_quantity'] / row['forecast_average'] if row['forecast_average'] > 0 else 0
            if row['w1_shrink_p'] == 0:
                if effective_cover > (1 + BASE_COVER_SOLD_OUT) and (row['forecast_quantity'] / case_pack_size) > 2:
                    # reduce the forecast quantity to be closer to the base cover sold out
                    target_quantity = math.ceil(row['forecast_average'] * (1 + BASE_COVER_SOLD_OUT) / row['case_pack_size']) * row['case_pack_size']
                    if target_quantity < row['forecast_quantity']:
                        row['forecast_quantity'] = target_quantity

            if row['store_no'] in INACTIVE_STORES:
                row['forecast_quantity'] = 0

            # Calculate delta from last week shipment
            try:
                row['delta_from_last_week'] = row['forecast_quantity'] - (0 if row['w1_shipped'] is None else row['w1_shipped'])
            except Exception as e:
                print(row)

            # Calculate the Shrink % if the Sales were same as last week
            if row['forecast_quantity'] > 0:
                row['forecast_shrink_last_week_sales'] = (row['forecast_quantity'] - row['w1_sold'])/row['forecast_quantity'] if (0 if row['w1_sold'] is None else row['w1_sold']) > 0 else 0
                row['forecast_shrink_average'] = (row['forecast_quantity'] - row['forecast_average'])/row['forecast_quantity'] if row['forecast_average'] > 0 else 0
            else:
                row['forecast_shrink_last_week_sales'] = 0
                row['forecast_shrink_average'] = 0

            # AI Forecasting Fields - Placeholder values for now
            row['ai_forecast'] = 0
            row['ai_difference'] = 0
            row['ai_reasoning'] = ''

            # If actuals exist, compute the forecast results
            if row['result_shipped'] is not None and row['result_sold'] is not None:
                row['result_forecast_shrink'] = row['forecast_quantity'] - row['result_sold']
                row['result_forecast_shrink_p'] = row['result_forecast_shrink'] / row['forecast_quantity'] if row['forecast_quantity'] > 0 else 0
                row['result_forecast_sold_out'] = 1 if row['result_forecast_shrink'] <= 0 else 0

                # round this

                row['result_sales_amount'] = round(row['result_sold'] * row['result_price_unit'], 4)

                if row['result_forecast_shrink'] > 0:
                    row['result_forecast_shrink_cost'] =  round(Decimal(row['result_price_unit']) * Decimal(row['result_forecast_shrink']) * Decimal(FOOD_COST_COMPONENT), 4)
                else:
                    row['result_forecast_shrink_cost'] = 0

                if row['result_forecast_shrink'] < 0:
                    row['result_forecast_lost_sales'] = round(Decimal(row['result_price_unit']) * abs(Decimal(row['result_forecast_shrink'])), 4)
                else:
                    row['result_forecast_lost_sales'] = 0

                row['result_forecast_margin_amount'] = round(((row['result_sales_amount'] - row['result_forecast_lost_sales']) * (1 - Decimal(FOOD_COST_COMPONENT))) - row['result_forecast_shrink_cost'], 4)
            else:
                row['result_forecast_shrink'] = 0
                row['result_forecast_shrink_p'] = 0
                row['result_forecast_sold_out'] = 0
                row['result_sales_amount'] = 0
                row['result_forecast_shrink_cost'] = 0
                row['result_forecast_lost_sales'] = 0
                row['result_forecast_margin_amount'] = 0

            forecast_set.append(row)
    return forecast_set


result_set = []
for region in region_codes:
    print(f"Processing region: {region}")
    # Here you would typically run your region-specific processing logic
    # For demonstration, we will just print the region code
    # Add your processing logic here

    # Delete from Forecast Results Table if already exists
    delete_query = f"""
    DELETE FROM forecast_results
    WHERE date_forecast >= '{FORECAST_START_DATE}' AND date_forecast <= '{FORECAST_END_DATE}' AND region_code = '{region}';
    """
    try:
        conn_dd.execute(delete_query)
        print(f"Deleted existing rows for range {FORECAST_START_DATE} to {FORECAST_END_DATE} for region {region}.")
    except Exception as e:
        print(f"An error occurred while deleting existing rows: {e}")

    if PUSH_FABRIC:
        # Delete from Fabric Warehouse Table if already exists
        delete_fabric_query = f"""
            DELETE FROM BNTO_WH_300_GOLD.dbo.costco_us_forecast
            WHERE date_forecast >= '{FORECAST_START_DATE}' AND date_forecast <= '{FORECAST_END_DATE}' AND region_code = '{region}';
        """
        try:
            wh.execute_query(delete_fabric_query)
            print(f"Deleted existing rows for range {FORECAST_START_DATE} to {FORECAST_END_DATE} for region {region} in Fabric.")
        except Exception as e:
            print(f"An error occurred while deleting existing rows in Fabric Warehouse: {e}")


    current_date = FORECAST_START_DATE_V
    while current_date <= FORECAST_END_DATE_V:

        date_str = current_date.strftime('%Y-%m-%d')
        print(f"Processing data for {region} | Date: {date_str}")
        
        forecast_date = current_date.strftime('%Y-%m-%d')
        # find the w1 to w4 dates, w1 being the immediate last week same day, but ignore the date if it falls in the exceptional days. continue assigning w2, w3, w4 until all 4 weeks are assigned
        w_dates = []
        for i in range(1, 6):
            w_date = current_date - timedelta(weeks=i)
            if w_date.strftime('%Y-%m-%d') not in EXCEPTIONAL_DAYS:
                w_dates.append(w_date.strftime('%Y-%m-%d'))
            if len(w_dates) == 4:
                break

        source_data = get_forecast_data(region, forecast_date, w_dates)
        print(f"Executing forecast for {len(param_sets)}  parameter sets for region {region} on date {forecast_date} with w_dates: {w_dates}")

        # I have so many param_sets to test on, what would be the best approach to make this process faster.

        total = len(param_sets)
        for index, params in enumerate(param_sets, 1):
            data = deepcopy(source_data)
            BASE_COVER = params['BASE_COVER']
            BASE_COVER_SOLD_OUT = params['BASE_COVER_SOLD_OUT']
            K_FACTOR = params['K_FACTOR']
            CASE_SIZE = params['CASE_SIZE']
            WEEK_WEIGHTS = params['WEEK_WEIGHTS']
            HIGH_SHRINK_THRESHOLD = params['HIGH_SHRINK_THRESHOLD']
            ROUND_DOWN_SHRINK_THRESHOLD = params['ROUND_DOWN_SHRINK_THRESHOLD']
        
            with concurrent.futures.ProcessPoolExecutor(os.cpu_count()) as executor:
                # Process the forecast in parallel
                data = deepcopy(source_data)
                results = list(executor.map(
                    process_forecast,
                    [index],
                    [total],
                    [data],
                    [BASE_COVER],
                    [BASE_COVER_SOLD_OUT],
                    [K_FACTOR],
                    [CASE_SIZE],
                    [WEEK_WEIGHTS],
                    [HIGH_SHRINK_THRESHOLD],
                    [ROUND_DOWN_SHRINK_THRESHOLD]
                ))
                result_set += results

                save_to_database(result_set)
                result_set = []
        current_date += timedelta(days=1)


# Export the results to the forecast_results table  

# exports the forecast results for a given region and forecast date to a json file
# the json should have a header with the column names, and the data should be in an array of objects (without columns as this would be redundant)
def export_json(region, forecast_date):
    export_query = f'''
        SELECT *
        FROM forecast_results
        WHERE region_code = '{region}'
        AND date_forecast = '{forecast_date}'
        ORDER BY store_no, item_no
    '''
    forecast_results = conn_dd.execute(export_query).fetchdf()
    output_file = f'costco_order_forecasting/json/Costco_{region}_PO_{forecast_date}.json'
    # 3. Export to JSON using orient='split' for a compact structure
    #    We set index=False so the DataFrame index isn't included in the output.
    forecast_results.to_json(output_file, orient='split', index=False, indent=2)
    print(f"Forecast results exported to {output_file}")

if GENERATE_FILE_OUTPUTS:
    for region in region_codes:
        print(f"Forecasting completed for region: {region}")

        output_file = f'costco_order_forecasting/excel/Costco_{region}_PO_{FORECAST_START_DATE}_{FORECAST_END_DATE}_SOURCE.xlsx'
        xl = XLWriter(filename=output_file)

        current_date = FORECAST_START_DATE_V
        while current_date <= FORECAST_END_DATE_V:
            date_str = current_date.strftime('%Y-%m-%d')

            if EXPORT_JSON:
                export_json(region, date_str)

            # write to excel
            forecats_results_query = f'''
                SELECT
                    -- week number from the forecast date
                    EXTRACT(WEEK FROM fr.date_forecast) AS "Fiscal Week #",
                    -- forecast date (formatted as mm/dd/yyyy)
                    fr.date_forecast AS "Date",
                    -- day name
                    strftime(fr.date_forecast, '%A') AS "Day Name",
                    fr.region_code AS "Region",
                    fr.store_no AS "Warehouse #",
                    nm.store_name AS "Warehouse Name",
                    fr.item_no AS "Item #",
                    fr.item_desc AS "Item Description",
                    fr.forecast_quantity AS "PO Qty (Units)",
                    fr.forecast_quantity / fr.case_pack_size AS "PO Qty (Cases)"
                FROM forecast_results fr
                -- left join against the latest data from shrink table which has store_no and store_name to derive the store_name, the store_name changes so we're using latest available name
                LEFT JOIN
                (
                    SELECT DISTINCT s.store_no, s.store_name
                    FROM main.shrink s
                    JOIN (
                        SELECT store_no, MAX(date_posting) AS latest_date
                        FROM main.shrink
                        WHERE region_code = '{region}'
                        GROUP BY store_no
                    ) AS latest
                    ON s.store_no = latest.store_no AND s.date_posting = latest.latest_date
                    WHERE s.region_code = '{region}'
                ) nm ON fr.store_no = nm.store_no
                WHERE fr.region_code = '{region}'
                AND fr.date_forecast = '{date_str}'
                ORDER BY fr.store_no, fr.item_no
            '''
            # extract the array of column names, and the array of the data as a list of dictionaries
            results = pl.from_pandas(conn_dd.sql(forecats_results_query).to_df())
            # add a worksheet with the name as day of week. ex. Sun
            ws = xl.wb.add_worksheet(name=current_date.strftime('%a').upper())
            # write the header
            header = results.columns
            for col_num, col_name in enumerate(header):
                ws.write(0, col_num, col_name, xl.format_col_title)

            # write the data
            for row_num, row in enumerate(results.to_dicts(), start=1):
                for col_num, col_name in enumerate(header):
                    if col_name == 'Date':
                        ws.write(row_num, col_num, row[col_name], xl.format_date_costco)
                    else:
                        ws.write(row_num, col_num, row[col_name])
            
            # freeze at A2
            ws.freeze_panes(1, 0)

            #autofit
            ws.autofit()

            # next date
            current_date += timedelta(days=1)

        # Save the workbook
        xl.wb.close()
        print(f"Forecast results exported to {output_file}")

conn_dd.close()
wh.disconnect()