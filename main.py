"""
Costco Order Forecasting v6 - Main Orchestrator
================================================
This is the main entry point for the forecasting pipeline.

The pipeline executes the following steps:
1. Load data from Fabric Datalake
2. Load configuration files
3. Load weather data
4. For each region and date:
   a. Calculate base forecast
   b. Apply adjustments (promotions, holidays, etc.)
   c. Apply rounding and safety stock
   d. Calculate result metrics
   e. Save to database
5. Export results (Excel, JSON)

Usage:
    python main.py

Configuration:
    Edit config/settings.py to modify forecast parameters.
"""

import os
import sys
import math
from datetime import datetime, timedelta
from copy import deepcopy
from decimal import Decimal
import concurrent.futures

import pandas as pd
import numpy as np

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import modules
from config import settings
from config.scenarios import get_scenarios
from data.loader import DataLoader
from data.prep import (
    create_forecast_results_table,
    get_historical_week_dates,
    get_forecast_data
)
from forecasting.engine import (
    calculate_base_forecast,
    apply_decline_adjustment,
    apply_high_shrink_adjustment
)
from forecasting.adjustments import (
    apply_all_adjustments,
    apply_store_level_pass,
    get_store_level_adjustment_summary
)
from forecasting.rounding import apply_all_rounding, calculate_safety_stock
from forecasting.weather_adjustment import (
    apply_weather_adjustments,
    get_weather_adjustment_summary,
    print_weather_adjustment_report
)
from weather.loader import load_all_weather_data, enrich_row_with_weather
from export.excel import export_all_regions_to_excel
from export.json_export import export_all_to_json
from export.regional_summary import export_all_regional_summaries
from utils.fabric_warehouse import FabricDatalakeWH


def calculate_forecast_metrics(row: dict) -> dict:
    """
    Calculate additional forecast metrics.
    
    Args:
        row: Processed forecast row
        
    Returns:
        Row with metrics calculated
    """
    forecast_qty = row.get('forecast_quantity', 0)
    forecast_avg = row.get('forecast_average', 0)
    w1_sold = row.get('w1_sold', 0) or 0
    
    # Calculate shrink metrics
    if forecast_qty > 0:
        row['forecast_shrink_last_week_sales'] = (forecast_qty - w1_sold) / forecast_qty if w1_sold > 0 else 0
        row['forecast_shrink_average'] = (forecast_qty - forecast_avg) / forecast_qty if forecast_avg > 0 else 0
    else:
        row['forecast_shrink_last_week_sales'] = 0
        row['forecast_shrink_average'] = 0
    
    # AI placeholder fields
    row['ai_forecast'] = 0
    row['ai_difference'] = 0
    row['ai_reasoning'] = ''
    
    return row


def calculate_result_metrics(row: dict, food_cost_component: float = 0.31) -> dict:
    """
    Calculate actual result metrics if actuals are available.
    
    Args:
        row: Forecast row with actuals
        food_cost_component: Food cost percentage
        
    Returns:
        Row with result metrics calculated
    """
    result_shipped = row.get('result_shipped')
    result_sold = row.get('result_sold')
    forecast_qty = row.get('forecast_quantity', 0)
    result_price = row.get('result_price_unit', 0) or 0
    
    if result_shipped is not None and result_sold is not None:
        row['result_forecast_shrink'] = forecast_qty - result_sold
        row['result_forecast_shrink_p'] = row['result_forecast_shrink'] / forecast_qty if forecast_qty > 0 else 0
        row['result_forecast_sold_out'] = 1 if row['result_forecast_shrink'] <= 0 else 0
        
        row['result_sales_amount'] = round(result_sold * result_price, 4)
        
        if row['result_forecast_shrink'] > 0:
            row['result_forecast_shrink_cost'] = round(
                float(result_price) * float(row['result_forecast_shrink']) * food_cost_component, 4
            )
        else:
            row['result_forecast_shrink_cost'] = 0
        
        if row['result_forecast_shrink'] < 0:
            row['result_forecast_lost_sales'] = round(
                float(result_price) * abs(float(row['result_forecast_shrink'])), 4
            )
        else:
            row['result_forecast_lost_sales'] = 0
        
        row['result_forecast_margin_amount'] = round(
            ((row['result_sales_amount'] - row['result_forecast_lost_sales']) * 
             (1 - food_cost_component)) - row['result_forecast_shrink_cost'], 4
        )
    else:
        row['result_forecast_shrink'] = 0
        row['result_forecast_shrink_p'] = 0
        row['result_forecast_sold_out'] = 0
        row['result_sales_amount'] = 0
        row['result_forecast_shrink_cost'] = 0
        row['result_forecast_lost_sales'] = 0
        row['result_forecast_margin_amount'] = 0
    
    return row


def process_forecast_row(row: dict, params: dict, current_date: datetime,
                         vc_weather: dict, accu_weather: dict, owm_weather: dict = None) -> dict:
    """
    Process a single forecast row through all pipeline steps.
    
    Args:
        row: Raw data row
        params: Forecast parameters
        current_date: Current forecast date
        vc_weather: VisualCrossing weather data dict
        accu_weather: AccuWeather data dict
        owm_weather: OpenWeatherMap data dict (optional)
        
    Returns:
        Fully processed forecast row
    """
    # Store parameters in row
    row['k_factor'] = params['K_FACTOR']
    row['forecast_w1_weight'] = params['WEEK_WEIGHTS'][0]
    row['forecast_w2_weight'] = params['WEEK_WEIGHTS'][1]
    row['forecast_w3_weight'] = params['WEEK_WEIGHTS'][2]
    row['forecast_w4_weight'] = params['WEEK_WEIGHTS'][3]
    row['forecast_high_shrink_threshold'] = params['HIGH_SHRINK_THRESHOLD']
    row['forecast_round_down_shrink_threshold'] = params['ROUND_DOWN_SHRINK_THRESHOLD']
    
    # Step 1: Enrich with weather data
    row = enrich_row_with_weather(row, vc_weather, accu_weather, owm_weather)
    
    # Step 2: Calculate base forecast (velocity, EMA, etc.)
    row = calculate_base_forecast(row, params['WEEK_WEIGHTS'])
    
    # Step 3: Apply decline adjustment
    row = apply_decline_adjustment(row)
    
    # Step 4: Apply all business adjustments
    row = apply_all_adjustments(
        row, current_date, 
        params['BASE_COVER'], params['BASE_COVER_SOLD_OUT']
    )
    
    # Step 5: Apply high shrink adjustment
    row = apply_high_shrink_adjustment(row, params['HIGH_SHRINK_THRESHOLD'])
    
    # Step 6: Calculate safety stock
    row['forecast_safety_stock'] = calculate_safety_stock(
        row.get('sales_volatility', 0), params['K_FACTOR']
    )
    
    # Step 7: Apply rounding and finalize quantity
    row = apply_all_rounding(row, params)
    
    # Step 8: Calculate forecast metrics
    row = calculate_forecast_metrics(row)
    
    # Step 9: Calculate result metrics (if actuals available)
    row = calculate_result_metrics(row, settings.FOOD_COST_COMPONENT)
    
    return row


def save_forecast_results(conn, results: list):
    """
    Save forecast results to DuckDB.
    
    Args:
        conn: DuckDB connection
        results: List of result dictionaries
    """
    if not results:
        return
    
    df = pd.DataFrame(results)
    
    # Get table columns
    table_cols = [col[0] for col in conn.execute(
        "SELECT name FROM pragma_table_info('forecast_results');"
    ).fetchall()]
    
    # Ensure columns match
    for col in table_cols:
        if col not in df.columns:
            df[col] = None
    
    df = df[table_cols]
    
    try:
        conn.append('forecast_results', df)
    except Exception as e:
        print(f"Error saving results: {e}")


def main():
    """Main entry point for the forecasting pipeline."""
    print("=" * 70)
    print("COSTCO ORDER FORECASTING v6")
    print("=" * 70)
    print(f"Forecast Range: {settings.FORECAST_START_DATE} to {settings.FORECAST_END_DATE}")
    print(f"Regions: {settings.REGION_CODES}")
    print("=" * 70)
    
    # Ensure output directories exist
    settings.ensure_directories()
    
    # Initialize data loader
    loader = DataLoader()
    conn = loader.get_connection()
    
    # Create forecast results table
    create_forecast_results_table(conn, force=True)
    
    # Step 1: Load data
    if settings.LOAD_DATA:
        print("\n[Step 1] Loading data from Fabric Datalake...")
        loader.load_shrink_data()
        
        # Load configuration
        if os.path.exists(settings.CONFIG_FILE_PATH):
            loader.load_configuration()
    
    # Step 2: Load weather data
    print("\n[Step 2] Loading weather data...")
    vc_weather, accu_weather, owm_weather = load_all_weather_data()
    
    # Step 3: Get scenarios to run
    param_sets = get_scenarios(settings.SCENARIO_TESTING)
    print(f"\n[Step 3] Running {len(param_sets)} scenario(s)...")
    
    # Initialize warehouse connection if pushing to Fabric
    wh = None
    if settings.PUSH_FABRIC:
        wh = FabricDatalakeWH()
    
    # Step 4: Process each region
    for region in settings.REGION_CODES:
        print(f"\n{'='*50}")
        print(f"Processing Region: {region}")
        print(f"{'='*50}")
        
        # Delete existing results for this date range
        delete_query = f"""
            DELETE FROM forecast_results
            WHERE date_forecast >= '{settings.FORECAST_START_DATE}' 
            AND date_forecast <= '{settings.FORECAST_END_DATE}' 
            AND region_code = '{region}';
        """
        conn.execute(delete_query)
        
        # Process each date
        current_date = settings.FORECAST_START_DATE_V
        while current_date <= settings.FORECAST_END_DATE_V:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n  Processing {region} | {date_str}")
            
            # Get historical week dates
            w_dates = get_historical_week_dates(current_date)
            print(f"    Week dates: {w_dates}")
            
            # Get base forecast data
            source_data = get_forecast_data(conn, region, date_str, w_dates)
            print(f"    Found {len(source_data)} item-store combinations")
            
            # Process each scenario
            for params in param_sets:
                data = deepcopy(source_data)
                forecast_results = []
                
                # Process each row through base forecast pipeline
                for row in data:
                    processed = process_forecast_row(
                        row, params, current_date, vc_weather, accu_weather, owm_weather
                    )
                    forecast_results.append(processed)
                
                # Step 4a: Apply store-level pass (BEFORE weather adjustments)
                # This ensures store-level shrink stays below 20% and coverage is addressed
                print("    Applying store-level pass...")
                forecast_results = apply_store_level_pass(
                    forecast_results,
                    shrink_threshold=0.20,  # 20% max store shrink
                    verbose=False
                )
                
                # Count store-level adjustments
                store_adj_count = sum(1 for r in forecast_results if r.get('store_level_adjusted', 0))
                if store_adj_count > 0:
                    print(f"    Store-level adjusted: {store_adj_count} items")
                
                # Step 4b: Apply weather adjustments
                if settings.APPLY_WEATHER_ADJUSTMENT:
                    print("    Applying weather adjustments...")
                    forecast_results = apply_weather_adjustments(
                        forecast_results,
                        vc_weather,
                        severity_threshold=settings.WEATHER_SEVERITY_THRESHOLD,
                        max_store_reduction_pct=getattr(settings, 'WEATHER_MAX_REDUCTION', 0.40),
                        verbose=False
                    )
                    
                    # Count adjustments for this batch
                    adjusted_count = sum(1 for r in forecast_results if r.get('weather_adjusted', 0))
                    if adjusted_count > 0:
                        print(f"    Weather adjusted: {adjusted_count} items")
                
                # Save results
                save_forecast_results(conn, forecast_results)
                print(f"    Saved {len(forecast_results)} forecasts")
            
            current_date += timedelta(days=1)
    
    # Step 5: Generate weather adjustment summary report
    if settings.APPLY_WEATHER_ADJUSTMENT:
        print("\n[Step 5] Weather Adjustment Summary...")
        
        # Query all results to generate summary
        all_results = conn.execute(f"""
            SELECT * FROM forecast_results
            WHERE date_forecast >= '{settings.FORECAST_START_DATE}'
            AND date_forecast <= '{settings.FORECAST_END_DATE}'
        """).fetchdf().to_dict('records')
        
        summary = get_weather_adjustment_summary(all_results)
        print_weather_adjustment_report(summary)
    
    # Step 6: Export results
    if settings.GENERATE_FILE_OUTPUTS:
        print("\n[Step 6] Exporting results...")
        
        if settings.EXPORT_JSON:
            export_all_to_json(
                conn, settings.REGION_CODES,
                settings.FORECAST_START_DATE, settings.FORECAST_END_DATE
            )
        
        # Export standard Excel files
        export_all_regions_to_excel(
            conn, settings.REGION_CODES,
            settings.FORECAST_START_DATE_V, settings.FORECAST_END_DATE_V
        )
        
        # Export regional summary reports (professional stakeholder reports)
        print("  Generating regional summary reports...")
        export_all_regional_summaries(
            conn, settings.REGION_CODES,
            settings.FORECAST_START_DATE_V, settings.FORECAST_END_DATE_V
        )
    
    # Cleanup
    loader.disconnect()
    if wh:
        wh.disconnect()
    
    print("\n" + "=" * 70)
    print("FORECASTING COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
