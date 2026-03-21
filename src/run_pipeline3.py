import yaml
import logging
import pandas as pd
import os
import sys
import yfinance as yf
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Custom modules I created
from fetch_data import download_ohlcv_to_csv, download_ecb_data
from validate_quality2 import load_data, run_quality_checks, check_with_benchmark
from forecast_analysis import generate_forecast

# Set custom cache location relative to project to avoid system level conflicts
if not os.path.exists("cache"):
    os.makedirs("cache")
yf.set_tz_cache_location("cache")

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Configure Logging
logging.basicConfig(
    level=getattr(logging, config['pipeline']['settings']['log_level']),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PipelineOrchestrator")

def sanitize_index(df, ticker_name):
    """Ensures Date index is clean, sorted and timezone-naive"""
    try:
        # Column normalization
        if 'TIME_PERIOD' in df.columns:
            df = df.rename(columns={'TIME_PERIOD': 'Date'})
        
        # Normalize ECB Value Column to 'Close'
        if 'OBS_VALUE' in df.columns:
            df = df.rename(columns={'OBS_VALUE': 'Close'})

        # Set Index
        if 'Date' in df.columns:
            df = df.set_index('Date')
        
        # Datetime conversion
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index)

        # Timezone strip
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        # Unit Normalization
        if ticker_name == "EURUSD=X" and not df.empty:
            avg_price = df['Close'].mean()
            if avg_price > 100 and avg_price < 2000:
                logger.warning(f"Scale anomaly detected for {ticker_name} (~{avg_price:.2f}). Normalizing /1000")
                for col in ['Open', 'High', 'Low', 'Close']:
                    if col in df.columns:
                        df[col] = df[col] / 1000
            elif avg_price > 10000:
                logger.critical(f"Data Type Mismatch: {ticker_name} contains prices around {avg_price:.0f}. This looks like Bitcoin data!")
                return pd.DataFrame()

        # Sort
        df = df.sort_index()

        return df
    except Exception as e:
        logger.error(f"Sanitization failed for {ticker_name}: {e}")
        return df

def process_yahoo_download(ticker, start, end, folder):
    path = download_ohlcv_to_csv(ticker, start, end, "1d", folder)
    return ticker, path

def process_ecb_download(etick, start, end, folder):
    path = download_ecb_data(etick, start, end, folder)
    return etick, path

def run_automation():
    logger.info("--- Starting Data Pipeline ---\n")

    # 1 Dynamic Dates
    days_back = config['pipeline']['settings']['history_days']
    max_workers = config['pipeline']['settings']['max_workers']
    data_folder = config['pipeline']['settings']['data_folder']

    # Load ML Targets from Config
    ml_target_list = config['pipeline'].get('ml_tickers', [])

    # Circuit Breaker Config
    FAILURE_THRESHOLD = config['pipeline']['settings'].get('failure_threshold', 0.50)
    processed_count = 0
    failure_count = 0

    today = datetime.now()
    start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    logger.info(f"Time window: {start_date} to {end_date} ({days_back} days history)")

    # 2 Yahoo Finance Data Ingestion
    yahoo_files = {}
    yahoo_tickers = config['pipeline']['yahoo_tickers']

    logger.info(f"Starting parallel download for {len(yahoo_tickers)} Yahoo tickers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_yahoo_download, t, start_date, end_date, data_folder): t for t in yahoo_tickers}

        for future in as_completed(futures):
            ticker, path = future.result()
            if path: yahoo_files[ticker] = path
            else:
                logger.error(f"Download failed for {ticker}")
    
    # 3 ECB Data Ingestion
    ecb_files = {}
    ecb_tickers = config['pipeline']['ecb_tickers']

    if ecb_tickers:
        logger.info(f"Starting parallel download for {len(ecb_tickers)} ECB benchmark...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_ecb_download, t, start_date, end_date, data_folder): t for t in ecb_tickers}

            for future in as_completed(futures):
                etick, path = future.result()
                if path: ecb_files[etick] = path
                else:
                    logger.error(f"Download failed for {etick}")

    # 4 Processing Loop (Validation -> Slice -> Forecast)
    logger.info("Ingestion Complete. Starting Processing Loop...")
    all_quarantine = pd.DataFrame()
    benchmark_map = config['pipeline']['benchmark_mapping']

    for ticker, file_path in yahoo_files.items():
        processed_count += 1
        ticker_has_issue = False
        
        # A Load master data worth 730 days
        df_full = load_data(file_path)
        if df_full is None: continue

        # B Validate Master Data
        # Clean the FULL history to ensure model doesn't train on garbage
        clean_df_full, quarantine_df_full = run_quality_checks(df_full, ticker)

        # If data is too messy (empty after cleaning), skip it
        if clean_df_full.empty:
            logger.warning(f"CRITICAL DATA LOSS: {ticker} is empty after validation")
            ticker_has_issue = True
        else:
            # Save as Parquet (Scale improvement: Faster and smaller than csv)
            trusted_path = file_path.replace(".csv", ".parquet")
            clean_df_full.to_parquet(trusted_path)

        # Overwrite the original file with CLEAN full history
        # Ensure file_path now points to trusted data for the ML model
        clean_df_full.to_csv(file_path)

        # C Create Weekly Slice for Analysts
        # Slice clean data to just the last 7 days
        clean_df_full = sanitize_index(clean_df_full, ticker)

        # calculate the cutoff (7 days ago)
        cutoff_date = datetime.now() - timedelta(days=7)

        # Create the slice
        df_weekly = clean_df_full[clean_df_full.index >= cutoff_date].copy()

        # Save slice for analysts
        weekly_filename = file_path.replace(".csv", "_Analyst_Weekly_view.csv")
        df_weekly.to_csv(weekly_filename)
        logger.info(f"Created Analyst View for {ticker}: {weekly_filename}")        

        # D. Benchmark check for EURUSD
        # Only run this for the weekly data
        if ticker in benchmark_map and not df_weekly.empty:
            ecb_key = benchmark_map[ticker]
            if ecb_key in ecb_files:
                try:
                    logger.info(f"Triggering Benchmark Check: {ticker} vs {ecb_key}")
                    df_ecb = pd.read_csv(ecb_files[ecb_key])
                
                    # Sanitize ECB Data
                    df_ecb = sanitize_index(df_ecb, ecb_key)

                    # Merge df_weekly and df_ecb
                    recon_failures = check_with_benchmark(df_weekly, df_ecb)

                    if not recon_failures.empty:
                        ticker_has_issue = True
                        logger.warning(f"Found {len(recon_failures)} mismatches for {ticker} (Weekly View)")
                        # Add to report
                        quarantine_df_full = pd.concat([quarantine_df_full, recon_failures[['Close', 'qa_reason']]])
                except Exception as e:
                    logger.error(f"Benchmark check failed for {ticker}: {e}")
                    
        # Accumulate ALL Failures (Full History Logic Failures + Weekly Recon Failures)
        if not quarantine_df_full.empty:
            quarantine_df_full['Ticker'] = ticker
            all_quarantine = pd.concat([all_quarantine, quarantine_df_full])

        # E ML Forecasting ON Full Clean History
        # Only run on key assets
        if ticker in ml_target_list:
            logger.info(f"Training Prophet Model on full clean history for {ticker}...")
            # Run Prophet Model
            # Returns image path and boolean is_anomaly flag
            img_path, is_anomaly = generate_forecast(file_path, ticker)

            if is_anomaly:
                ticker_has_issue = True
                msg = "ML Anomaly: Price outside 95% Confidence Interval"
                logger.error(f"[ML ALERT] {msg} for {ticker}")

                # Add to Quarantine Report
                anomaly_row = pd.DataFrame([{
                    'Ticker': ticker,
                    'Close': 'Check Forecast',
                    'qa_reason': msg
                }])
                all_quarantine = pd.concat([all_quarantine, anomaly_row])

        # F Circuit Breaker Logic
        if ticker_has_issue:
            failure_count += 1
        
        if processed_count >= 2:
            fail_rate = failure_count / processed_count
            if fail_rate >= FAILURE_THRESHOLD:
                logger.critical(f"CIRCUIT BREAKER TRIPPED! Failure rate {fail_rate:.0%} exceeds {FAILURE_THRESHOLD:.0%}.")
                logger.critical("Stopping pipeline to prevent data corruption.")
                sys.exit(1) # Kill GitHub Action
        

    # 6 Final Reports
    if not all_quarantine.empty:
        report_name = f"{data_folder}/QUARANTINE_REPORT_{datetime.now().strftime('%Y_%m_%d')}.csv"        
        all_quarantine.to_csv(report_name) # Only save if errors exist

        # Only alert on New issues (Last 7 days)
        # Assumes index is Datetime If not skip filtering for now or need to set index
        # For safety in this script, just log the total count

        logger.error(f"Pipeline finished with {len(all_quarantine)} TOTAL issues. Report: {report_name}")
    else:
        logger.info("Pipeline finished SUCCESSFULLY. No data issues found.")

if __name__ == "__main__":
    run_automation()