import yaml
import logging
import pandas as pd
import os
import yfinance as yf
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from fetch_data import download_ohlcv_to_csv, download_ecb_data
from validate_quality import load_data, run_quality_checks, check_with_benchmark
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
        # A Load master data worth 730 days
        df_full = load_data(file_path)
        if df_full is None: continue

        # B Validate Master Data
        # Clean the FULL history to ensure model doesn't train on garbage
        clean_df_full, quarantine_df_full = run_quality_checks(df_full, ticker)

        # If data is too messy (empty after cleaning), skip it
        if clean_df_full.empty:
            logger.warning(f"Skipping {ticker}: No clean data remaining after validation")
            continue

        # Overwrite the original file with CLEAN full history
        # Ensure file_path now points to trusted data for the ML model
        clean_df_full.to_csv(file_path)

        # C Create Weekly Slice for Analysts
        # Slice clean data to just the last 7 days
        if 'Date' in clean_df_full.columns:
            clean_df_full['Date'] = pd.to_datetime(clean_df_full['Date'])
            
        # Remove timezone info so we can safely compare with 'cutoff_date'
        if clean_df_full.index.tz is not None:
            clean_df_full.index = clean_df_full.index.tz_localize(None)

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
        if ticker in benchmark_map:
            ecb_key = benchmark_map[ticker]
            if ecb_key in ecb_files:
                logger.info(f"Triggering Benchmark Check: {ticker} vs {ecb_key}")
                df_ecb = pd.read_csv(ecb_files[ecb_key])

                recon_failures = check_with_benchmark(df_weekly, df_ecb)

                if not recon_failures.empty:
                    logger.warning(f"Found {len(recon_failures)} mismatches for {ticker} (Weekly View)")
                    # Add to report
                    quarantine_df_full = pd.concat([quarantine_df_full, recon_failures[['Close', 'qa_reason']]])
                    
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
                msg = "ML Anomaly: Price outside 95% Confidence Interval"
                logger.error(f"[ML ALERT] {msg} for {ticker}")

                # Add to Quarantine Report
                anomaly_row = pd.DataFrame([{
                    'Ticker': ticker,
                    'Close': 'Check Forecast',
                    'qa_reason': msg
                }])
                all_quarantine = pd.concat([all_quarantine, anomaly_row])

    # 6 Final Reports
    if not all_quarantine.empty:
        report_name = f"{data_folder}/QUARANTINE_REPORT_{datetime.now().strftime('%Y_%m_%d')}.csv"        
        all_quarantine.to_csv(report_name)

        # Only alert on New issues (Last 7 days)
        # Assumes index is Datetime If not skip filtering for now or need to set index
        # For safety in this script, just log the total count

        logger.error(f"Pipeline finished with {len(all_quarantine)} TOTAL issues. Report: {report_name}")
    else:
        logger.info("Pipeline SUCCESSFULLY. No data issues found.")

if __name__ == "__main__":
    run_automation()