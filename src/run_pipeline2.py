import yaml
import logging
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from fetch_data import download_ohlcv_to_csv, download_ecb_data
from validate_quality import load_data, run_quality_checks, check_with_benchmark


# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Configure Logging
logging.basicConfig(
    level=getattr(logging, config['pipeline']['setting']['log_level']),
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
    logger.info("--- Starting Weekly Data Pipeline ---\n")

    # 1 Dynamic Dates
    days_back = config['pipeline']['settings']['history_days']
    max_workers = config['pipeline']['settings']['max_workers']
    data_folder = config['pipeline']['settings']['data_folder']

    today = datetime.now()
    start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')

    logger.info(f"Time window: {start_date} to {end_date}")

    # 2 Yahoo Finance Data Ingestion
    yahoo_files = {}
    yahoo_tickers = config['pipeline']['yahoo_tickers']

    logger.info(f"Starting parallel download for {len(yahoo_tickers)} Yahoo tickers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_yahoo_download, t, start_date, end_date, data_folder): t for t in yahoo_tickers}

        for future in as_completed(futures):
            ticker, path = future.result()
            if path:
                yahoo_files[ticker] = path
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
                if path:
                    ecb_files[etick] = path

    # 4 Validation
    logger.info("Ingestion Complete. Starting Validation...")
    all_quarantine = pd.DataFrame()
    benchmark_map = config['pipeline']['benchmark_mapping']

    for ticker, file_path in yahoo_files.items():
        df = load_data(file_path)
        if df is None: continue

        # A. Standard checks
        clean_df, quarantine_df = run_quality_checks(df, ticker)

        # B. Benchmark check for EURUSD
        if ticker in benchmark_map:
            ecb_key = benchmark_map[ticker]
            if ecb_key in ecb_files:
                logger.info(f"Triggering Benchmark Check: {ticker} vs {ecb_key}")
                df_ecb = pd.read_csv(ecb_files[ecb_key])

                recon_failures = check_with_benchmark(clean_df, df_ecb)

                if not recon_failures.empty:
                    logger.warning(f"Found {len(recon_failures)} mismatches for {ticker}")
                    # Move failures from clean to quarantine
                    clean_df = clean_df.drop(recon_failures.index)

                    # Align columns for quarantine
                    recon_failures_clean = recon_failures[['Close', 'qa_reason']]
                    quarantine_df = pd.concat([quarantine_df, recon_failures_clean])
            else:
                logger.warning(f"Benchmark {ecb_key} missing for {ticker}")

        # Accumulate Failures
        if not quarantine_df.empty:
            quarantine_df['Ticker'] = ticker
            all_quarantine = pd.concat([all_quarantine, quarantine_df])
    
    # 5 Final reports
    if not all_quarantine.empty:
        report_name = f"{data_folder}/QUARANTINE_REPORT_{datetime.now().strftime('%Y_%m_%d')}.csv"        
        all_quarantine.to_csv(report_name)
        logger.error(f"Pipeline finished with ISSUES. Report: {report_name}")
    else:
        logger.info("Pipeline SUCCESSFULLY. No data issues found.")

if __name__ == "__main__":
    run_automation()