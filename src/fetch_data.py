import yfinance as yf
import pandas as pd
import os
import logging
from ecbdata import ecbdata

# Initialize Logger
logger = logging.getLogger(__name__)

def download_ohlcv_to_csv(ticker, start_date, end_date, dinterval, output_folder='data'):
    # Download OHLCV data from Yahoo Finance

    logger.info(f"Starting Download for {ticker} ({start_date} to {end_date})")

    try:
        df = yf.download(ticker, start=start_date, end=end_date, interval=dinterval, auto_adjust=False, progress=False)

        if df.empty:
            logger.warning(f"No data found for {ticker}")
            return None
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        df = df[[c for c in required_cols if c in df.columns]]

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        safe_ticker = ticker.replace("=X", "")
        filename = f"{output_folder}/{safe_ticker}_{start_date}_{end_date}_{dinterval}.csv"

        df.to_csv(filename)
        logger.info(f"Successfully saved {ticker} to {filename}")

        return filename
    
    except Exception as e:
        logger.error(f"Failed to download {ticker}: {e}")
        return None
    
def download_ecb_data(etick, start_date, end_date, output_folder="data"):
    # Download from ECB
    logger.info(f"Starting ECB Download for {etick}")

    try:
        dft = ecbdata.get_series(etick, start=start_date, end=end_date)

        if dft.empty:
            logger.warning(f"No ECB data found for {etick}")
            return None
        
        dft['TIME_PERIOD'] = pd.to_datetime(dft['TIME_PERIOD'])

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        filename = f"{output_folder}/{etick}_{start_date}_{end_date}.csv)"
        dft.to_csv(filename)

        logger.info(f"Successfully saved ECB data {etick} to {filename}")
        return filename
    
    except Exception as e:
        logger.error(f"Failed to download ECB {etick}: {e}")
        return None