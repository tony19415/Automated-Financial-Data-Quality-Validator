import yfinance as yf
import pandas as pd
import os
from ecbdata import ecbdata

# print(msft.financials)

#print(msft.balancesheet)

# daily_data = yf.download("AAPL", start="2026-01-01", end="2026-01-21", interval="1d")
# hourly_data = yf.download("SPY", start="2026-01-01", end="2026-01-21", interval="1h")

# print(hourly_data.head())


def download_ohlcv_to_csv(ticker, start_date, end_date, dinterval, output_folder="data"):
    print(f"--- Starting Download for {ticker} ---")
    print(f"Period: {start_date} to {end_date} | Interval: {dinterval}")

    # Download the data
    df = yf.download(ticker, start=start_date, end=end_date, interval=dinterval, auto_adjust=False, progress=False)

    # Raise error if data can't be downloaded
    if df.empty:
        raise ValueError (f"No data fround for {ticker}. Check your dates or ticker symbol.")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    available_cols = [col for col in required_cols if col in df.columns]
    df = df[available_cols]

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    safe_ticker = ticker.replace("=X", "")
    filename = f"{output_folder}/{safe_ticker}_{start_date}_to_{end_date}_{dinterval}.csv"
    df.to_csv(filename)

    print(f"Success! Data saved to: {filename}")
    print(f"Rows downloaded: {len(df)}")
    print("-" * 30)

def download_ecb_data(etick, start_date, end_date, output_folder="data"):
    print(f"---Starting Download for {etick} ---")
    print(f"Period: {start_date} to {end_date}")

    dft = ecbdata.get_series(etick, start=start_date, end=end_date)


    # Raise error if data can't be downloaded
    if dft.empty:
        raise ValueError (f"No data fround for {etick}. Check your dates or ticker symbol.")

    dft['TIME_PERIOD'] = pd.to_datetime(dft['TIME_PERIOD'])

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    filename = f"{output_folder}/{etick}_{start_date}_to_{end_date}.csv"
    dft.to_csv(filename)

    print(f"Success! Data saved to: {filename}")
    print(f"Rows downloaded: {len(dft)}")
    print("-" * 30)

# if __name__ == "__main__":
#     try:
#         download_ohlcv_to_csv(
#             ticker="AAPL",
#             start_date="2024-11-01",
#             end_date="2026-01-21",
#             dinterval="1h"
#         )

#         download_ohlcv_to_csv(
#             ticker="SPY",
#             start_date="2024-11-01",
#             end_date="2026-01-21",
#             dinterval="1h"
#         )

#         download_ohlcv_to_csv(
#             ticker="EURUSD=X",
#             start_date="2024-11-01",
#             end_date="2026-01-21",
#             dinterval="1h"
#         )

#         # Test the pipeline error intentionally
#         download_ohlcv_to_csv(
#             ticker="INVALID_TICKER_XYZ",
#             start_date="2024-11-01",
#             end_date="2024-12-01",
#             dinterval="1h"
#         )

#     except ValueError as e:
#         print(f"Critical Pipeline failure: {e}")

if __name__ == "__main__":
    # List of tickers to download
    download_ticks = [
        # "ASML.AS",
        # "UNA.AS",
        "EURUSD=X",
        # "TLT",
        # "HYG",
        # "ICLN",
        # "VNQ",
        # "GLD",
        # "EEM",
        "BTC-USD"
    ]

    ecb_tick = [
        "EXR.D.USD.EUR.SP00.A"
    ]

    print(f"Starting Batch Download for {len(download_ticks)} Assets...\n")

    for ticker in download_ticks:
        try:
            download_ohlcv_to_csv(
                ticker=ticker,
                start_date="2024-01-01",
                end_date="2026-01-30",
                dinterval="1d"
            )
        except ValueError as e:
            print(f"Skipping {ticker}: {e}")

    for tick in ecb_tick:
        try:
            download_ecb_data(
                etick=tick,
                start_date="2024-01-01",
                end_date="2026-01-30"
            )
        except ValueError as e:
            print(f"Skipping {tick}: {e}")

    print("\nBatch Download Complete.")