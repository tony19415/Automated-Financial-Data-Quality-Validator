import pandas as pd
import numpy as np
import os

def load_data(filepath):
    try:
        df = pd.read_csv(filepath, index_col=0)
        
        df.index = pd.to_datetime(df.index, utc=True)

        return df
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return None
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def run_quality_checks(df, ticker_name):
    print(f"Running Quality Assurance on {ticker_name}...")
    
    # Create a copy to avoid problems
    df_qc = df.copy()

    # Initiate error tracking
    df_qc['qa_flag'] = False
    df_qc['qa_reason'] = ""

    # Convert volume to int
    df_qc['Volume'] = pd.to_numeric(df_qc['Volume']).astype('Int64')

    # Check for logical results
    # High price must be >= Low Price
    mask_logic_error = df_qc['High'] < df_qc['Low']
    df_qc.loc[mask_logic_error, 'qa_flag'] = True
    df_qc.loc[mask_logic_error, 'qa_reason'] += "Logic Error (High < Low)"

    # Volume can't be negative
    mask_vol_error = df_qc['Volume'] < 0
    df_qc.loc[mask_vol_error, 'qa_flag'] = True
    df_qc.loc[mask_vol_error, 'qa_reason'] += "Negative Volume"

    # Check for null values
    mask_nulls = df_qc[['Open', 'High', 'Low', 'Close']].isnull().any(axis=1)
    df_qc.loc[mask_nulls, 'qa_flag'] = True
    df_qc.loc[mask_nulls, 'qa_reason'] += "Missing Values"

    # Check for anomaly in daily price
    df_qc['daily_change_pct'] = abs((df_qc['Close'] - df_qc['Open']) / df_qc['Open'])

    mask_anomaly = df_qc['daily_change_pct'] > 0.20
    df_qc.loc[mask_anomaly, 'qa_flag'] = True
    df_qc.loc[mask_anomaly, 'qa_reason'] += "Price Anomaly (>20% Swing)"


    # Split data
    quarantine_df = df_qc[df_qc['qa_flag'] == True].copy()
    clean_df = df_qc[df_qc['qa_flag'] == False].copy()

    # Remove the qa_flag, qa_reason, daily_change_pct from clean data
    clean_df.drop(columns=['qa_flag', 'qa_reason', 'daily_change_pct'], inplace=True)

    return clean_df, quarantine_df

def check_with_benchmark(df_target, df_benchmark, threshold=0.01):
    # Compare target data (yahoo) with benchmark (ECB)
    # Flag rows where difference > threshold in this case 1%

    print("Running external benchmark check")

    # Check for the TIME_PERIOD column of ECB
    time_col = next((col for col in df_benchmark.columns if col.startswith('TIME')), None)

    if time_col:
        df_benchmark[time_col] = pd.to_datetime(df_benchmark[time_col])
        df_benchmark.set_index(time_col, inplace=True)
    
        # Convert to UTC
        if df_benchmark.index.tz is None:
            # If there are no timezone then assign to UTC
            df_benchmark.index = df_benchmark.index.tz_localize('UTC')
        else:
            # If there are no timezone then assign to 
            df_benchmark.index = df_benchmark.index.tz_convert('UTC')

    # Merge on Date Index
    combined = pd.merge(
        df_target[['Close']],
        df_benchmark[['OBS_VALUE']],
        left_index=True,
        right_index=True
    )

    # Calculate % Difference
    combined['diff_pct'] = abs((combined['Close'] - combined['OBS_VALUE']) / combined['OBS_VALUE'])

    # Find inconcistencies
    discrepancies = combined[combined['diff_pct']>threshold].copy()

    if not discrepancies.empty:
        discrepancies['qa_reason'] = f"Discrepancy Error (Diff > {threshold*100}%)"

    return discrepancies

if __name__ == "__main__":
    df_yf = load_data('data/EURUSD_2024-01-01_to_2026-01-28_1d.csv')
    
    # Run data quality checks
    clean_data, quarantine_data = run_quality_checks(df_yf, "EURUSD=X")

    # Run check_with_benchmarks

    df_ecb = load_data('data/EURUSD_ecb.csv')

    recon_failures = check_with_benchmark(clean_data, df_ecb)

    # If check with benchmark failes, move rows from clean to quarantine
    if not recon_failures.empty:
        print(f"Found {len(recon_failures)} discrepancy errors")
        # remove from clean
        clean_data = clean_data.drop(recon_failures.index)
        # Add to quarantine
        quarantine_data = pd.concat([quarantine_data, recon_failures])

    # Report
    print("\n" + "="*30)
    print("Final data quality report")
    print("="*30)
    print(f"Total Rows Processed: {len(df_yf)}")
    print(f"Rows Passed (Clean):  {len(clean_data)}")
    print(f"Rows Quarantine:      {len(quarantine_data)}")

    if not quarantine_data.empty:
        print("\n--- Quarantine Details ---")
        print(quarantine_data[['Close', 'qa_reason']])

        # Save to CSV
        quarantine_data.to_csv("quarantine_report.csv")
        print("\n[Action] Quarantine report saved to 'quarantine_report.csv'")