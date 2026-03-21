import pandas as pd
import duckdb
import logging

logger = logging.getLogger("QualityValidator")

def load_data(file_path):
    """
    Load CSV data into a Pandas DataFrame.
    Assume 'Date' is present and parse it
    """

    if not file_path:
        return None
    try:
        # Load raw data
        df = pd.read_csv(file_path)

        # Standardize Data Column
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)

        return df
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return None
    
def run_quality_checks(df, ticker_name):
    """
    Use DuckDB (SQL) to validate data logic
    Returns: (clean_df, quarantine_df)
    """
    try:
        # 1. Prepare DuckDB
        df_flat = df.reset_index()
        con = duckdb.connect(database=':memory:')
        con.register('market_data', df_flat)

        # 2. Define SQL rules
        query_bad_rows = """
        SELECT Date, Close, 'Logic Error: High < Low' as qa_reason
        FROM market_data
        WHERE High < Low

        UNION ALL

        SELECT Date, Close, 'Missing Value: Close' as qa_reason
        FROM market_data
        WHERE Close IS NULL OR High IS NULL OR Low IS NULL        
        """

        # Execute Query
        quarantine_df = con.execute(query_bad_rows).fetchdf()

        # 3. Filter Clean Data
        if not quarantine_df.empty:
            logger.warning(f"DuckDB found {len(quarantine_df)} logical errors in {ticker_name}")

            # Get list of bad dates to exclude
            bad_dates = pd.to_datetime(quarantine_df['Date']).tolist()
            clean_df = df[~df.index.isin(bad_dates)].copy()

            # Format Quarantine DF
            quarantine_df.set_index('Date', inplace=True)
        else:
            clean_df = df.copy()

        return clean_df, quarantine_df
    
    except Exception as e:
        logger.error(f"DuckDB Validation failed for {ticker_name}: {e}")
        return df, pd.DataFrame()

def check_with_benchmark(df_target, df_benchmark, threshold=0.01):
    """
    Use DuckDB to JOIN and compare target vs benchmark
    threshold=0.01 means 1% difference triggers an alert
    """
    try:
        # 1. Prepare DuckDB
        t_flat = df_target.reset_index()
        b_flat = df_benchmark.reset_index()

        con = duckdb.connect(database=':memory:')
        con.register('target', t_flat)
        con.register('benchmark', b_flat)

        # 2. SQL Check
        query_recon = f"""
        SELECT
            t.Date,
            t.Close,
            'Benchmark Mismatch > {threshold*100}%' as qa_reason
        FROM target t
        INNER JOIN benchmark b ON t.Date = b.Date
        WHERE ABS((t.Close - b.Close) / b.Close) > {threshold}        
        """

        failures = con.execute(query_recon).fetchdf()

        if not failures.empty:
            failures.set_index('Date', inplace=True)
            return failures
        
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"DuckDB Reconciliation failed: {e}")
        return pd.DataFrame()