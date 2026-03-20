import pytest
import pandas as pd
import numpy as np
from src.validate_quality import run_quality_checks, check_with_benchmark
from src.run_pipeline3 import sanitize_index

# Test 1 SQL Validation Logic
def test_duckdb_logic_error():
    """Test if DuckDB catches High < Low and volume <= 0"""
    # Create fake bad data
    data = {
        'Date': ['2026-01-01', '2026-01-02'],
        'High': [100, 50, 110],
        'Low': [90, 95, 105],
        'Close': [95, 92, 100],
        'Volume': [1000, 1000, -5]
    }
    df = pd.DataFrame(data).set_index(pd.to_datetime(data['Date']))

    clean_df, quarantine_df = run_quality_checks(df, "TEST_TICKER")

    assert len(clean_df) == 1
    assert len(quarantine_df) == 1
    assert "High < Low" in quarantine_df.iloc[0]['qa_reason']
    assert "Volume <= 0" in quarantine_df.iloc[1]['qa_reason']

# Test 2 Timezone & Column Sanitization
def test_sanitize_index_logic():
    """Verify ECB column renaming and timezone stripping"""
    data = {
        'TIME_PERIOD': ['2026-01-01 00:00:00+00:00'],
        'Close': [1.05]
    }
    df = pd.DataFrame(data)

    sanitized_df = sanitize_index(df, "EURUSD")

    # Check if 'TIME_PERIOD' was renamed to 'Date' and set as index
    assert sanitized_df.index.name == 'Date'
    # Check if timezone was stripped (Should be Naive)
    assert sanitized_df.index.tz is None
    assert isinstance(sanitized_df.index, pd.DatetimeIndex)

# Test 3 Benchmark Check
def test_check_benchmark_mismatch():
    """Verify that a >1% difference between Yahoo and ECB triggers a failure."""
    # Yahoo data
    y_data = {'Date': ['2026-01-01'], 'Close': [100.0]}
    df_y = pd.DataFrame(y_data).set_index(pd.to_datetime(y_data['Date']))

    # ECB Data (5% difference)
    e_data = {'Date': ['2026-01-01'], 'Close': [105.0]}
    df_e = pd.DataFrame(e_data).set_index(pd.to_datetime(e_data['Date']))

    failures = check_with_benchmark(df_y, df_e, threshold=0.01)

    assert not failures.empty
    assert "Benchmark Mismatch" in failures.iloc[0]['qa_reason']