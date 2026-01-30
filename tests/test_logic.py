import pytest
import pandas as pd
from src.validate_quality import run_quality_checks

def test_high_low_logic_check():
    # Does the function correctly identify that High < Low is bad?

    # Create fake bad data
    df_bad = pd.DataFrame({
        'Open': [180], 'High': [90], 'Low': [95], 'Close': [100], 'Volume': [100]
    })

    # Run function
    clean, quarantine = run_quality_checks(df_bad, "TEST_TICKER")

    # Code should flag this row
    assert len(quarantine) == 1
    assert "Logic Error" in quarantine.iloc[0]['qa_reason']