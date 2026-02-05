# Automated-Financial-Data-Quality-Validator
## Overview
This project is an end-to-end DataOps and MLOps Pipeline that ingests, validates and forecasts financial market data (Equities, Forex, Crypto, etc). It automates the morning checks typically performed by data stewards in asset management firms.

Unlike simple scripts, this system features parallel processing, cross vendor validation (YahooFinance vs ECB) and Machine Learning anomaly detection using Meta's Prophet model.

MLflow is integrated to track model drift (MAPE), log parameters and for version control forecast artifacts

The architecture is set up so that from a single download the pipeline will serve both the deep learning model with 2 years of data and business analysts with a 7 day view.

---
## Architecture
The pipeline is orchestrated via GitHub Actions on a weekly scheduel (Mondays 07:00 UTC).

## Architecture Diagram
<img width="1440" height="663" alt="Image" src="https://github.com/user-attachments/assets/c4c022aa-949f-48c7-8f74-8db702c2dbe1" />

## Key Features
- Parallel ingestion
- Quality Gate
- ECB Validation: Validates YahooFinance EUR/USD rates against the European Central Bank (ECB) official reference rates to detect vendor discrepancies
- AI/ML Forecasting:
    - Trains Prophet time series model on 2 years of historical data
    - Generates 30 day forecast with 95% Confidence interval
    - Auto Anomaly Detection, flags any current price that falls outside the model's predicted confidence interval
- Audit Trails, full logging implementation (Info/Error levels) replacing the standard print statement for production observability
- MLOps Experiment tracking
    - Tracks MAPE (Mean Absolute Percentage Error) for every run
    - Detects drifts by allowing us to see if the model's accuracy is degrading as market conditions change
    - Automatically saves forecast plots and parameters for every experiment

## Project Structure
```
financial-validator/
├── .github/workflows/
│   └── weekly_pipeline.yml      # CI/CD Orchestration
├── data/                        # Clean CSVs, Forecast Plots, & Reports
├── mlruns/                      # MLflow Experiment Logs
├── src/
│   ├── fetch_data.py            # Parallel Download Engine
│   ├── validate_quality.py      # DuckDB SQL Logic
│   ├── forecast_analysis.py     # Prophet + MLflow Engine
│   └── run_pipeline.py          # Main Orchestrator
├── Dockerfile                   # Container Definition
├── config.yaml                  # Central Configuration
├── requirements.txt             # Dependencies
└── README.md                    # Documentation
```

## Configuration
This project uses decoupled configuration pattern, you can modify tickers, history windows, benchmarks, ML settings without touching the Python code.
Much safer when working with non technical staff for the organisation.

```
pipeline:
  settings:
    log_level: "INFO"
    max_workers: 5         # Concurrency level
    history_days: 730      # 2 Years (Required for ML Seasonality)
  
  yahoo_tickers:
    - "AAPL"
    - "EURUSD=X"
    - "BTC-USD"
    
  ecb_tickers:
    - "EXR.D.USD.EUR.SP00.A" # Daily USD/EUR Reference Rate
  
  # MLflow Settings
  mlflow:
    experiment_name: "Market_Forecasts_Production"
    tracking_uri: "mlruns"

  # Assets to Forecast
  ml_tickers:
    - "EURUSD=X"
    - "BTC-USD"
```

## Machine Learning Module
Pipeline uses Meta's Prophet model to add a statistical layer to data validation
- Training Data: 730 Days of Close prices
- Seasonality: Captures weekly and yearly trends
- Anomaly logic:
    - If Actual Price > Predicted_Upper_Bound OR Actual Price < Predicted_Lower_Bound
    - Action: Row is flagged as an "Anomaly" and added into the Quarantine Report automatically for manual checks

## CI/CD Pipeline
The project is fully automated using GitHub Actions
- Schedule is set currently to run every Monday at 07:00 or manually
- Checkout -> Install Depositories -> Run Tests (pytest) -> Run Pipeline -> Upload Artifacts (Held for 5 days)
- All CSV reports, logs and forecast charts are available for download in the "Actions" tab after every run


## Progress:
- Created custom python script to download data from yfinance into csv
- Identify: null values, when high is lower than low, when volume is less than 0
- CI/CD via GitHub Actions where the data gets automatically quarantined for manual analyst review

## To do:
- Visualize data in PowerBI dashboard

