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
<img width="1440" height="663" alt="Image" src="https://github.com/user-attachments/assets/7ff2b954-2e41-46c5-bae2-ab61e5f9249c" />

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
│   ├── validate_quality.py      # Validating data quality
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

## Data Governance & Reliability
1. Circuit Breaker Pattern
To prevent "Data Poisoning", system calculates real time Failure Rate. If the percentage of corrupted or anomalous assets exceeds the failure_threshold (default: 50%) the system executes an emergency sys.exit(1).

2. DuckDB SQL Validation
Instead of slow row by row iteration, pipeline uses DuckDB for vectorized SQL validation. Allowing complex checks (verifying High >= Low) at fast speeds directly in memory

3. Self Healing Unit Normalizer
Recognizing real world API inconsistencies, pipeline includes a harmonization layer that detects scale anomalies and automatically normalizes data before storage.

## Setup and Installation

1. Clone the repository:
```
git clone https://github.com/yourusername/financial-validator.git
```

2. Run with Docker
```
docker build -t financial-validator .
docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/mlruns:/app/mlruns" financial-validator
```
3. Inspect Logs & Metrics
```
mlflow ui --backend-store-uri sqlite:///mlflow.db
```

## Incident & Resolution Log
Full history of technical hurdles such as handling yfinance API shifts, Docker volume pathing on Windows is documented in the TECHNICAL_DOC.md