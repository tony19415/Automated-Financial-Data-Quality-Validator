# Automated-Financial-Data-Quality-Validator
## Overview
This project is an end-to-end DataOps Pipeline that ingests, validates and forecasts financial market data (Equities, Forex, Crypto, etc). It automates the morning checks typically performed by data stewards in asset management frims.

Unlike simple scripts, this system features parallel processing, cross vendor validation (YahooFinance vs ECB) and Machine Learning anomaly detection using Meta's Prophet model.

---
## Architecture
The pipeline is orchestrated via GitHub Actions on a weekly scheduel (Mondays 07:00 UTC).

## Architecture Diagram
<img width="1440" height="663" alt="Image" src="https://github.com/user-attachments/assets/4245d4a3-f0ba-4b4b-aea1-89eb8b4885b4" />

## Key Features
- Parallel ingestion
- Quality Gate
- ECB Validation: Validates YahooFinance EUR/USD rates against the European Central Bank (ECB) official reference rates to detect vendor discrepancies
- AI/ML Forecasting:
    - Trains Prophet time series model on 2 years of historical data
    - Generates 30 day forecast with 95% Confidence interval
    - Auto Anomaly Detection, flags any current price that falls outside the model's predicted confidence interval
- Audit Trails, full logging implementation (Info/Error levels) replacing the standard print statement for production observability

## Project Structure
financial-data-validator/
├── .github/workflows/
│   └── daily_data_pipeline.yml  # CI/CD Orchestration (Weekly)
├── data/                        # CSVs, Logs, and Forecast Plots
├── src/
│   ├── fetch_data.py            # Ingestion Modules (Yahoo + ECB)
│   ├── validate_quality.py      # Logic Rules & Recon Engine
│   ├── forecast_analysis.py     # ML Model (Prophet)
│   └── run_pipeline.py          # Main Orchestrator
├── tests/
│   └── test_logic.py            # Pytest Unit Tests
├── config.yaml                  # Central Configuration (No hardcoding!)
├── requirements.txt             # Dependencies
└── README.md                    # Documentation

## Configuration
This project uses decoupled configuration pattern, you can modify tickers, history windows or benchmarks without touching the Python code.
Much safer when working with non technical staff for the organisation.

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

