# Project: Automated Financial Data Quality Validator

The document outlines the critical technical hurdles encountered during the development of the production grade dataops pipeline and the architectural decisions made to resolve them
---

## 1. Infrastructure & Environment

| Error                                | Root Cause                                                                    | Resolution                                                                                               |
|--------------------------------------|-------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| Docker: Invalid Reference Format     | Shell-specific syntax mismatch for the pwd command on Windows vs. Linux.      | Switched to ${PWD} for PowerShell and implemented quotes around volume paths to handle directory spaces. |
| Docker: Daemon Not Running           | Docker Desktop engine was not initialized or WSL 2 integration was disabled.  | Initialized Docker Desktop and verified "Engine Running" status before executing builds                  |
| ModuleNotFoundError (GitHub Actions) | Python's sys.path did not include the /src directory during Pytest execution. | Implemented a dynamic path injection in test_pipeline.py using sys.path.insert(0, os.path.abspath(...)). |

---

## 2. Data Ingestions & Integrity

| Error                                            | Root Cause                                                                               | Resolution                                                                                                 |
|--------------------------------------------------|------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| TypeError: 'NoneType' object is not subscriptabl | yfinance API version mismatch due to upstream changes in Yahoo Finance's data structure. | Executed pip install --upgrade yfinance and cleared the local cache/ directory to force a fresh handshake. |
| Data Poisoning (BTC in EURUSD)                   | Local file naming collision or variable mismatch during early testing phases.            | Implemented a Data Guard in the sanitization layer to trip the Circuit Breaker if FX prices exceed $5,000. |
| ECB Connection Timeout                           | Network latency or server-side rate-limiting on the European Central Bank API            | Implemented a timeout parameter in the requests call and documented the need for a Retry Decorator.        |

---

## 3. Logic & Validation (DuckDB / Pandas)

| Error                                | Root Cause                                                                           | Resolution                                                                                      |
|--------------------------------------|--------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| Pandas FutureWarning (isin)          | Dtype mismatch between DuckDB (String/Object) and Pandas Index (Datetime64).         | Explicitly cast DuckDB outputs using pd.to_datetime() before performing index-based filtering.  |
| DuckDB Binder Error (Missing Column) | Source data from ECB used OBS_VALUE while Yahoo used Close.                          | Standardized the schema in the sanitize_index function before the data reached the SQL engine.  |
| Unit Scaling (1128.00 vs 1.128)      | Yahoo Finance occasionally quotes FX pairs in pips/points instead of standard rates. | Added a Unit Normalizer that detects mean > 100 for EURUSD and automatically divides by $1000$. |

---

## 4. MLOps & Observability

| Error                      | Root Cause                                                                           | Resolution                                                                                 |
|----------------------------|--------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| MLflow Deprecation Warning | Filesystem tracking (./mlruns) is deprecated as of Feb 2026.                         | Migrated the tracking backend to a local SQLite database using sqlite:///mlflow.db.        |
| Git SHA Missing (Docker)   | python:3.10-slim image lacks the git binary required by MLflow for version tracking. | Updated the Dockerfile to include apt-get install -y git in the system dependencies layer. |

---

## Statistical Integirty Note

The pipeline tracks accuracy using Mean Absolute Percentage Error (MAPE):$$MAPE = \frac{100\%}{n} \sum_{t=1}^{n} \left| \frac{A_t - F_t}{A_t} \right|$$The Circuit Breaker is designed to trip if the Failure Rate ($FR$) exceeds the threshold ($T$):$$FR = \frac{Assets_{Failed}}{Assets_{Total}} \geq T$$