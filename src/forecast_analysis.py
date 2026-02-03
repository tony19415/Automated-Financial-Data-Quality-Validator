import pandas as pd
from prophet import Prophet
from prophet.diagnostics import performance_metrics
import matplotlib.pyplot as plt
import yaml
import mlflow
from datetime import datetime
import os
import logging
from sklearn.metrics import mean_absolute_error

# Load Config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Forecaster")

# Setup MLflow
mlflow_config = config['pipeline'].get('mlflow', {})
MLFLOW_tracking_URI = mlflow_config.get('tracking_uri', 'mlruns')
EXP_NAME = mlflow_config.get('experiment_name', 'Default_Experiment')

mlflow.set_tracking_uri('MLFLOW_tracking_URI')
mlflow.set_experiment(EXP_NAME)

def generate_forecast(file_path, ticker):
    """
    Docstring for generate_forecast
    
    1. Train Prophet model on data
    2. Forecast 30 days ahead
    3. Check: Does the latest actual data point fall inside the predicted range?
    4. Log everything to MLflow
    """

    # 1. Load data
    try:
        df = pd.read_csv(file_path)
        if 'Date' not in df.columns:
            logger.error(f"Missing 'Date' column in {file_path}")
            return None, False
        
        # Prophet needs columns 'ds' as Date and 'y' as Value
        df = df.rename(columns={'Date': 'ds', 'Close': 'y'})
        df['ds'] = pd.to_datetime(df['ds'])
        
        # Strip timezone if present
        if df['ds'].dt.tz is not None:
            df['ds'] = df['ds'].dt.tz_localize(None)

        # Start MLFLOW RUN
        with mlflow.start_run(run_name=f"Forecast_{ticker}"):

            # A. Define Hyperperameters
            interval_width = 0.95
            daily_seasonality = True

            # B. Log Params
            mlflow.log_param("ticker", ticker)
            mlflow.log_param("model_type", "Prophet")
            mlflow.log_param("interval_width", interval_width)
            mlflow.log_param("history_len", len(df))

            # C. Train model
            m = Prophet(interval_width=interval_width, daily_seasonality=daily_seasonality)
            m.fit(df)

            # D. Make Future Dataframe (30 Days)
            future = m.make_future_dataframe(periods=30)
            forecast = m.predict(future)

            # Calculate In Sample Metric (MAE)
            # Compre actual 'y' vs predcited 'yhat' for the historical dates
            metric_df = forecast.set_index('ds')[['yhat']].join(df.set_index('ds')[['y']], how='inner')
            mae = mean_absolute_error(metric_df['y'], metric_df['yhat'])

            # Log Metric How good is the fit?
            mlflow.log_metric("mae", mae)
            logger.info(f"Model trained for {ticker}. In-sample MAE: {mae:.4f}")

            # F. Anomaly Detection Logic
            # Check if latest actual price is inside the confidence interval
            latest_actual = df.iloc[-1]
            latest_date = latest_actual['ds']
            latest_price = latest_actual['y']

            # Find prediction for that specific date
            pred = forecast[forecast['ds'] == latest_date].iloc[0]
            lower = pred['yhat_lower']
            upper = pred['yhat_upper']

            is_anomaly = False
            if latest_price < lower or latest_price > upper:
                is_anomaly = True
                mlflow.set_tag("anomaly_detected", "true")
                anomaly_msg = f"ANOMALY DETECTED! {latest_date}: Price {latest_price:.4f} is outside range [{lower:.4f}, {upper:.4f}]"
                logger.warning(f"[{ticker}] {anomaly_msg}")
            
            # G. Generate & Save Plot
            fig1 = m.plot(forecast)
            plt.title(f"Forecast for {ticker} (MAE: {mae:.2f})")

            # Save locally first
            plot_filename = f"{ticker}_forecast_plot.png"
            plot_path = os.path.join(config['pipeline']['settings']['data_folder'], plot_filename)
            fig1.savefig(plot_path)
            logger.info(f"Forecast plot saved to {plot_path}")

            # H. Log Artifact (Upload plot to MLflow)
            mlflow.log_artifact(plot_path)

            # Optional, save full model (heavy but can be useful)
            # mlflow.prophet.log_model(m, artifact_path="model")

            return plot_path, is_anomaly
    
    except Exception as e:
        logger.error(f"ML Forecasting failed for {ticker}: {e}")
        return None, False