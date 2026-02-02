import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
from datetime import datetime
import os
import logging

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Forecaster")

def generate_forecast(file_path, ticker, periods=30, confidence_interval=0.95):
    """
    Docstring for generate_forecast
    
    1. Train Prophet model on data
    2. Forecast 30 days ahead
    3. Check: Does the latest actual data point fall inside the predicted range?
    """

    # 1. Load data
    try:
        df = pd.read_csv(file_path)
        # Rename columns for Prophet model: Date -> ds, Close -> y
        # Assume first column is the Date index
        df = df.rename(columns={df.columns[0]: 'ds', 'Close': 'y'})

        # Ensure timezone-naive
        df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)
    except Exception as e:
        logger.error(f"Could not load data for {ticker}: {e}")
        return None, False
    
    # 2. Train Model
    # interval_width=0.95 means there's 95% probability of the value being in this range
    # Value outisde this is the 5% probability event which is a potential anomaly
    model = Prophet(daily_seasonality=True, interval_width=confidence_interval)
    model.fit(df)

    # 3. Create Future Dates (History + 30 Days)
    future = model.make_future_dataframe(periods=periods)

    # 4. Predict
    forecast = model.predict(future)

    # Merge actuals (df) with Predictions (forecast) on Date (ds)
    # Compare 'y' (Actual) vs 'yhat_lower'/'yhat_upper' (Predicted)
    merged = pd.merge(df, forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], on='ds')

    # Check the Latest available data point
    latest_row = merged.iloc[-1]
    last_date = latest_row['ds'].strftime('%Y-%m-%d')
    actual_price = latest_row['y']
    lower_bound = latest_row['yhat_lower']
    upper_bound = latest_row['yhat_upper']

    is_anomaly = False
    anomaly_msg = "Normal"

    # Is actual outside the bounds?
    if actual_price < lower_bound or actual_price > upper_bound:
        is_anomaly = True
        anomaly_msg = f"ANOMALY DETECTED! {last_date}: Price {actual_price:.4f} is outside range [{lower_bound:.4f}, {upper_bound:.4f}]"
        logger.warning(f"[{ticker}] {anomaly_msg}")
    else:
        logger.info(f"[{ticker}] Reality Check Passed. {last_date}: {actual_price:.4f} is within model range.")

    # 5. Save Forecast Data
    forecast_date = datetime.now().strftime('%Y_%m_%d')
    output_csv = f"data/{ticker}_forecast_{forecast_date}.csv"
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(periods).to_csv(output_csv, index=False)

    # 6. Plot & Save Image
    fig1 = model.plot(forecast)
    plt.title(f"{ticker} Forecast (Anomaly: {is_anomaly})")

    # Highlight the anomaly on the chart if it exists
    if is_anomaly:
        plt.scatter(latest_row['ds'], actual_price, color='red', s=50, label='Anomaly', zorder=5)
        plt.legend()
    
    img_path = f"data/{ticker}_{forecast_date}_forecast_plot.png"
    plt.savefig(img_path)
    logger.info(f"Forecast plot saved to {img_path}")

    return img_path, is_anomaly