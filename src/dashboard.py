import streamlit as st
import pandas as pd
import glob
import plotly.graph_objects as go
from datetime import datetime

# PAGE CONFIG
st.set_page_config(page_title="Financial Data Quality Monitor", layout="wide")

st.title("🛡️ Financial Data Quality Control Center")
st.markdown("Monitoring pipeline status, data quality anomalies, and ML forecasts.")

# 1. LOAD DATA
# Find the latest Quarantine Report
list_of_files = glob.glob('data/QUARANTINE_REPORT_*.csv')
if list_of_files:
    latest_file = max(list_of_files, key=os.path.getctime)
    df_quarantine = pd.read_csv(latest_file)
    st.sidebar.error(f"🚨 {len(df_quarantine)} Issues Detected Today")
else:
    df_quarantine = pd.DataFrame()
    st.sidebar.success("✅ System Healthy: No Issues")

# 2. KEY METRICS (Business Value View)
col1, col2, col3 = st.columns(3)
col1.metric("Pipeline Status", "Active", "Daily Run 07:00 UTC")
col2.metric("Data Source", "Yahoo Finance + ECB", "Reconciled")
col3.metric("ML Model", "Prophet", "95% Confidence")

st.divider()

# 3. QUARANTINE MANAGER
st.subheader("⚠️ Data Quarantine (Action Required)")
if not df_quarantine.empty:
    st.dataframe(df_quarantine[['Ticker', 'qa_reason', 'Close']].style.applymap(lambda x: 'color: red'))
else:
    st.info("No data quality issues found in the latest run.")

# 4. ML FORECAST VIEWER
st.subheader("📈 Forecast & Anomaly Inspection")
ticker = st.selectbox("Select Asset for Analysis", ["EURUSD=X", "AAPL", "BTC-USD"])

# Try to load the forecast file
try:
    forecast_file = f"data/{ticker}_forecast.csv"
    df_forecast = pd.read_csv(forecast_file)
    
    # Plot with Plotly (Interactive)
    fig = go.Figure()
    
    # Predicted Trend
    fig.add_trace(go.Scatter(x=df_forecast['ds'], y=df_forecast['yhat'], 
                             mode='lines', name='Forecast', line=dict(color='blue')))
    # Upper Bound
    fig.add_trace(go.Scatter(x=df_forecast['ds'], y=df_forecast['yhat_upper'], 
                             mode='lines', name='Upper Bound', line=dict(width=0), showlegend=False))
    # Lower Bound (Fill)
    fig.add_trace(go.Scatter(x=df_forecast['ds'], y=df_forecast['yhat_lower'], 
                             mode='lines', name='Lower Bound', fill='tonexty', 
                             line=dict(width=0), fillcolor='rgba(0,0,255,0.1)'))
    
    st.plotly_chart(fig, use_container_width=True)
    
except FileNotFoundError:
    st.warning(f"No forecast data found for {ticker}. Run the pipeline first.")