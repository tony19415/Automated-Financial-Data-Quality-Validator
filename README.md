# Automated-Financial-Data-Quality-Validator
Pipeline that ingests daily market data, runs automated quality checks and quarantine bad data before it hits dashboard

## Progress:
- Created custom python script to download data from yfinance into csv
- Identify: null values, when high is lower than low, when volume is less than 0

## To do:
- Script to identify data quality
- Save clean data into csv
- Visualize data in PowerBI dashboard