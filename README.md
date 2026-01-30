# Automated-Financial-Data-Quality-Validator
Pipeline that ingests daily market data, runs automated quality checks and quarantine bad data before it hits dashboard

## Progress:
- Created custom python script to download data from yfinance into csv
- Identify: null values, when high is lower than low, when volume is less than 0
- CI/CD via GitHub Actions where the data gets automatically quarantined for manual analyst review

## To do:
- Visualize data in PowerBI dashboard

## Architecture Diagram
<img width="1440" height="663" alt="Image" src="https://github.com/user-attachments/assets/4245d4a3-f0ba-4b4b-aea1-89eb8b4885b4" />