import requests
import json
import os
import logging
import csv
from datetime import datetime

# === Settings ===
OUTPUT_DIR = "Raw Data Storage"
LOG_FILE = os.path.join(OUTPUT_DIR, "fetch_raw_nflx_data.log")
API_CREDENTIALS_PATH = "credentials/secretkey.json"
BASE_URL = 'https://www.alphavantage.co/query'

# Ensure output directory exists before logging setup
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Logging Setup ===
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE),   # Save logs to file
        logging.StreamHandler()          # Also print logs to console
    ]
)

# Load API key
with open(API_CREDENTIALS_PATH) as f:
    secrets = json.load(f)

API_KEY = secrets.get('ALPHAVANTAGE_API_KEY')
if not API_KEY:
    raise ValueError("API key not found in secretkey.json.")

def fetch_all_stock_data(symbol="NFLX"):
    """Fetch full historical daily stock price data and save as JSON, CSV, and logs."""
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'outputsize': 'full',
        'apikey': API_KEY
    }

    try:
        logging.info(f"Requesting full daily time series for {symbol}...")
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Check for API errors or rate limit messages
        if "Note" in data or "Error Message" in data:
            logging.error(f"API returned an error or rate limit message: {data}")
            return None

        if "Time Series (Daily)" not in data:
            logging.error(f"Unexpected response format: {data}")
            return None

        timeseries = data["Time Series (Daily)"]

        # === Save JSON ===
        json_path = os.path.join(OUTPUT_DIR, f"raw_{symbol.lower()}_all_data.json")
        with open(json_path, "w") as f:
            json.dump(timeseries, f, indent=4)

        # === Save CSV ===
        csv_path = os.path.join(OUTPUT_DIR, f"raw_{symbol.lower()}_all_data.csv")
        with open(csv_path, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "open", "high", "low", "close", "volume"])
            for date, values in sorted(timeseries.items()):
                writer.writerow([
                    date,
                    values["1. open"],
                    values["2. high"],
                    values["3. low"],
                    values["4. close"],
                    values["5. volume"]
                ])

        # === Summary ===
        dates = sorted(timeseries.keys())
        start_date = dates[0]
        end_date = dates[-1]
        total_days = len(dates)
        approx_years = total_days / 252  # trading days/year

        logging.info(f"Saved JSON: {json_path}")
        logging.info(f"Saved CSV: {csv_path}")
        logging.info(f"Logs saved to: {LOG_FILE}")
        logging.info(f"From {start_date} to {end_date} (~{approx_years:.2f} years)")

        return timeseries

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None

if __name__ == '__main__':
    fetch_all_stock_data(symbol="NFLX")
