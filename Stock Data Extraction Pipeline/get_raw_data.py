import requests
import json
from datetime import datetime, timedelta

# Load API key from JSON file
with open('secretkey.json') as f:
    secrets = json.load(f)

API_KEY = secrets.get('ALPHAVANTAGE_API_KEY')
if not API_KEY:
    raise ValueError("API key not found in secrets.json.")

BASE_URL = 'https://www.alphavantage.co/query'

params = {
    'function': 'TIME_SERIES_DAILY',
    'symbol': 'XAUUSD',
    'outputsize': 'full',
    'apikey': API_KEY
}

def fetch_last_7_years_gold_data():
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if "Time Series (Daily)" not in data:
            print("Unexpected response format or error:", data)
            return

        timeseries = data["Time Series (Daily)"]
        seven_years_ago = datetime.today() - timedelta(days=7*365)

        filtered_data = {
            date: info
            for date, info in timeseries.items()
            if datetime.strptime(date, "%Y-%m-%d") >= seven_years_ago
        }

        with open("gold_last_7_years.json", "w") as f:
            json.dump(filtered_data, f, indent=4)

        print(f"Saved {len(filtered_data)} daily records from the last 7 years.")

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")

if __name__ == '__main__':
    fetch_last_7_years_gold_data()
