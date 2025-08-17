import pandas as pd
import numpy as np
import requests as r
import yfinance as yf
# WORLD COIN INDEX (COIN/BTC)

wci_key = 'X1rWsSfF4Rm2zgixd0vjT5cuDq4YuxCmijH'
wci_url = f"https://www.worldcoinindex.com/apiservice/v2getmarkets?key={wci_key}&fiat=btc"

response = r.get(wci_url)
data = response.json()
wci_df = pd.DataFrame(data['Markets'][0])
#print(wci_df.info()) #24h volume data and price for each coin

# COINGecko (COIN/USD)

# CoinGecko free API endpoint (no API key needed)
cg_url = "https://api.coingecko.com/api/v3/coins/markets"

all_data = []
page = 1
max_pages = 10  # Add a limit to prevent infinite loops

while page <= max_pages:
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",  # sort by market cap
        "per_page": 250,             # max coins per page
        "page": page,                # Use the page variable here!
        "sparkline": False
    }

    try:
        response = r.get(cg_url, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
    except r.exceptions.RequestException as e:
        print(f"Error fetching data on page {page}: {e}")
        break

    if not data:
        print(f"No more data after page {page-1}")
        break

    all_data.extend(data)
    page += 1

# Convert to DataFrame using all_data, not just data
df = pd.DataFrame(all_data)

# Select specific columns if they exist
if not df.empty:
    available_columns = [col for col in [
        "id", "symbol", "name", "current_price",
        "market_cap", "total_volume", "high_24h", "low_24h",
        "price_change_percentage_24h", "last_updated"
    ] if col in df.columns]

    df = df[available_columns]

# Show info and first 10 rows
print(df.info())

# YAHOO FINANCE
# Example: Bitcoin vs USD
ticker = yf.Ticker("BTC-USD")

# Historical data (e.g., last 30 days, daily candles)
hist = ticker.history(period="30d", interval="1d")

print(hist.info())

