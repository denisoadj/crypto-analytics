from api_libraries import *
from yahoo_finance_data import yahoo_feature

bin_url = "https://api.binance.com/api/v3/klines"

def binance_data(symbol: str=None):
    """
    param: the coin symbol based USDT
    return: a dataframe containing all historical data

    """
    try:
        bin_params = {
            "symbol": symbol,   # trading pair (Bitcoin/USDT)
            "interval": "1h",      # 1m, 5m, 15m, 1h, 1d, etc.
            "limit": 10000           # number of candles (max 1000)
        }

        # Fetch data
        bin_response = r.get(bin_url, params=bin_params)
        bin_data = bin_response.json()

        # Convert to DataFrame
        bin_df = pd.DataFrame(bin_data, columns=[
            "Open time", "Open", "High", "Low", "Close", "Volume",
            "Close time", "Quote asset volume", "Number of trades",
            "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"
        ])

        # Convert timestamp
        bin_df["Open time"] = pd.to_datetime(bin_df["Open time"], unit="ms")
        bin_df["Close time"] = pd.to_datetime(bin_df["Close time"], unit="ms")

        # Convert numeric columns
        numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        bin_df[numeric_cols] = bin_df[numeric_cols].astype(float)
        if bin_df.empty:
            print(f"No data found for {symbol}")
            return None

        return bin_df

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

binance_ticker = [t.replace('-USD', 'USDT') for t in ALL_TICKERS ]
res = []
for ticker in binance_ticker:
    _df = binance_data(ticker)
    if _df is None:
        continue
    _features = yahoo_feature(_df, ticker[:-4])
    #_features["category"] = CATEGORY_MAP[ticker]
    res.append(_features)

historical_res_df = pd.DataFrame(res).dropna().drop(columns=['volatility_ratio'])
#print(historical_res_df.head())


# check the number of symbols
bin_exch_url = "https://api.binance.com/api/v3/exchangeInfo"
bin_exch_response = r.get(bin_exch_url)
bin_exch_data = bin_exch_response.json()
usdt_pairs = [
    s for s in bin_exch_data["symbols"]
    if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
] #409 symbols


# historical_res_df.to_parquet(
#     output_path / "binance_raw",
#     engine="pyarrow",
#     index=False,
#     coerce_timestamps='us',  # This is the key parameter
#     allow_truncated_timestamps=True
# )

