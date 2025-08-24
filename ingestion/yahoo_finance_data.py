from api_libraries import *

def y_df(coin: str=None):

    try:
        t = yf.Ticker(coin)
        t_df = t.history(period="4000d", interval="1d")
        if t_df.empty:
            print(f"No data found for {coin}")
            return None
        return t_df

    except Exception as e:
        print(f"Error fetching {coin}: {e}")
        return None

def yahoo_feature(
    df: pd.DataFrame, symbol: str = None
) -> dict:
    """
    Extracts aggregated crypto features for category classification.
    Returns a single-row feature dictionary.
    """

    df = df.copy()

    # Ensure datetime index
    df.index = pd.to_datetime(df.index)

    # --- Basic Calculations ---
    df["Return"] = df["Close"].pct_change()
    df = df[df["Close"] > 0]
    df["LogReturn"] = np.log(df["Close"] / df["Close"].shift(1))
    df["High_Low"] = df["High"] - df["Low"]
    df["Close_Open"] = df["Close"] - df["Open"]

    # --- Recent Windows ---
    last_7d = df[df.index >= (df.index.max() - pd.Timedelta(days=7))]
    last_30d = df[df.index >= (df.index.max() - pd.Timedelta(days=30))]
    last_90d = df[df.index >= (df.index.max() - pd.Timedelta(days=90))]

    # --- CATEGORY-SPECIFIC FEATURES ---

    # 1. STABILITY (Payment coins)
    stability_score = 1 / (last_90d["Return"].std() + 1e-6)
    price_stability_30d = last_30d["Close"].std() / (last_30d["Close"].mean() + 1e-6)

    # 2. VOLATILITY (Meme tokens)
    volatility_7d = last_7d["Return"].std() * np.sqrt(365) * 100
    volatility_30d = last_30d["Return"].std() * np.sqrt(365) * 100
    volatility_90d = last_90d["Return"].std() * np.sqrt(365) * 100
    volatility_ratio = volatility_7d / volatility_90d if volatility_90d > 0 else np.nan
    max_daily_swing_30d = last_30d["High_Low"].max() / last_30d["Close"].mean() * 100

    # 3. TREND STRENGTH (Layer 1)
    ma_50 = df["Close"].rolling(50).mean()
    ma_200 = df["Close"].rolling(200).mean()
    trend_strength = (
        (ma_50.iloc[-1] - ma_200.iloc[-1]) / ma_200.iloc[-1]
        if ma_200.iloc[-1]
        else np.nan
    )

    # 4. LIQUIDITY (Exchange vs Niche tokens)
    avg_volume_30d = np.log1p(last_30d["Volume"].mean())
    vol_change_ratio = avg_volume_30d / (last_90d["Volume"].mean() + 1e-6)

    # --- Feature Dictionary ---
    features = {
        "symbol": symbol,
        # Stability
        "stability_score": stability_score,
        "price_stability_30d": price_stability_30d,
        # Volatility
        "volatility_7d": volatility_7d,
        "volatility_30d": volatility_30d,
        "volatility_90d": volatility_90d,
        "volatility_ratio": volatility_ratio,
        "max_daily_swing_30d": max_daily_swing_30d,
        # Trend
        "trend_strength": trend_strength,
        # Liquidity
        "avg_volume_30d": avg_volume_30d,
        "vol_change_ratio": vol_change_ratio,
    }

    return features
if __name__ == "__main__":

    results = []
    for ticker in ALL_TICKERS:
        _df = y_df(ticker)
        if _df is None:
            continue
        _features = yahoo_feature(_df, ticker.split("-")[0])
        _features["category"] = CATEGORY_MAP[ticker]
        results.append(_features)

    # Convert to DataFrame
    final_df = pd.DataFrame(results).dropna()
    #print(final_df.info())
    #final_df.to_parquet(output_path / "yahoo_finance_raw", engine="pyarrow", index=False)

    # final_df.to_parquet(
    #     output_path / "yahoo_raw",
    #     engine="pyarrow",
    #     index=False,
    #     coerce_timestamps='us',  # This is the key parameter
    #     allow_truncated_timestamps=True
    # )