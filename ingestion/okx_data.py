from api_libraries import *

okx_url_symbols = "https://www.okx.com/api/v5/public/instruments"
params = {"instType": "SPOT"}
okx_symbols_data = r.get(okx_url_symbols, params=params).json()

okx_df = pd.DataFrame(okx_symbols_data['data'])

def extract_okx_features(df_row: pd.Series) -> dict:
    """
    Extract useful features from an OKX market row.
    Returns a dict of standardized features for ML categorization.
    """
    return {
        "symbol": df_row.get("baseCcy", None),  # coin name (BTC, ETH, etc.)
        "quote": df_row.get("quoteCcy", None),  # quoted currency (USDT, USD, etc.)
        "Type": df_row.get("instType", None),  # SPOT, FUTURE, etc.
        "trade_size": float(df_row.get("lotSz", np.nan)),  # minimum trade size
        "tick_size": float(df_row.get("tickSz", np.nan)),  # tick size
        "max_order_size": float(df_row.get("maxLmtSz", np.nan)),  # max order size
        "max_market_size": float(df_row.get("maxMktSz", np.nan)),  # max market size
        "has_leverage": int(df_row.get("lever", 0)) if df_row.get("lever") else 0,  # leverage if available
        "is_live": 1 if df_row.get("state") == "live" else 0,  # market live flag
    }


okx_features = okx_df.apply(extract_okx_features, axis=1)
okx_features_df = pd.DataFrame(okx_features.tolist())

#print(okx_features_df.info())
#okx_features_df.to_parquet(output_path / "okx_raw", engine="pyarrow", index=False)