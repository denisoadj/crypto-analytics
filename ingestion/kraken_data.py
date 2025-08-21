from api_libraries import *

## KRAKEN 37 PAIRS
k_url = "https://api.kraken.com/0/public/AssetPairs"

k_response = r.get(k_url).json()
k_pairs = k_response["result"]
k_df = pd.DataFrame.from_dict(k_pairs, orient="index")
k_usdt_pairs = k_df[k_df["quote"] == "USDT"]

def kraken_features(df_row: pd.Series) -> dict:
    """
    Extract useful features from a Kraken market row.
    Returns a dict of standardized features for ML categorization.
    """
    return {
        "symbol": df_row.get("base", None),  # coin name (BTC, ETH, etc.)
        "quote": df_row.get("quote", None),  # quoted currency (USDT, USD, etc.)
        "lot_decimals": float(df_row.get("lot_decimals", np.nan)),
        "pair_decimals": float(df_row.get("pair_decimals", np.nan)),
        "tick_size": float(df_row.get("tick_size", np.nan)),
        "order_min": float(df_row.get("ordermin", np.nan)),
        "margin_call": float(df_row.get("margin_call", np.nan)),
        "margin_stop": float(df_row.get("margin_stop", np.nan)),
        "is_live": 1 if df_row.get("status") == "online" else 0,
        "has_leverage": 1 if df_row.get("leverage_buy") or df_row.get("leverage_sell") else 0,
    }

kraken_feat = k_usdt_pairs.apply(kraken_features, axis=1)
kraken_df = pd.DataFrame(kraken_feat.tolist())
#print(kraken_df.info())
#raken_df.to_parquet(output_path / "kraken_raw", engine="pyarrow", index=False)