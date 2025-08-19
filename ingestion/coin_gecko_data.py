from api_libraries import *

cg_url = "https://api.coingecko.com/api/v3/coins/markets" # CoinGecko free API endpoint (no API key needed)

all_data = []
page = 1
max_pages = 10  # Add a limit to prevent infinite loops

while page <= max_pages:
    cg_params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",  # sort by market cap
        "per_page": 250,             # max coins per page
        "page": page,
        "sparkline": False
    }

    try:
        response = r.get(cg_url, params=cg_params)
        response.raise_for_status()
        cg_data = response.json()
    except r.exceptions.RequestException as e:
        print(f"Error fetching data on page {page}: {e}")
        break

    if not cg_data:
        print(f"No more data after page {page-1}")
        break

    all_data.extend(cg_data)
    page += 1


cg_df = pd.DataFrame(all_data)


# Feature engineering
def engineer_coingecko_features(df):
    # Volatility measure
    df["price_volatility_24h"] = (
        (df["high_24h"] - df["low_24h"]) / df["current_price"]
    ) * 100

    # Volume to market cap ratio (liquidity indicator)
    df["volume_mcap_ratio"] = df["total_volume"] / df["market_cap"]

    # Price stability indicator
    df["is_stable_price"] = (df["price_change_percentage_24h"].abs() < 0.5).astype(int)

    # Market cap tiers
    df["mcap_tier"] = pd.cut(
        df["market_cap"],
        bins=[0, 1e7, 1e8, 1e9, 1e10, float("inf")],
        labels=["micro", "small", "mid", "large", "mega"],
    )

    # Price range
    df["daily_price_range"] = df["high_24h"] - df["low_24h"]

    # Log transformations for ML models
    df["log_market_cap"] = np.log1p(df["market_cap"])
    df["log_volume"] = np.log1p(df["total_volume"])
    df['log_price'] = np.log1p(df['current_price'])

    # for the cdc
    df['last_updated'] = pd.to_datetime(df['last_updated'])

    return df

eng_cg_df = engineer_coingecko_features(cg_df)
feature_columns = [
    'id',
    'symbol',
    'name',
    'log_market_cap',
    'log_volume',
    'volume_mcap_ratio',
    'price_volatility_24h',
    'price_change_percentage_24h',
    'is_stable_price',
    'log_price',
    'last_updated'
]

final_cg_df = eng_cg_df[feature_columns]
print(final_cg_df.head())