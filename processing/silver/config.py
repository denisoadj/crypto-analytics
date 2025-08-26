"""Configuration module for Silver Layer Processing"""

# Snowflake configurations
SNOWFLAKE_CONFIG = {
    "sfURL": "anppsyn-fv19174.snowflakecomputing.com",
    "sfDatabase": "CRYPTO_DB",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfUser": "DEN",
    "sfPassword": "InfoTechCons#2025"
}

# Category mappings
CRYPTO_CATEGORIES = {
    "Payment": ["USDT", "USDC", "DAI", "BUSD", "TUSD", "XRP"],
    "DeFi": ["UNI", "AAVE", "COMP", "SUSHI", "MKR", "CAKE", "CRV"],
    "Layer 1": ["BTC", "ETH", "SOL", "AVAX", "ADA", "DOT"],
    "Meme tokens": ["DOGE", "SHIB", "FLOKI", "PEPE", "CKB"],
    "NFT": ["SAND", "MANA", "AXS", "ENJ", "THETA"],
    "Infrastructure": ["LINK", "GRT", "RUNE", "AGIX", "OCEAN", "NEAR", "TAO"]
}

# Create reverse mapping for quick lookup
SYMBOL_TO_CATEGORY = {}
for category, symbols in CRYPTO_CATEGORIES.items():
    for symbol in symbols:
        SYMBOL_TO_CATEGORY[symbol] = category

# Source priority for conflict resolution (higher = more trusted)
SOURCE_PRIORITY = {
    "binance": 5,    # Most liquid, real-time
    "okx": 4,        # Good coverage, reliable
    "kraken": 4,     # Regulated, high quality
    "coingecko": 3,  # Comprehensive coverage
    "yahoo": 2,      # Has categories but limited
    "wci": 1         # Widest coverage but basic
}

# Column standardization mappings
STANDARD_COLUMNS = {
    "symbol": "symbol",
    "coin_name": "name",
    "log_price": "log_price",
    "log_market_cap": "log_market_cap",
    "log_volume_24h": "log_volume_24h",
    "price_change_24h_pct": "price_change_24h_pct",
    "volatility_24h": "volatility_24h",
    "volatility_7d": "volatility_7d",
    "volatility_30d": "volatility_30d",
    "volatility_90d": "volatility_90d",
    "stability_score": "stability_score",
    "trend_strength": "trend_strength",
    "has_leverage": "has_leverage",
    "is_stablecoin": "is_stablecoin",
    "category": "category",
    "last_updated": "last_updated"
}

# Numeric columns for validation
NUMERIC_COLUMNS = [
    "log_price", "log_market_cap", "log_volume_24h",
    "volatility_24h", "volatility_7d", "volatility_30d", "volatility_90d",
    "price_change_24h_pct", "stability_score", "trend_strength"
]