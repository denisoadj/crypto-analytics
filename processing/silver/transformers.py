"""Transformation functions for Silver Layer"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from config import *
from functools import reduce
from operator import add
from typing import Dict, List
import re

class SilverTransformer:
    """Handles all silver layer transformations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def standardize_symbols(self, df: DataFrame) -> DataFrame:
        """Standardize crypto symbols across all sources"""
        # Remove trading pairs suffixes (USDT, USD, BUSD, etc.)
        # Convert to uppercase
        return df.withColumn(
            "symbol",
            upper(regexp_replace(col("symbol"), r'[-/]?(USDT|USD|BUSD|USDC|BTC|ETH)$', ''))
        )

    def map_columns_by_source(self, df: DataFrame, source: str) -> DataFrame:
        """Map source-specific columns to standard names"""
        column_mapping = self._get_column_mapping(source)

        # Select and rename columns
        select_expr = []
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                select_expr.append(col(old_col).alias(new_col))

        # Add source columns - these need to be preserved
        source_columns = ["LOAD_DATETIME", "PROCESS_ID", "SOURCE_SYSTEM"]
        for src_col in source_columns:
            if src_col in df.columns:
                select_expr.append(col(src_col).alias(src_col.lower()))

        return df.select(*select_expr)

    def _get_column_mapping(self, source: str) -> Dict:
        """Get column mapping for each source"""
        mappings = {
            "coingecko": {
                "ID": "coin_id",
                "SYMBOL": "symbol",
                "NAME": "coin_name",
                "LOG_PRICE": "log_price",
                "LOG_MARKET_CAP": "log_market_cap",
                "LOG_VOLUME": "log_volume_24h",
                "VOLUME_MCAP_RATIO": "volume_mcap_ratio",
                "PRICE_VOLATILITY_24H": "volatility_24h",
                "PRICE_CHANGE_PERCENTAGE_24H": "price_change_24h_pct",
                "IS_STABLE_PRICE": "is_stable_price",
                "LAST_UPDATED": "last_updated"
            },
            "wci": {
                "LABEL": "symbol",
                "NAME": "coin_name",
                "LOG_PRICE": "log_price",
                "LOG_VOLUME_24H": "log_volume_24h",
                "DATETIME_LOCAL": "last_updated"
            },
            "binance": {
                "SYMBOL": "symbol",
                "STABILITY_SCORE": "stability_score",
                "PRICE_STABILITY_30D": "price_stability_30d",
                "VOLATILITY_7D": "volatility_7d",
                "VOLATILITY_30D": "volatility_30d",
                "VOLATILITY_90D": "volatility_90d",
                "MAX_DAILY_SWING_30D": "max_daily_swing_30d",
                "TREND_STRENGTH": "trend_strength",
                "AVG_VOLUME_30D": "avg_volume_30d",
                "VOL_CHANGE_RATIO": "vol_change_ratio"
            },
            "yahoo": {
                "SYMBOL": "symbol",
                "STABILITY_SCORE": "stability_score",
                "PRICE_STABILITY_30D": "price_stability_30d",
                "VOLATILITY_7D": "volatility_7d",
                "VOLATILITY_30D": "volatility_30d",
                "VOLATILITY_90D": "volatility_90d",
                "VOLATILITY_RATIO": "volatility_ratio",
                "MAX_DAILY_SWING_30D": "max_daily_swing_30d",
                "TREND_STRENGTH": "trend_strength",
                "AVG_VOLUME_30D": "avg_volume_30d",
                "VOL_CHANGE_RATIO": "vol_change_ratio",
                "CATEGORY": "yahoo_category"
            },
            "okx": {
                "SYMBOL": "symbol",
                "QUOTE": "quote_currency",
                "TYPE": "instrument_type",
                "TRADE_SIZE": "min_trade_size",
                "TICK_SIZE": "tick_size",
                "MAX_ORDER_SIZE": "max_order_size",
                "MAX_MARKET_SIZE": "max_market_size",
                "HAS_LEVERAGE": "has_leverage",
                "IS_LIVE": "is_live"
            },
            "kraken": {
                "SYMBOL": "symbol",
                "QUOTE": "quote_currency",
                "LOT_DECIMALS": "lot_decimals",
                "PAIR_DECIMALS": "pair_decimals",
                "TICK_SIZE": "tick_size",
                "ORDER_MIN": "min_order_size",
                "MARGIN_CALL": "margin_call_level",
                "MARGIN_STOP": "margin_stop_level",
                "HAS_LEVERAGE": "has_leverage",
                "STATUS": "trading_status"
            }
        }

        return mappings.get(source, {})


    def normalize_timestamps(self, df: DataFrame) -> DataFrame:
        """Normalize timestamp columns across sources"""
        timestamp_cols = ["last_updated", "load_datetime"]

        for col_name in timestamp_cols:
            if col_name in df.columns:
                # Convert to timestamp if string
                df = df.withColumn(
                    col_name,
                    to_timestamp(col(col_name))
                )

        # If no last_updated, use load_datetime
        if "last_updated" not in df.columns and "load_datetime" in df.columns:
            df = df.withColumn("last_updated", col("load_datetime"))

        return df

    def normalize_numeric_values(self, df: DataFrame) -> DataFrame:
        """Normalize numeric values to consistent units"""
        # Convert percentage values to decimals if needed
        pct_columns = ["price_change_24h_pct", "volatility_ratio", "vol_change_ratio"]

        for col_name in pct_columns:
            if col_name in df.columns:
                # If values are > 1, assume they're percentages
                df = df.withColumn(
                    col_name,
                    when(abs(col(col_name)) > 1, col(col_name) / 100)
                    .otherwise(col(col_name))
                )

        return df

    def add_derived_features(self, df: DataFrame) -> DataFrame:
        """Add calculated features useful for analysis"""
        # Price in actual USD (from log)
        if "log_price" in df.columns:
            df = df.withColumn(
                "price_usd",
                when(col("log_price").isNotNull(), exp(col("log_price")))
                .otherwise(None)
            )

        # Market cap in actual USD
        if "log_market_cap" in df.columns:
            df = df.withColumn(
                "market_cap_usd",
                when(col("log_market_cap").isNotNull(), exp(col("log_market_cap")))
                .otherwise(None)
            )

        # Volume in actual USD
        if "log_volume_24h" in df.columns:
            df = df.withColumn(
                "volume_24h_usd",
                when(col("log_volume_24h").isNotNull(), exp(col("log_volume_24h")))
                .otherwise(None)
            )

        # Volume to market cap ratio (if not already present)
        if "volume_mcap_ratio" not in df.columns and all(c in df.columns for c in ["log_volume_24h", "log_market_cap"]):
            df = df.withColumn(
                "volume_mcap_ratio",
                when(
                    col("log_market_cap").isNotNull() & col("log_volume_24h").isNotNull(),
                    exp(col("log_volume_24h") - col("log_market_cap"))
                ).otherwise(None)
            )

        # Average volatility across timeframes
        volatility_cols = ["volatility_7d", "volatility_30d", "volatility_90d"]
        available_vol_cols = [c for c in volatility_cols if c in df.columns]

        if available_vol_cols:
            # Calculate average volatility
            vol_sum = reduce(add, [coalesce(col(c), lit(0)) for c in available_vol_cols])
            vol_count = reduce(add, [when(col(c).isNotNull(), 1).otherwise(0) for c in available_vol_cols])

            df = df.withColumn(
                "avg_volatility",
                when(vol_count > 0, vol_sum / vol_count).otherwise(None)
            )

        return df

    def harmonize_leverage_flags(self, df: DataFrame) -> DataFrame:
        """Harmonize leverage/margin trading flags across sources"""
        # Create unified has_leverage flag
        leverage_indicators = []

        if "has_leverage" in df.columns:
            leverage_indicators.append(col("has_leverage") == True)

        if "margin_call_level" in df.columns:
            leverage_indicators.append(col("margin_call_level").isNotNull())

        if "instrument_type" in df.columns:
            leverage_indicators.append(col("instrument_type").isin(["FUTURES", "PERPETUAL", "MARGIN"]))

        if leverage_indicators:
            df = df.withColumn(
                "has_leverage_unified",
                when(
                    reduce(lambda a, b: a | b, leverage_indicators),
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("has_leverage_unified", lit(False))

        # Rename to standard column
        if "has_leverage" not in df.columns:
            df = df.withColumnRenamed("has_leverage_unified", "has_leverage")

        return df

    def add_data_source_features(self, df: DataFrame, source: str) -> DataFrame:
        """Add features specific to data source quality"""
        # Add source quality score
        source_quality_scores = {
            "binance": 0.95,   # High quality, real-time
            "okx": 0.90,       # Good quality, comprehensive
            "kraken": 0.90,    # Regulated, reliable
            "coingecko": 0.80, # Good coverage
            "yahoo": 0.70,     # Limited but has categories
            "wci": 0.60        # Wide coverage but basic
        }

        df = df.withColumn(
            "source_quality_score",
            lit(source_quality_scores.get(source, 0.5))
        )

        # Add source type
        source_types = {
            "binance": "exchange",
            "okx": "exchange",
            "kraken": "exchange",
            "coingecko": "aggregator",
            "yahoo": "financial_data",
            "wci": "aggregator"
        }

        df = df.withColumn(
            "source_type",
            lit(source_types.get(source, "unknown"))
        )

        return df

    def clean_text_fields(self, df: DataFrame) -> DataFrame:
        """Clean and standardize text fields"""
        text_columns = ["coin_name", "coin_id", "yahoo_category"]

        for col_name in text_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    trim(regexp_replace(col(col_name), r'[^\w\s-]', ''))
                )

        return df

    def add_market_indicators(self, df: DataFrame) -> DataFrame:
        """Add market health indicators"""
        # Liquidity indicator based on volume
        if "log_volume_24h" in df.columns:
            df = df.withColumn(
                "liquidity_tier",
                when(col("log_volume_24h") > 20, "Very High")     # > $1B
                .when(col("log_volume_24h") > 18, "High")         # > $100M
                .when(col("log_volume_24h") > 16, "Medium")       # > $10M
                .when(col("log_volume_24h") > 14, "Low")          # > $1M
                .otherwise("Very Low")
            )

        # Price momentum indicator
        if "trend_strength" in df.columns:
            df = df.withColumn(
                "momentum_signal",
                when(col("trend_strength") > 0.7, "Strong Bullish")
                .when(col("trend_strength") > 0.3, "Bullish")
                .when(col("trend_strength") > -0.3, "Neutral")
                .when(col("trend_strength") > -0.7, "Bearish")
                .otherwise("Strong Bearish")
            )

        # Volatility regime
        if "volatility_30d" in df.columns:
            df = df.withColumn(
                "volatility_regime",
                when(col("is_stablecoin") == True, "Stable")
                .when(col("volatility_30d") < 0.5, "Low Vol")
                .when(col("volatility_30d") < 1.0, "Medium Vol")
                .when(col("volatility_30d") < 2.0, "High Vol")
                .otherwise("Extreme Vol")
            )

        return df

    def apply_all_transformations(self, df: DataFrame, source: str) -> DataFrame:
        """Apply all transformations in the correct order"""
        # 1. Map columns to standard names
        df = self.map_columns_by_source(df, source)

        # 2. Standardize symbols
        df = self.standardize_symbols(df)

        # 3. Normalize timestamps
        df = self.normalize_timestamps(df)

        # 4. Normalize numeric values
        df = self.normalize_numeric_values(df)

        # 5. Clean text fields
        df = self.clean_text_fields(df)

        # 6. Add derived features
        df = self.add_derived_features(df)

        # 7. Harmonize leverage flags
        df = self.harmonize_leverage_flags(df)

        # 8. Add data source features
        df = self.add_data_source_features(df, source)

        # 9. Add market indicators
        df = self.add_market_indicators(df)

        return df

    def create_exchange_features(self, df: DataFrame) -> DataFrame:
        """Create exchange-specific features for coins"""
        # Count exchanges per coin (to be used after consolidation)
        if "source_system" in df.columns:
            exchange_window = Window.partitionBy("symbol")

            # Create binary flags for each exchange
            for exchange in ["binance", "okx", "kraken"]:
                df = df.withColumn(
                    f"on_{exchange}",
                    when(col("source_system") == exchange, 1).otherwise(0)
                )

            # Aggregate exchange presence
            df = df.withColumn(
                "exchange_count",
                reduce(add, [col(f"on_{exchange}") for exchange in ["binance", "okx", "kraken"]]).over(exchange_window)
            )

            # Premium exchange indicator (Binance + Kraken)
            df = df.withColumn(
                "on_premium_exchanges",
                when((col("on_binance") == 1) | (col("on_kraken") == 1), True).otherwise(False)
            )

        return df

    def calculate_relative_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate metrics relative to market or category"""
        # This should be called after consolidation when we have all symbols

        # Market-wide statistics window
        market_window = Window.partitionBy(lit(1))  # All rows

        # Calculate market percentiles for volume
        if "log_volume_24h" in df.columns:
            df = df.withColumn(
                "volume_percentile",
                percent_rank().over(Window.orderBy(col("log_volume_24h")))
            )

        # Calculate market percentiles for market cap
        if "log_market_cap" in df.columns:
            df = df.withColumn(
                "mcap_percentile",
                percent_rank().over(Window.orderBy(col("log_market_cap")))
            )

        # Calculate volatility percentile
        if "volatility_30d" in df.columns:
            df = df.withColumn(
                "volatility_percentile",
                percent_rank().over(Window.orderBy(col("volatility_30d")))
            )

        return df

    def add_technical_indicators(self, df: DataFrame) -> DataFrame:
        """Add technical analysis indicators where applicable"""
        # Relative Strength Index proxy (if we have price change data)
        if "price_change_24h_pct" in df.columns:
            df = df.withColumn(
                "rsi_signal",
                when(col("price_change_24h_pct") > 0.10, "Overbought")
                .when(col("price_change_24h_pct") > 0.05, "Strong")
                .when(col("price_change_24h_pct") > -0.05, "Neutral")
                .when(col("price_change_24h_pct") > -0.10, "Weak")
                .otherwise("Oversold")
            )

        # Volume surge indicator
        if all(c in df.columns for c in ["vol_change_ratio", "avg_volume_30d"]):
            df = df.withColumn(
                "volume_surge",
                when(col("vol_change_ratio") > 2.0, "Extreme Surge")
                .when(col("vol_change_ratio") > 1.5, "High Volume")
                .when(col("vol_change_ratio") > 0.8, "Normal")
                .otherwise("Low Volume")
            )

        return df

    def create_composite_scores(self, df: DataFrame) -> DataFrame:
        """Create composite scores for easy filtering and ranking"""
        # Investment attractiveness score (example composite)
        score_components = []
        weights = {}

        # Liquidity component
        if "liquidity_score" in df.columns:
            score_components.append(col("liquidity_score") * 0.3)
            weights["liquidity"] = 0.3

        # Stability component (inverse of volatility)
        if "volatility_30d" in df.columns:
            stability = when(
                col("volatility_30d").isNotNull(),
                greatest(lit(0), 1 - col("volatility_30d"))
            ).otherwise(0.5)
            score_components.append(stability * 0.2)
            weights["stability"] = 0.2

        # Market cap component
        if "mcap_percentile" in df.columns:
            score_components.append(col("mcap_percentile") * 0.2)
            weights["market_cap"] = 0.2

        # Exchange coverage component
        if "exchange_count" in df.columns:
            exchange_score = col("exchange_count") / 6  # Normalize by total exchanges
            score_components.append(least(exchange_score, lit(1)) * 0.15)
            weights["exchange"] = 0.15

        # Data quality component
        if "data_quality_score" in df.columns:
            score_components.append(col("data_quality_score") * 0.15)
            weights["quality"] = 0.15

        # Calculate weighted score
        if score_components:
            total_weight = sum(weights.values())  # This is fine - it's summing regular Python numbers
            df = df.withColumn(
                "investment_score",
                reduce(add, score_components) / total_weight * 100
            )

        return df

    def add_alert_flags(self, df: DataFrame) -> DataFrame:
        """Add flags for potential alerts or warnings"""
        # High volatility alert
        if "volatility_30d" in df.columns:
            df = df.withColumn(
                "high_volatility_alert",
                when(col("volatility_30d") > 2.0, True).otherwise(False)
            )

        # Low liquidity warning
        if "log_volume_24h" in df.columns:
            df = df.withColumn(
                "low_liquidity_warning",
                when(col("log_volume_24h") < 14, True).otherwise(False)  # < $1M volume
            )

        # Price surge alert
        if "price_change_24h_pct" in df.columns:
            df = df.withColumn(
                "price_surge_alert",
                when(abs(col("price_change_24h_pct")) > 0.20, True).otherwise(False)
            )

        # New listing alert (only on one exchange)
        if "exchange_count" in df.columns:
            df = df.withColumn(
                "new_listing_flag",
                when(col("exchange_count") == 1, True).otherwise(False)
            )

        return df

    def format_display_values(self, df: DataFrame) -> DataFrame:
        """Format values for display in dashboard"""
        # Format large numbers for display
        if "market_cap_usd" in df.columns:
            df = df.withColumn(
                "market_cap_display",
                when(col("market_cap_usd") >= 1e9,
                     concat(round(col("market_cap_usd") / 1e9, 2), lit("B")))
                .when(col("market_cap_usd") >= 1e6,
                      concat(round(col("market_cap_usd") / 1e6, 2), lit("M")))
                .when(col("market_cap_usd") >= 1e3,
                      concat(round(col("market_cap_usd") / 1e3, 2), lit("K")))
                .otherwise(round(col("market_cap_usd"), 2))
            )

        if "volume_24h_usd" in df.columns:
            df = df.withColumn(
                "volume_24h_display",
                when(col("volume_24h_usd") >= 1e9,
                     concat(round(col("volume_24h_usd") / 1e9, 2), lit("B")))
                .when(col("volume_24h_usd") >= 1e6,
                      concat(round(col("volume_24h_usd") / 1e6, 2), lit("M")))
                .when(col("volume_24h_usd") >= 1e3,
                      concat(round(col("volume_24h_usd") / 1e3, 2), lit("K")))
                .otherwise(round(col("volume_24h_usd"), 2))
            )

        # Format percentages
        percentage_cols = ["price_change_24h_pct", "volatility_30d", "volatility_7d"]
        for col_name in percentage_cols:
            if col_name in df.columns:
                df = df.withColumn(
                    f"{col_name}_display",
                    concat(round(col(col_name) * 100, 2), lit("%"))
                )

        return df