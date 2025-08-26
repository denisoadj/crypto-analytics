"""Main orchestration script for Silver Layer Processing"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from datetime import datetime
import logging
import sys
from typing import Dict, List

# Import from our modules
from config import SNOWFLAKE_CONFIG, CRYPTO_CATEGORIES, SOURCE_PRIORITY, SYMBOL_TO_CATEGORY
from transformers import SilverTransformer
from validators import DataValidator
from utils import SparkUtils, SnowflakeUtils, DataQualityUtils, StatsCalculator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SilverLayerProcessor:
    """Main processor for Silver Layer ETL"""

    def __init__(self):
        self.spark = SparkUtils.create_spark_session("silver_layer_processing")
        self.process_id = SparkUtils.generate_process_id()
        self.sf_utils = SnowflakeUtils(self.spark, SNOWFLAKE_CONFIG)
        self.transformer = SilverTransformer(self.spark)
        self.validator = DataValidator()
        self.stats_calc = StatsCalculator()

        logger.info(f"Initialized Silver Layer Processor with process_id: {self.process_id}")

    def process_source(self, source: str) -> DataFrame:
        """Process data from a single source"""
        logger.info(f"Processing source: {source}")

        # Read from Bronze
        df = self.sf_utils.read_bronze_table(source)
        logger.info(f"Read {df.count()} rows from {source}")

        # Map columns to standard names
        df = self.transformer.map_columns_by_source(df, source)

        # Standardize symbols
        df = self.transformer.standardize_symbols(df)

        # Validate data
        df = self.validator.validate_symbols(df)
        df = self.validator.validate_numeric_columns(df,
                                                     ["log_price", "log_volume_24h", "volatility_24h", "volatility_30d"])
        df = self.validator.validate_ranges(df)

        # Add source priority for conflict resolution
        df = df.withColumn("source_priority", lit(SOURCE_PRIORITY.get(source, 0)))

        return df

    def consolidate_sources(self, source_dfs: List[DataFrame]) -> DataFrame:
        """Consolidate data from multiple sources with conflict resolution"""
        logger.info("Consolidating data from all sources")

        # Union all source dataframes
        consolidated = source_dfs[0]
        for df in source_dfs[1:]:
            consolidated = consolidated.unionByName(df, allowMissingColumns=True)

        # Define window for selecting best data per symbol
        window_spec = Window.partitionBy("symbol").orderBy(
            col("source_priority").desc(),
            col("last_updated").desc()
        )

        # Columns to aggregate (take from the highest priority source)
        value_columns = [
            "log_price", "log_market_cap", "log_volume_24h",
            "volatility_24h", "volatility_7d", "volatility_30d", "volatility_90d",
            "stability_score", "trend_strength", "has_leverage",
            "price_change_24h_pct", "volume_mcap_ratio"
        ]

        # Create first() expressions for each column
        agg_exprs = []
        for col_name in value_columns:
            agg_exprs.append(
                first(col(col_name), ignorenulls=True).over(window_spec).alias(col_name)
            )

        # Add metadata columns
        agg_exprs.extend([
            col("symbol"),
            first("coin_name", ignorenulls=True).over(window_spec).alias("coin_name"),
            first("coin_id", ignorenulls=True).over(window_spec).alias("coin_id"),
            max("last_updated").over(window_spec).alias("last_updated"),
            collect_set("source_system").over(window_spec).alias("source_systems"),
            count("*").over(window_spec).alias("source_count")
        ])

        # Select consolidated data
        consolidated = consolidated.select(*agg_exprs).distinct()

        return consolidated

    def enrich_with_categories(self, df: DataFrame) -> DataFrame:
        """Add categories and calculated fields"""
        logger.info("Enriching data with categories and calculated fields")

        # Add category from mapping
        category_udf = udf(lambda x: SYMBOL_TO_CATEGORY.get(x, "Other"))
        df = df.withColumn("category", category_udf(col("symbol")))

        # Add market tier based on market cap
        df = df.withColumn(
            "market_tier",
            when(col("log_market_cap") > 23, "Large Cap")      # > $10B
            .when(col("log_market_cap") > 20.7, "Mid Cap")     # > $1B
            .when(col("log_market_cap") > 18.4, "Small Cap")   # > $100M
            .when(col("log_market_cap").isNotNull(), "Micro Cap")
            .otherwise("Unknown")
        )

        # Detect stablecoins
        stable_symbols = ["USDT", "USDC", "DAI", "BUSD", "TUSD", "USDD", "FRAX", "GUSD", "USDP"]
        df = df.withColumn(
            "is_stablecoin",
            when(
                col("symbol").isin(stable_symbols) |
                (col("stability_score") > 0.95) |
                (col("volatility_30d") < 0.01),
                True
            ).otherwise(False)
        )

        # Calculate risk score (composite of volatility metrics)
        df = df.withColumn(
            "risk_score",
            when(
                col("volatility_30d").isNotNull(),
                col("volatility_30d") * 0.5 +
                coalesce(col("volatility_7d"), col("volatility_30d")) * 0.3 +
                coalesce(col("volatility_90d"), col("volatility_30d")) * 0.2
            ).when(
                col("volatility_24h").isNotNull(),
                col("volatility_24h")
            ).otherwise(None)
        )

        # Calculate liquidity score
        df = df.withColumn(
            "liquidity_score",
            when(
                col("log_volume_24h").isNotNull() & col("log_market_cap").isNotNull(),
                least(col("volume_mcap_ratio") * 100, lit(100))  # Cap at 100
            ).when(
                col("log_volume_24h").isNotNull(),
                (col("log_volume_24h") / lit(25)) * 100  # Normalize assuming max log volume of 25
            ).otherwise(0)
        )

        # Trading availability
        df = df.withColumn(
            "trading_availability",
            when(col("has_leverage") == True, "Margin")
            .when(col("source_count") >= 3, "Spot")
            .otherwise("Limited")
        )

        return df

    def calculate_statistics(self, df: DataFrame) -> DataFrame:
        """Calculate descriptive statistics for dashboard"""
        logger.info("Calculating descriptive statistics")

        # Basic statistics
        df = self.stats_calc.calculate_descriptive_stats(df)

        # Percentiles for key metrics
        df = self.stats_calc.calculate_percentiles(df, "log_price", [0.25, 0.5, 0.75])
        df = self.stats_calc.calculate_percentiles(df, "volatility_30d", [0.25, 0.5, 0.75])

        # Add relative metrics
        df = df.withColumn(
            "price_vs_category_avg",
            when(
                col("category") != "Other",
                col("log_price") / avg("log_price").over(Window.partitionBy("category"))
            ).otherwise(None)
        )

        # Volatility ranking within category
        category_window = Window.partitionBy("category").orderBy("volatility_30d")
        df = df.withColumn(
            "volatility_rank_in_category",
            percent_rank().over(category_window)
        )

        # Volume ranking overall
        volume_window = Window.orderBy(col("log_volume_24h").desc())
        df = df.withColumn(
            "volume_rank_overall",
            row_number().over(volume_window)
        )

        return df

    def create_silver_tables(self, df: DataFrame):
        """Create optimized Silver layer tables"""
        logger.info("Creating Silver layer tables")

        # Add metadata
        df = SparkUtils.add_metadata_columns(df, self.process_id)

        # Calculate data quality scores
        required_columns = ["symbol", "log_price", "log_volume_24h", "category"]
        df = DataQualityUtils.calculate_completeness_score(df, required_columns)
        df = DataQualityUtils.calculate_freshness_score(df)
        df = DataQualityUtils.calculate_overall_quality_score(df)

        # Main consolidated table
        self.sf_utils.write_silver_table(
            df,
            "SILVER_CRYPTO_STATS",
            mode="overwrite"  # Full refresh for now
        )

        # Create summary table for dashboard
        summary_df = df.groupBy("symbol", "coin_name", "category", "market_tier").agg(
            last("log_price").alias("latest_log_price"),
            last("log_volume_24h").alias("latest_log_volume"),
            last("volatility_30d").alias("latest_volatility_30d"),
            last("risk_score").alias("latest_risk_score"),
            last("liquidity_score").alias("latest_liquidity_score"),
            last("is_stablecoin").alias("is_stablecoin"),
            last("trading_availability").alias("trading_availability"),
            last("source_count").alias("exchange_count"),
            last("last_updated").alias("last_updated"),
            last("data_quality_score").alias("data_quality_score"),
            avg("log_price").alias("avg_log_price"),
            min("log_price").alias("min_log_price"),
            max("log_price").alias("max_log_price"),
            stddev("log_price").alias("stddev_log_price")
        )

        self.sf_utils.write_silver_table(
            summary_df,
            "SILVER_CRYPTO_SUMMARY",
            mode="overwrite"
        )

        logger.info(f"Created Silver tables with {df.count()} detailed records and {summary_df.count()} summary records")

    def run(self):
        """Main execution method"""
        logger.info("Starting Silver Layer Processing")
        start_time = datetime.now()

        try:
            # Process each source
            sources = ["coingecko", "wci", "binance", "yahoo", "okx", "kraken"]
            source_dfs = []

            for source in sources:
                try:
                    df = self.process_source(source)
                    if df.count() > 0:
                        source_dfs.append(df)
                        logger.info(f"Successfully processed {source}")
                except Exception as e:
                    logger.error(f"Error processing {source}: {e}")
                    continue

            if not source_dfs:
                logger.error("No data processed from any source")
                return

            # Consolidate all sources
            consolidated_df = self.consolidate_sources(source_dfs)
            logger.info(f"Consolidated to {consolidated_df.count()} unique symbols")

            # Enrich with categories and calculated fields
            enriched_df = self.enrich_with_categories(consolidated_df)

            # Calculate statistics
            final_df = self.calculate_statistics(enriched_df)

            # Create Silver tables
            self.create_silver_tables(final_df)

            # Log summary statistics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"Silver Layer Processing completed in {duration:.2f} seconds")
            logger.info(f"Process ID: {self.process_id}")

            # Print summary stats
            final_df.groupBy("category").agg(
                count("*").alias("count"),
                avg("risk_score").alias("avg_risk"),
                avg("liquidity_score").alias("avg_liquidity")
            ).show()

        except Exception as e:
            logger.error(f"Fatal error in Silver Layer Processing: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    """Entry point for Silver Layer Processing"""
    processor = SilverLayerProcessor()
    processor.run()

if __name__ == "__main__":
    main()