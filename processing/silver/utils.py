"""Utility functions for Silver Layer Processing"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from typing import Dict, List, Optional
import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

class SparkUtils:
    """Spark utility functions"""

    @staticmethod
    def create_spark_session(app_name: str = "silver_layer") -> SparkSession:
        """Create optimized Spark session"""
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()

    @staticmethod
    def optimize_dataframe(df: DataFrame) -> DataFrame:
        """Optimize DataFrame for better performance"""
        # Coalesce to reduce number of partitions if small dataset
        row_count = df.count()
        if row_count < 10000:
            return df.coalesce(1)
        elif row_count < 100000:
            return df.coalesce(10)
        else:
            return df.repartition(200, "symbol")

    @staticmethod
    def generate_process_id() -> str:
        """Generate unique process ID"""
        return str(uuid.uuid4())

    @staticmethod
    def add_metadata_columns(df: DataFrame, process_id: str) -> DataFrame:
        """Add standard metadata columns"""
        return df.withColumn("silver_process_id", lit(process_id)) \
            .withColumn("silver_load_datetime", current_timestamp()) \
            .withColumn("data_quality_score", lit(1.0))  # Default, will be updated

class SnowflakeUtils:
    """Snowflake-specific utility functions"""

    def __init__(self, spark: SparkSession, sf_options: Dict):
        self.spark = spark
        self.sf_options = sf_options

    def read_bronze_table(self, source: str, limit: Optional[int] = None) -> DataFrame:
        """Read data from Bronze layer table"""
        bronze_options = self.sf_options.copy()
        bronze_options["sfSchema"] = "BRONZE"

        table_name = f"BRONZE_{source.upper()}"

        if limit:
            query = f"SELECT * FROM {table_name} ORDER BY load_datetime DESC LIMIT {limit}"
            return self.spark.read \
                .format("net.snowflake.spark.snowflake") \
                .options(**bronze_options) \
                .option("query", query) \
                .load()
        else:
            return self.spark.read \
                .format("net.snowflake.spark.snowflake") \
                .options(**bronze_options) \
                .option("dbtable", table_name) \
                .load()

    def write_silver_table(self, df: DataFrame, table_name: str, mode: str = "append"):
        """Write data to Silver layer table"""
        silver_options = self.sf_options.copy()
        silver_options["sfSchema"] = "SILVER"

        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**silver_options) \
            .option("dbtable", table_name) \
            .mode(mode) \
            .save()

        logger.info(f"Successfully wrote {df.count()} rows to {table_name}")

    def check_table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists in Snowflake"""
        try:
            query = f"""
            SELECT COUNT(*) as count 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema.upper()}' 
            AND TABLE_NAME = '{table.upper()}'
            """

            result = self.spark.read \
                .format("net.snowflake.spark.snowflake") \
                .options(**self.sf_options) \
                .option("query", query) \
                .load()

            return result.collect()[0]['count'] > 0
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def get_latest_process_timestamp(self, source: str) -> Optional[datetime]:
        """Get latest processed timestamp for incremental loading"""
        try:
            silver_options = self.sf_options.copy()
            silver_options["sfSchema"] = "SILVER"

            query = f"""
            SELECT MAX(last_updated) as max_timestamp 
            FROM SILVER_CRYPTO_STATS 
            WHERE source_system = '{source}'
            """

            result = self.spark.read \
                .format("net.snowflake.spark.snowflake") \
                .options(**silver_options) \
                .option("query", query) \
                .load()

            max_ts = result.collect()[0]['max_timestamp']
            return max_ts
        except Exception as e:
            logger.info(f"No previous data found for {source}: {e}")
            return None

class DataQualityUtils:
    """Data quality scoring utilities"""

    @staticmethod
    def calculate_completeness_score(df: DataFrame, required_columns: List[str]) -> DataFrame:
        """Calculate data completeness score for each row"""
        # Count non-null values for required columns
        null_counts = [when(col(c).isNull(), 1).otherwise(0) for c in required_columns if c in df.columns]

        if null_counts:
            total_required = len(null_counts)
            # Fix: Use reduce to sum the column expressions
            from functools import reduce
            from operator import add
            null_sum = reduce(add, null_counts)

            return df.withColumn(
                "completeness_score",
                (lit(total_required) - null_sum) / lit(total_required)
            )
        else:
            return df.withColumn("completeness_score", lit(1.0))

    @staticmethod
    def calculate_freshness_score(df: DataFrame) -> DataFrame:
        """Calculate data freshness score based on last_updated"""
        if "last_updated" in df.columns:
            return df.withColumn(
                "freshness_score",
                when(
                    datediff(current_timestamp(), col("last_updated")) <= 1, 1.0
                ).when(
                    datediff(current_timestamp(), col("last_updated")) <= 7, 0.8
                ).when(
                    datediff(current_timestamp(), col("last_updated")) <= 30, 0.5
                ).otherwise(0.3)
            )
        else:
            return df.withColumn("freshness_score", lit(0.5))

    @staticmethod
    def calculate_overall_quality_score(df: DataFrame) -> DataFrame:
        """Calculate overall data quality score"""
        return df.withColumn(
            "data_quality_score",
            (col("completeness_score") * 0.6 + col("freshness_score") * 0.4)
        ).drop("completeness_score", "freshness_score")

class StatsCalculator:
    """Calculate descriptive statistics for crypto assets"""

    @staticmethod
    def calculate_descriptive_stats(df: DataFrame) -> DataFrame:
        """Calculate comprehensive statistics for each symbol"""
        # Window for calculating stats across all records for a symbol
        symbol_window = Window.partitionBy("symbol")

        return df.withColumn("price_min", min("log_price").over(symbol_window)) \
            .withColumn("price_max", max("log_price").over(symbol_window)) \
            .withColumn("price_avg", avg("log_price").over(symbol_window)) \
            .withColumn("price_stddev", stddev("log_price").over(symbol_window)) \
            .withColumn("volume_avg", avg("log_volume_24h").over(symbol_window)) \
            .withColumn("volatility_avg", avg("volatility_30d").over(symbol_window)) \
            .withColumn("exchange_coverage", col("source_count"))

    @staticmethod
    def calculate_percentiles(df: DataFrame, col_name: str, percentiles: List[float]) -> DataFrame:
        """Calculate percentiles for a given column"""
        for p in percentiles:
            percentile_col = f"{col_name}_p{int(p*100)}"
            df = df.withColumn(
                percentile_col,
                expr(f"percentile_approx({col_name}, {p})").over(Window.partitionBy("symbol"))
            )
        return df