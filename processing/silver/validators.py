"""Data validation functions for Silver Layer"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates and cleans data for silver layer"""

    @staticmethod
    def validate_numeric_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """Validate numeric columns - remove invalid values"""
        for col_name in columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNull() |
                        isnan(col(col_name)) |
                        (col(col_name) == float('inf')) |
                        (col(col_name) == float('-inf')),
                        None
                    ).otherwise(col(col_name))
                )
        return df

    @staticmethod
    def validate_symbols(df: DataFrame) -> DataFrame:
        """Validate and clean symbol column"""
        return df.filter(
            col("symbol").isNotNull() &
            (length(col("symbol")) > 0) &
            (length(col("symbol")) <= 10)  # Max symbol length
        )

    @staticmethod
    def remove_duplicates(df: DataFrame, key_columns: List[str]) -> DataFrame:
        """Remove duplicates based on key columns, keeping latest"""
        window_spec = Window.partitionBy(key_columns).orderBy(col("last_updated").desc())

        return df.withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

    @staticmethod
    def validate_ranges(df: DataFrame) -> DataFrame:
        """Validate data ranges for specific columns"""
        # Volatility should be between 0 and 1 (0-100%)
        volatility_cols = ["volatility_24h", "volatility_7d", "volatility_30d", "volatility_90d"]
        for col_name in volatility_cols:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    when(
                        (col(col_name) < 0) | (col(col_name) > 10),  # >1000% volatility is suspicious
                        None
                    ).otherwise(col(col_name))
                )

        # Stability score between 0 and 1
        if "stability_score" in df.columns:
            df = df.withColumn(
                "stability_score",
                when(
                    (col("stability_score") < 0) | (col("stability_score") > 1),
                    None
                ).otherwise(col("stability_score"))
            )

        return df

    @staticmethod
    def log_validation_stats(df: DataFrame, stage: str):
        """Log validation statistics"""
        total_rows = df.count()
        null_counts = {}

        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_counts[col_name] = null_count

        logger.info(f"Validation stats for {stage}:")
        logger.info(f"Total rows: {total_rows}")
        logger.info(f"Columns with nulls: {null_counts}")