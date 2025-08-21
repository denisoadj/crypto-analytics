from pyspark_libraries import *


# Initialize Spark with Snowflake connector
spark = SparkSession.builder \
    .appName("CryptoCDC_Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.11.1-spark_3.3") \
    .getOrCreate()

# # Snowflake connection options
# SNOWFLAKE_OPTIONS = {
#     "sfURL": "<your_snowflake_account>.snowflakecomputing.com",
#     "sfDatabase": "<your_database>",
#     "sfSchema": "BRONZE",
#     "sfWarehouse": "<your_warehouse>",
#     "sfRole": "<your_role>",
#     "sfUser": "<your_username>",
#     "sfPassword": "<your_password>"
# }
#
# # Local or cloud storage directory with parquet files
# SOURCE_PATHS = {
#     "coingecko": "/mnt/data/parquet/coingecko/",
#     "world_coin_index": "/mnt/data/parquet/world_coin_index/",
#     "binance": "/mnt/data/parquet/binance/",
#     "yahoo_finance": "/mnt/data/parquet/yahoo_finance/",
#     "okx": "/mnt/data/parquet/okx/",
#     "kraken": "/mnt/data/parquet/kraken/"
# }
#
# def add_cdc_metadata(df, source_name):
#     """
#     Add CDC metadata columns:
#     - process_id: unique UUID per batch
#     - load_timestamp: when loaded into Bronze
#     - source_system: original source
#     - row_hash: fingerprint for CDC detection
#     """
#     process_id = str(uuid.uuid4())
#     df = df.withColumn("process_id", lit(process_id)) \
#            .withColumn("load_timestamp", current_timestamp()) \
#            .withColumn("source_system", lit(source_name)) \
#            .withColumn("row_hash", md5(concat_ws("||", *df.columns)))
#     return df
#
# # Loop through each source, load parquet, add metadata, and write to Snowflake
# for source, path in SOURCE_PATHS.items():
#     print(f"Processing {source}...")
#
#     # Read parquet
#     df = spark.read.parquet(path)
#
#     # Add CDC + lineage metadata
#     df_with_meta = add_cdc_metadata(df, source)
#
#     # Write to Snowflake Bronze schema (append mode for CDC archival)
#     df_with_meta.write \
#         .format("snowflake") \
#         .options(**SNOWFLAKE_OPTIONS) \
#         .option("dbtable", f"{source.upper()}_BRONZE") \
#         .mode("append") \
#         .save()
#
#     print(f"✅ {source} ingested into Snowflake Bronze layer")

spark.stop()
