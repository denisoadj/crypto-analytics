from pyspark_libraries import *


# Create Spark session - remove Windows paths when running in Docker
spark = SparkSession.builder \
    .appName("bronze_layer") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

process_id = str(uuid.uuid4())

sources = {
    "coingecko": "/app/storage/coin_gecko_raw",
    "wci": "/app/storage/wci_raw",
    "binance": "/app/storage/binance_raw",
    "yahoo": "/app/storage/yahoo_raw",
    "okx": "/app/storage/okx_raw",
    "kraken": "/app/storage/kraken_raw"
}

# Snowflake config
sfOptions = {
    "sfURL": "anppsyn-fv19174.snowflakecomputing.com",
    "sfDatabase": "CRYPTO_DB",
    "sfSchema": "BRONZE",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfUser": "DEN",
    "sfPassword": "InfoTechCons#2025"
}

def check_if_already_loaded(spark, sfOptions, source, process_id):
    """Check if this source and process_id combination already exists"""
    try:
        query = f"""
        SELECT COUNT(*) as count 
        FROM BRONZE_{source.upper()} 
        WHERE source_system = '{source}' 
        AND process_id = '{process_id}'
        """

        existing_df = spark.read \
            .format("net.snowflake.spark.snowflake") \
            .options(**sfOptions) \
            .option("query", query) \
            .load()

        count = existing_df.collect()[0]['count']
        return count > 0
    except Exception as e:
        # Table doesn't exist yet or other error
        print(f"Check failed for {source}: {e}")
        return False

bronze_dfs = {}

# Read all sources
for src, path in sources.items():
    try:
        print(f"\nReading {src} from {path}")
        df_bronze = spark.read.option("mergeSchema", "true").parquet(path)

        df = (
            df_bronze
            .withColumn("load_datetime", current_timestamp())
            .withColumn("process_id", lit(process_id))
            .withColumn("source_system", lit(src))
        )

        bronze_dfs[src] = df
        print(f"Successfully read {src}, row count: {df.count()}")

    except Exception as e:
        print(f"Error reading {src}: {e}")
        continue

# Write to Snowflake with duplicate check
for src, df in bronze_dfs.items():
    try:
        # Check if already loaded
        if check_if_already_loaded(spark, sfOptions, src, process_id):
            print(f"\nSkipping {src} - already loaded with process_id {process_id}")
            continue

        print(f"\nWriting {src} to Snowflake...")

        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sfOptions) \
            .option("dbtable", f"BRONZE_{src.upper()}") \
            .mode("append") \
            .save()

        print(f"Successfully wrote {src} to Snowflake")

    except Exception as e:
        print(f"Error writing {src} to Snowflake: {e}")
        import traceback
        traceback.print_exc()

spark.stop()