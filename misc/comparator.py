#!/usr/bin/env python3
"""
DataFrame Comparison Tool for PySpark
Compares two DataFrames stored in Redis cache (ORDER INDEPENDENT)
Usage: python dataframe_comparator.py <df1_name> <df2_name>
"""

import sys
import os
import redis
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    """Create SparkSession connected to dockerized Spark cluster"""
    try:
        import os
        os.environ["SPARK_MASTER_HOST"] = "localhost"
        spark = SparkSession.builder \
            .appName("DataFrameComparator") \
            .master("spark://localhost:7077") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "8g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.driver.host", "192.168.31.254") \
            .getOrCreate()

        print("✅ Successfully connected to Spark cluster")
        print(f"📊 Spark Version: {spark.version}")
        return spark
    except Exception as e:
        print(f"❌ Failed to connect to Spark: {e}")
        sys.exit(1)


def connect_redis():
    """Connect to Redis server"""
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
        redis_client.ping()
        print("✅ Successfully connected to Redis")
        return redis_client
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        sys.exit(1)


def load_dataframe_from_redis(redis_client, name):
    """Load DataFrame from Redis cache (CSV format)"""
    try:
        # Get CSV data
        key = f"df:{name}"
        csv_data = redis_client.get(key)

        if csv_data is None:
            print(f"❌ DataFrame '{name}' not found in Redis")
            return None

        # Convert bytes to string if necessary
        if isinstance(csv_data, bytes):
            csv_data = csv_data.decode('utf-8')

        # Load CSV into pandas DataFrame
        import io
        df_data = pd.read_csv(io.StringIO(csv_data))
        print(f"✅ Successfully loaded '{name}' from CSV format")

        # Get metadata (JSON format)
        meta_key = f"meta:{name}"
        meta_data = redis_client.get(meta_key)
        if meta_data:
            try:
                import json
                if isinstance(meta_data, bytes):
                    meta_data = meta_data.decode('utf-8')
                metadata = json.loads(meta_data)
                print(f"📊 Loaded '{name}': {metadata.get('rows', 'unknown')} rows x {metadata.get('cols', 'unknown')} columns")
            except Exception as meta_error:
                print(f"⚠️  Could not load metadata for '{name}': {meta_error}")

        return df_data

    except Exception as e:
        print(f"❌ Error loading DataFrame '{name}': {e}")
        return None


def pandas_to_spark(spark, pandas_df, name):
    """Convert Pandas DataFrame to Spark DataFrame"""
    try:
        spark_df = spark.createDataFrame(pandas_df)
        print(f"✅ Converted '{name}' to Spark DataFrame")
        return spark_df
    except Exception as e:
        print(f"❌ Error converting '{name}' to Spark DataFrame: {e}")
        return None


def spark_to_pandas(spark_df, name):
    """Convert Spark DataFrame to Pandas DataFrame"""
    try:
        pandas_df = spark_df.toPandas()
        print(f"✅ Converted '{name}' back to Pandas DataFrame")
        return pandas_df
    except Exception as e:
        print(f"❌ Error converting '{name}' to Pandas DataFrame: {e}")
        return None


def cache_dataframe_to_redis(redis_client, pandas_df, name, suffix="_diff"):
    """Cache DataFrame to Redis in CSV format with metadata"""
    try:
        cache_name = f"{name}{suffix}"

        # Convert DataFrame to CSV string
        csv_string = pandas_df.to_csv(index=False)

        # Store CSV data
        df_key = f"df:{cache_name}"
        redis_client.set(df_key, csv_string)

        # Create and store metadata
        metadata = {
            "name": cache_name,
            "rows": len(pandas_df),
            "cols": len(pandas_df.columns),
            "columns": list(pandas_df.columns),
            "dtypes": {col: str(dtype) for col, dtype in pandas_df.dtypes.items()},
            "cached_at": pd.Timestamp.now().isoformat(),
            "source": "dataframe_comparison_diff"
        }

        meta_key = f"meta:{cache_name}"
        redis_client.set(meta_key, json.dumps(metadata))

        # Add to dataframe index
        redis_client.sadd("dataframe_index", cache_name)

        print(f"💾 Cached '{cache_name}' to Redis:")
        print(f"   📊 Size: {metadata['rows']} rows x {metadata['cols']} columns")
        print(f"   🔑 Keys: {df_key}, {meta_key}")

        return cache_name

    except Exception as e:
        print(f"❌ Error caching DataFrame '{name}': {e}")
        return None


def compare_dataframes_detailed(df1, df2, name1, name2, redis_client):
    """
    Detailed comparison of two Spark DataFrames with diagnostics (ORDER INDEPENDENT)
    Caches DataFrames to Redis when they differ
    """
    print()
    print("=" * 50)
    print(f"🔍 COMPARING DATAFRAMES: '{name1}' vs '{name2}'")
    print("=" * 50)

    # Schema comparison
    print()
    print("1️⃣ SCHEMA COMPARISON:")
    if df1.schema != df2.schema:
        print("❌ Schemas differ")
        print(f"'{name1}' schema:")
        df1.printSchema()
        print(f"'{name2}' schema:")
        df2.printSchema()
        return False, "schema_mismatch"
    else:
        print("✅ Schemas match")
        print(f"   Columns: {len(df1.columns)}")
        for col in df1.columns:
            print(f"   - {col}: {dict(df1.dtypes)[col]}")

    # Row count comparison
    print()
    print("2️⃣ ROW COUNT COMPARISON:")
    count1, count2 = df1.count(), df2.count()
    if count1 != count2:
        print(f"❌ Row counts differ: {count1} vs {count2}")
        print(f"   Difference: {abs(count1 - count2)} rows")
        print("   Note: Row count differences indicate structural differences.")
        print("   Full DataFrames would need to be analyzed separately.")
        return False, "row_count_mismatch"
    else:
        print(f"✅ Row counts match: {count1:,} rows")

    # Data comparison (ORDER INDEPENDENT)
    print()
    print("3️⃣ DATA COMPARISON (ORDER INDEPENDENT):")
    print("   Sorting both DataFrames for order-independent comparison...")

    try:
        # Sort both DataFrames by all columns to ensure consistent ordering
        all_columns = df1.columns
        print(f"   Sorting by columns: {all_columns}")

        sorted_df1 = df1.orderBy(*all_columns)
        sorted_df2 = df2.orderBy(*all_columns)

        print("   Checking for data differences...")

        # Use exceptAll for efficient comparison on sorted DataFrames
        diff1 = sorted_df1.exceptAll(sorted_df2)  # Rows in df1 but not in df2
        diff2 = sorted_df2.exceptAll(sorted_df1)  # Rows in df2 but not in df1

        diff1_count = diff1.count()
        diff2_count = diff2.count()

        if diff1_count > 0 or diff2_count > 0:
            print(f"❌ Data differs:")
            print(f"   Rows unique to '{name1}': {diff1_count}")
            print(f"   Rows unique to '{name2}': {diff2_count}")

            # Show sample differences (first 10 rows)
            if diff1_count > 0:
                print(f"   Sample rows unique to '{name1}':")
                diff1.show(10, truncate=False)

            if diff2_count > 0:
                print(f"   Sample rows unique to '{name2}':")
                diff2.show(10, truncate=False)

            # Cache ONLY the differing rows (not the full DataFrames)
            print()
            print("💾 Caching ONLY the differing rows for analysis...")

            # Cache rows unique to first DataFrame
            if diff1_count > 0 and diff1_count <= 100000:  # Only cache if reasonable size
                print(f"💾 Caching {diff1_count} rows unique to '{name1}'...")
                pandas_diff1 = spark_to_pandas(diff1, f"{name1}_unique")
                if pandas_diff1 is not None:
                    cached_diff1 = cache_dataframe_to_redis(redis_client, pandas_diff1, name1, "_unique_rows")
                    if cached_diff1:
                        print(f"✅ Cached unique rows as '{cached_diff1}'")
            elif diff1_count > 100000:
                print(f"⚠️  Skipping cache of {diff1_count} unique rows from '{name1}' (too large)")

            # Cache rows unique to second DataFrame
            if diff2_count > 0 and diff2_count <= 100000:  # Only cache if reasonable size
                print(f"💾 Caching {diff2_count} rows unique to '{name2}'...")
                pandas_diff2 = spark_to_pandas(diff2, f"{name2}_unique")
                if pandas_diff2 is not None:
                    cached_diff2 = cache_dataframe_to_redis(redis_client, pandas_diff2, name2, "_unique_rows")
                    if cached_diff2:
                        print(f"✅ Cached unique rows as '{cached_diff2}'")
            elif diff2_count > 100000:
                print(f"⚠️  Skipping cache of {diff2_count} unique rows from '{name2}' (too large)")

            return False, "data_mismatch"
        else:
            print("✅ All data matches perfectly!")

    except Exception as e:
        print(f"❌ Error during data comparison: {e}")
        print("   Falling back to alternative comparison method...")

        try:
            # Alternative method: Convert to sets for comparison
            print("   Using hash-based comparison...")

            # Create hash for each row and compare
            from pyspark.sql.functions import hash as spark_hash, concat_ws

            # Create row hashes
            df1_hashed = df1.withColumn("row_hash", spark_hash(concat_ws("|", *all_columns)))
            df2_hashed = df2.withColumn("row_hash", spark_hash(concat_ws("|", *all_columns)))

            # Get distinct hashes
            hashes1 = set([row.row_hash for row in df1_hashed.select("row_hash").distinct().collect()])
            hashes2 = set([row.row_hash for row in df2_hashed.select("row_hash").distinct().collect()])

            if hashes1 != hashes2:
                unique_to_df1 = len(hashes1 - hashes2)
                unique_to_df2 = len(hashes2 - hashes1)
                print(f"❌ Data differs (hash comparison):")
                print(f"   Unique row patterns in '{name1}': {unique_to_df1}")
                print(f"   Unique row patterns in '{name2}': {unique_to_df2}")
                print("   Note: Hash comparison detected differences but couldn't isolate specific rows.")
                print("   Consider using a more detailed comparison method for analysis.")
                return False, "data_mismatch_hash"
            else:
                print("✅ All data matches perfectly (verified by hash)!")

        except Exception as e2:
            print(f"❌ Alternative comparison also failed: {e2}")
            print("   Unable to perform detailed comparison due to technical issues.")
            return False, "comparison_error"

    print()
    print("🎉 RESULT: DataFrames are IDENTICAL!")
    print("✅ Same schema, same row count, same data (ignoring row order)")
    return True, "identical"


def main():
    """Main function"""
    if len(sys.argv) != 3:
        print("Usage: python dataframe_comparator.py <df1_name> <df2_name>")
        print("Example: python dataframe_comparator.py sales_data_v1 sales_data_v2")
        sys.exit(1)

    df1_name = sys.argv[1]
    df2_name = sys.argv[2]

    print("🚀 Starting DataFrame Comparison (ORDER INDEPENDENT)")
    print(f"📋 Comparing: '{df1_name}' vs '{df2_name}'")

    # Initialize connections
    spark = create_spark_session()
    redis_client = connect_redis()

    try:
        # Load DataFrames from Redis
        print()
        print("📥 Loading DataFrames from Redis...")

        # List available DataFrames
        available_dfs = redis_client.smembers("dataframe_index")
        if available_dfs:
            available_names = [name.decode('utf-8') if isinstance(name, bytes) else name for name in available_dfs]
            print(f"📋 Available DataFrames: {available_names}")

        pandas_df1 = load_dataframe_from_redis(redis_client, df1_name)
        pandas_df2 = load_dataframe_from_redis(redis_client, df2_name)

        if pandas_df1 is None or pandas_df2 is None:
            print("❌ Failed to load one or both DataFrames")
            sys.exit(1)

        # Convert to Spark DataFrames
        print()
        print("🔄 Converting to Spark DataFrames...")
        spark_df1 = pandas_to_spark(spark, pandas_df1, df1_name)
        spark_df2 = pandas_to_spark(spark, pandas_df2, df2_name)

        if spark_df1 is None or spark_df2 is None:
            print("❌ Failed to convert one or both DataFrames to Spark")
            sys.exit(1)

        # Perform comparison (now with Redis client for caching)
        is_identical, result_type = compare_dataframes_detailed(
            spark_df1, spark_df2, df1_name, df2_name, redis_client
        )

        # Set exit code based on result
        if is_identical:
            print()
            print("✅ SUCCESS: DataFrames are identical (ignoring row order)")
            exit_code = 0
        else:
            print()
            print(f"❌ FAILURE: DataFrames differ ({result_type})")
            if result_type == "data_mismatch":
                print("💾 Differing rows have been cached to Redis for analysis")
            exit_code = 1

    except Exception as e:
        print()
        print(f"💥 UNEXPECTED ERROR: {e}")
        exit_code = 2

    finally:
        # Cleanup
        print()
        print("🧹 Cleaning up...")
        spark.stop()
        redis_client.close()
        print("👋 Done!")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
