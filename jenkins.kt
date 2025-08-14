pipeline {
    agent any
    
    parameters {
        string(name: 'DF1_NAME', defaultValue: '', description: 'Name of first DataFrame in Redis')
        string(name: 'DF2_NAME', defaultValue: '', description: 'Name of second DataFrame in Redis')
    }
    
    environment {
        // Path to global virtual environment
        // add a comment
        GLOBAL_VENV = "/opt/jenkins/spark-dataframe-venv"
    }
    
    stages {
        stage('Validate Parameters') {
            steps {
                script {
                    if (params.DF1_NAME == '' || params.DF2_NAME == '') {
                        error("❌ Both DataFrame names must be provided")
                    }
                    echo "📊 Comparing DataFrames: '${params.DF1_NAME}' vs '${params.DF2_NAME}'"
                }
            }
        }
        
        stage('Create Comparison Script') {
            steps {
                echo "📝 Creating DataFrame comparison script..."
                sh '''
                    # Create the Python script in workspace
                    cat > dataframe_comparator.py << 'EOF'
#!/usr/bin/env python3
"""
DataFrame Comparison Tool for PySpark
Compares two DataFrames stored in Redis cache
Usage: python dataframe_comparator.py <df1_name> <df2_name>
"""

import sys
import os
import redis
import pandas as pd
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
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
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


def compare_dataframes_detailed(df1, df2, name1, name2):
    """
    Detailed comparison of two Spark DataFrames with diagnostics
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
        return False, "row_count_mismatch"
    else:
        print(f"✅ Row counts match: {count1:,} rows")

    # Data comparison
    print()
    print("3️⃣ DATA COMPARISON:")
    print("   Checking for data differences...")

    try:
        # Use exceptAll for efficient comparison
        diff1 = df1.exceptAll(df2)  # Rows in df1 but not in df2
        diff2 = df2.exceptAll(df1)  # Rows in df2 but not in df1

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

            return False, "data_mismatch"
        else:
            print("✅ All data matches perfectly!")

    except Exception as e:
        print(f"❌ Error during data comparison: {e}")
        return False, "comparison_error"

    print()
    print("🎉 RESULT: DataFrames are IDENTICAL!")
    print("✅ Same schema, same row count, same data")
    return True, "identical"


def main():
    """Main function"""
    if len(sys.argv) != 3:
        print("Usage: python dataframe_comparator.py <df1_name> <df2_name>")
        print("Example: python dataframe_comparator.py sales_data_v1 sales_data_v2")
        sys.exit(1)

    df1_name = sys.argv[1]
    df2_name = sys.argv[2]

    print("🚀 Starting DataFrame Comparison")
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

        # Perform comparison
        is_identical, result_type = compare_dataframes_detailed(
            spark_df1, spark_df2, df1_name, df2_name
        )

        # Set exit code based on result
        if is_identical:
            print()
            print("✅ SUCCESS: DataFrames are identical")
            exit_code = 0
        else:
            print()
            print(f"❌ FAILURE: DataFrames differ ({result_type})")
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

EOF
                    chmod +x dataframe_comparator.py
                '''
            }
        }
        
        stage('Run DataFrame Comparison') {
            steps {
                echo "🔄 Running DataFrame comparison..."
                sh '''
                    # Activate global venv and run comparison
                    . "$GLOBAL_VENV/bin/activate"
                    python dataframe_comparator.py "${DF1_NAME}" "${DF2_NAME}"
                '''
            }
        }
    }
    
    post {
        success {
            echo "🎉 DataFrame comparison completed successfully - DataFrames are identical!"
        }
        failure {
            echo "❌ DataFrame comparison failed - DataFrames differ or error occurred"
        }
        always {
            echo "📋 Comparison summary:"
            echo "   DataFrame 1: ${params.DF1_NAME}"
            echo "   DataFrame 2: ${params.DF2_NAME}"
            echo "   Job URL: ${BUILD_URL}"
            echo "   Virtual environment: ${GLOBAL_VENV}"
        }
    }
}
