"""
Complete NYC Taxi Pipeline Runner
Runs Bronze → Silver → Gold
"""

import os
import sys
from datetime import datetime

# Windows fix
os.environ['HADOOP_HOME'] = r'C:\hadoop'
sys.path.insert(0, r'C:\hadoop\bin')

from pyspark.sql import SparkSession
from src.transformation.bronze_layer import BronzeLayer
from src.transformation.silver_layer import SilverLayer
from src.transformation.gold_layer import GoldLayer


def create_spark_session():
    return SparkSession.builder \
        .appName("NYC Taxi - Complete Pipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .master("local[*]") \
        .getOrCreate()


def run_pipeline():
    print("\n" + "="*70)
    print("🚀 NYC TAXI DATA PIPELINE - COMPLETE RUN")
    print("="*70 + "\n")
    
    start_time = datetime.now()
    
    # Initialize Spark
    print("1️⃣ Initializing Spark...")
    spark = create_spark_session()
    print("✅ Spark initialized\n")
    
    # BRONZE
    print("2️⃣ BRONZE LAYER...")
    bronze = BronzeLayer(spark)
    
    months = ["2024-01", "2024-02", "2024-03"]
    for month in months:
        source_file = f"data/raw/yellow_tripdata_{month}.parquet"
        if os.path.exists(source_file):
            bronze.load_raw_to_bronze(source_file, month)
    
    bronze_df = bronze.read_bronze()
    bronze_count = bronze_df.count()
    print(f"✅ Bronze complete: {bronze_count:,} records\n")
    
    # SILVER
    print("3️⃣ SILVER LAYER...")
    silver = SilverLayer(spark)
    silver_df = silver.transform_bronze_to_silver(bronze_df)
    silver.save_silver(silver_df)
    silver_count = silver_df.count()
    print(f"✅ Silver complete: {silver_count:,} records\n")
    
    # GOLD
    print("4️⃣ GOLD LAYER...")
    gold = GoldLayer(spark)
    tables = gold.create_all_gold_tables(silver_df)
    print(f"✅ Gold complete: {len(tables)} tables created\n")
    
    # SUMMARY
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*70)
    print("🎉 PIPELINE COMPLETE!")
    print("="*70)
    print(f"⏱️  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print(f"📊 Bronze records: {bronze_count:,}")
    print(f"📊 Silver records: {silver_count:,}")
    print(f"💎 Gold tables: {len(tables)}")
    print(f"🗑️  Records filtered: {bronze_count - silver_count:,} ({(bronze_count-silver_count)/bronze_count*100:.1f}%)")
    print("="*70 + "\n")
    
    # Next steps
    print("📊 Next Steps:")
    print("1. Run analytics: python notebooks/02_analytics_queries.py")
    print("2. View dashboard: python dashboards/nyc_taxi_dashboard.py")
    print("3. Explore data in notebooks/")
    
    spark.stop()


if __name__ == "__main__":
    run_pipeline()