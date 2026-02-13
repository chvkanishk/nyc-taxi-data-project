"""
Complete pipeline runner
Bronze → Silver → Gold
"""

from pyspark.sql import SparkSession
from src.transformation.bronze_layer import BronzeLayer, create_spark_session
from src.transformation.silver_layer import SilverLayer
from src.transformation.gold_layer import GoldLayer
import os
from datetime import datetime


def run_complete_pipeline():
    """Run the complete medallion pipeline"""
    
    print("\n" + "="*70)
    print("🚀 NYC TAXI DATA PIPELINE - COMPLETE RUN")
    print("="*70 + "\n")
    
    start_time = datetime.now()
    
    # Create Spark session
    print("1️⃣ Initializing Spark...")
    spark = create_spark_session()
    print("✅ Spark initialized\n")
    
    # BRONZE LAYER
    print("2️⃣ BRONZE LAYER: Loading raw data...")
    bronze = BronzeLayer(spark)
    
    months = ["2024-01", "2024-02", "2024-03"]
    for month in months:
        source_file = f"data/raw/yellow_tripdata_{month}.parquet"
        if os.path.exists(source_file):
            bronze.load_raw_to_bronze(source_file, month)
    
    bronze.show_bronze_stats()
    bronze_df = bronze.read_bronze()
    print("✅ Bronze layer complete\n")
    
    # SILVER LAYER
    print("3️⃣ SILVER LAYER: Cleaning and validating...")
    silver = SilverLayer(spark)
    silver_df = silver.transform_bronze_to_silver(bronze_df)
    silver.save_silver(silver_df)
    print("✅ Silver layer complete\n")
    
    # GOLD LAYER
    print("4️⃣ GOLD LAYER: Creating analytics tables...")
    gold = GoldLayer(spark)
    gold_tables = gold.create_all_gold_tables(silver_df)
    print("✅ Gold layer complete\n")
    
    # SUMMARY
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*70)
    print("🎉 PIPELINE COMPLETE!")
    print("="*70)
    print(f"⏱️  Duration: {duration:.1f} seconds")
    print(f"📊 Bronze records: {bronze_df.count():,}")
    print(f"📊 Silver records: {silver_df.count():,}")
    print(f"💎 Gold tables created: {len(gold_tables)}")
    print("="*70 + "\n")
    
    # Cleanup
    spark.stop()


if __name__ == "__main__":
    run_complete_pipeline()