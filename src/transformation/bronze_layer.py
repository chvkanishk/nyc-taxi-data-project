"""
Bronze Layer: Raw data ingestion
Loads raw Parquet files into Delta Lake format
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, 
    col, to_date
)
from delta import configure_spark_with_delta_pip
import os

class BronzeLayer:
    """Handles Bronze layer operations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.bronze_path = "data/processed/bronze"
        
    def load_raw_to_bronze(self, source_path: str, year_month: str):
        """
        Load raw Parquet files to Bronze Delta table
        
        Args:
            source_path: Path to raw Parquet file
            year_month: Year-month identifier (e.g., "2024-01")
        """
        print(f"\n{'='*60}")
        print(f"🔵 BRONZE LAYER: Loading {year_month}")
        print(f"{'='*60}\n")
        
        # Read raw data
        print(f"📖 Reading from: {source_path}")
        df = self.spark.read.parquet(source_path)
        
        original_count = df.count()
        print(f"📊 Original records: {original_count:,}")
        
        # Add metadata columns (audit trail)
        df_with_metadata = df \
            .withColumn("bronze_load_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name()) \
            .withColumn("year_month", lit(year_month)) \
            .withColumn("load_date", to_date(current_timestamp()))
        
        print(f"✅ Added metadata columns")
        
        # Define target path
        target_path = f"{self.bronze_path}/taxi_trips"
        
        # Write to Delta Lake (append mode)
        print(f"💾 Writing to Delta: {target_path}")
        
        df_with_metadata.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year_month") \
            .option("mergeSchema", "true") \
            .save(target_path)
        
        print(f"✅ Bronze layer updated!")
        print(f"📦 Total records written: {original_count:,}\n")
        
        return df_with_metadata
    
    def read_bronze(self):
        """Read entire Bronze layer"""
        bronze_path = f"{self.bronze_path}/taxi_trips"
        return self.spark.read.format("delta").load(bronze_path)
    
    def show_bronze_stats(self):
        """Display Bronze layer statistics"""
        df = self.read_bronze()
        
        print(f"\n{'='*60}")
        print(f"📊 BRONZE LAYER STATISTICS")
        print(f"{'='*60}\n")
        
        total_records = df.count()
        print(f"Total records: {total_records:,}")
        
        # Records by month
        print("\nRecords by month:")
        df.groupBy("year_month") \
            .count() \
            .orderBy("year_month") \
            .show()
        
        # Latest load info
        print("\nLatest loads:")
        df.groupBy("year_month", "load_date") \
            .count() \
            .orderBy(col("load_date").desc()) \
            .show(5)


# 🤔 WHAT'S HAPPENING?
# 
# 1. We read the raw Parquet file
# 2. Add audit columns (when loaded, from where, which month)
# 3. Write to Delta format (enables ACID transactions, time travel)
# 4. Partition by year_month (makes queries faster)
# 5. Use append mode (never overwrite historical data)


def create_spark_session():
    """Create Spark session with Delta Lake support"""
    
    # Get or create builder
    builder = SparkSession.builder \
        .appName("NYC Taxi - Bronze Layer") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]")
    
    # Create session with Delta support
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    return spark


if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()
    
    # Initialize Bronze layer
    bronze = BronzeLayer(spark)
    
    # Load each month of data
    months = ["2024-01", "2024-02", "2024-03"]
    
    for month in months:
        source_file = f"data/raw/yellow_tripdata_{month}.parquet"
        
        if os.path.exists(source_file):
            bronze.load_raw_to_bronze(source_file, month)
        else:
            print(f"⚠️  File not found: {source_file}")
    
    # Show statistics
    bronze.show_bronze_stats()
    
    print("\n✅ Bronze layer creation complete!")
    
    spark.stop()