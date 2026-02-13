# src/transformation/silver_layer.py
"""
Silver Layer: Cleaned and validated data
COMPLETE FIXED VERSION - Windows Hadoop fix + expr() fix
"""

import os
import sys

# ===== WINDOWS HADOOP FIX =====
os.environ['HADOOP_HOME'] = r'C:\hadoop'
if r'C:\hadoop\bin' not in sys.path:
    sys.path.insert(0, r'C:\hadoop\bin')
# ==============================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, hour, dayofweek, month, year, 
    when, round as spark_round, unix_timestamp, lit, expr  # ADDED expr HERE
)
from typing import List, Tuple


class DataQualityChecker:
    """Data quality validation and cleaning"""
    
    @staticmethod
    def get_validation_rules() -> List[Tuple[str, str, str]]:
        """Define validation rules"""
        return [
            ("valid_trip_distance", "(trip_distance >= 0) AND (trip_distance <= 100)", "Trip distance between 0 and 100 miles"),
            ("valid_passenger_count", "(passenger_count >= 1) AND (passenger_count <= 6)", "Passenger count between 1 and 6"),
            ("valid_fare_amount", "(fare_amount >= 0) AND (fare_amount <= 500)", "Fare amount between $0 and $500"),
            ("valid_total_amount", "(total_amount >= 0) AND (total_amount <= 1000)", "Total amount between $0 and $1000"),
            ("valid_pickup_datetime", "tpep_pickup_datetime IS NOT NULL", "Pickup datetime must exist"),
            ("valid_dropoff_datetime", "tpep_dropoff_datetime IS NOT NULL", "Dropoff datetime must exist"),
            ("valid_trip_duration", "tpep_dropoff_datetime > tpep_pickup_datetime", "Dropoff must be after pickup"),
            ("valid_locations", "(PULocationID IS NOT NULL) AND (DOLocationID IS NOT NULL)", "Pickup and dropoff locations must exist"),
        ]
    
    @staticmethod
    def apply_validation(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Apply validation rules and separate valid/invalid records"""
        rules = DataQualityChecker.get_validation_rules()
        
        all_conditions = [rule[1] for rule in rules]
        combined_condition = " AND ".join([f"({cond})" for cond in all_conditions])
        
        valid_df = df.filter(combined_condition)
        invalid_df = df.filter(f"NOT ({combined_condition})")
        
        # FIXED: Use expr() instead of col()
        for rule_name, condition, _ in rules:
            invalid_df = invalid_df.withColumn(
                f"failed_{rule_name}",
                when(~expr(condition), lit(True)).otherwise(lit(False))  # ✅ FIXED with expr()
            )
        
        return valid_df, invalid_df
    
    @staticmethod
    def clean_data(df: DataFrame) -> DataFrame:
        """Clean and standardize data"""
        cleaned_df = df
        
        cleaned_df = cleaned_df.fillna({
            'passenger_count': 1,
            'RatecodeID': 1,
            'store_and_fwd_flag': 'N',
            'extra': 0.0,
            'mta_tax': 0.0,
            'tip_amount': 0.0,
            'tolls_amount': 0.0,
            'improvement_surcharge': 0.0,
            'congestion_surcharge': 0.0,
            'airport_fee': 0.0
        })
        
        cleaned_df = cleaned_df.withColumn(
            "store_and_fwd_flag",
            when(col("store_and_fwd_flag").isin(["Y", "y", "1"]), "Y").otherwise("N")
        )
        
        cleaned_df = cleaned_df \
            .withColumn("trip_distance", when(col("trip_distance") > 100, 100.0).when(col("trip_distance") < 0, 0.0).otherwise(col("trip_distance"))) \
            .withColumn("fare_amount", when(col("fare_amount") > 500, 500.0).when(col("fare_amount") < 0, 0.0).otherwise(col("fare_amount")))
        
        return cleaned_df


class SilverLayer:
    """Handles Silver layer transformations"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.silver_path = "data/processed/silver"
        self.quality_checker = DataQualityChecker()
    
    def transform_bronze_to_silver(self, bronze_df):
        """Transform Bronze data to Silver"""
        print(f"\n{'='*60}")
        print(f"⚪ SILVER LAYER: Transforming data")
        print(f"{'='*60}\n")
        
        initial_count = bronze_df.count()
        print(f"📊 Input records: {initial_count:,}")
        
        print("\n1️⃣ Validating data quality...")
        valid_df, invalid_df = self.quality_checker.apply_validation(bronze_df)
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        print(f"   ✅ Valid records: {valid_count:,} ({valid_count/initial_count*100:.1f}%)")
        print(f"   ❌ Invalid records: {invalid_count:,} ({invalid_count/initial_count*100:.1f}%)")
        
        if invalid_count > 0:
            quarantine_path = f"{self.silver_path}/quarantine"
            invalid_df.write.format("delta").mode("append").save(quarantine_path)
            print(f"   🗃️  Invalid records saved to quarantine")
        
        print("\n2️⃣ Cleaning data...")
        cleaned_df = self.quality_checker.clean_data(valid_df)
        print(f"   ✅ Data cleaned")
        
        print("\n3️⃣ Engineering features...")
        enriched_df = self._add_derived_columns(cleaned_df)
        print(f"   ✅ Features added")
        
        print("\n4️⃣ Removing duplicates...")
        deduplicated_df = self._deduplicate(enriched_df)
        final_count = deduplicated_df.count()
        duplicates = valid_count - final_count
        print(f"   ✅ Removed {duplicates:,} duplicates")
        
        final_df = deduplicated_df.withColumn("silver_load_timestamp", current_timestamp())
        
        print(f"\n📦 Final Silver records: {final_count:,}")
        
        return final_df
    
    def _add_derived_columns(self, df):
        """Add calculated columns"""
        return df \
            .withColumn("trip_duration_minutes", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60) \
            .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
            .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime")) \
            .withColumn("pickup_month", month("tpep_pickup_datetime")) \
            .withColumn("pickup_year", year("tpep_pickup_datetime")) \
            .withColumn("is_weekend", when(col("pickup_day_of_week").isin([1, 7]), True).otherwise(False)) \
            .withColumn("speed_mph", when(col("trip_duration_minutes") > 0, spark_round((col("trip_distance") / col("trip_duration_minutes")) * 60, 2)).otherwise(0.0)) \
            .withColumn("fare_per_mile", when(col("trip_distance") > 0, spark_round(col("fare_amount") / col("trip_distance"), 2)).otherwise(0.0)) \
            .withColumn("tip_percentage", when(col("fare_amount") > 0, spark_round((col("tip_amount") / col("fare_amount")) * 100, 2)).otherwise(0.0)) \
            .withColumn("time_of_day", 
                when(col("pickup_hour").between(6, 11), "Morning")
                .when(col("pickup_hour").between(12, 16), "Afternoon")
                .when(col("pickup_hour").between(17, 20), "Evening")
                .otherwise("Night"))
    
    def _deduplicate(self, df):
        """Remove duplicate records"""
        return df.dropDuplicates(["tpep_pickup_datetime", "PULocationID", "DOLocationID", "total_amount"])
    
    def save_silver(self, df):
        """Save to Silver Delta table"""
        target_path = f"{self.silver_path}/cleaned_trips"
        
        print(f"\n💾 Saving to Silver: {target_path}")
        
        df.write.format("delta").mode("overwrite").partitionBy("pickup_year", "pickup_month").save(target_path)
        
        print(f"✅ Silver layer saved!")
    
    def read_silver(self):
        """Read Silver layer"""
        return self.spark.read.format("delta").load(f"{self.silver_path}/cleaned_trips")


def create_spark_session():
    """Create Spark session with Delta Lake support"""
    print("🚀 Creating Spark session with Delta Lake...")
    
    spark = SparkSession.builder \
        .appName("NYC Taxi - Silver Layer") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"✅ Spark {spark.version} initialized with Delta Lake support\n")
    
    return spark


if __name__ == "__main__":
    from bronze_layer import BronzeLayer
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read Bronze layer
    bronze = BronzeLayer(spark)
    bronze_df = bronze.read_bronze()
    
    # Transform to Silver
    silver = SilverLayer(spark)
    silver_df = silver.transform_bronze_to_silver(bronze_df)
    
    # Save Silver layer
    silver.save_silver(silver_df)
    
    # Show sample
    print("\n📊 Sample Silver records:")
    silver_df.select(
        "tpep_pickup_datetime",
        "trip_distance",
        "trip_duration_minutes",
        "speed_mph",
        "fare_amount",
        "tip_percentage",
        "time_of_day"
    ).show(5)
    
    print("\n✅ Silver layer creation complete!")
    
    spark.stop()