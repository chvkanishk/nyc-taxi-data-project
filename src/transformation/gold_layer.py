"""
Gold Layer: Business-ready analytics tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, max as spark_max, 
    min as spark_min, round as spark_round, when
)
from pyspark.sql.window import Window


class GoldLayer:
    """Handles Gold layer analytics tables"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.gold_path = "data/processed/gold"
    
    def create_fact_trips(self, silver_df):
        """
        Create fact_trips table
        This is the main transaction table for analysis
        """
        print(f"\n{'='*60}")
        print(f"💎 GOLD LAYER: Creating fact_trips")
        print(f"{'='*60}\n")
        
        fact_trips = silver_df.select(
            # Keys
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            col("PULocationID").alias("pickup_location_id"),
            col("DOLocationID").alias("dropoff_location_id"),
            
            # Dimensions
            col("VendorID").alias("vendor_id"),
            col("RatecodeID").alias("rate_code_id"),
            col("payment_type"),
            col("passenger_count"),
            
            # Measures (metrics)
            col("trip_distance"),
            col("trip_duration_minutes"),
            col("speed_mph"),
            col("fare_amount"),
            col("extra"),
            col("mta_tax"),
            col("tip_amount"),
            col("tolls_amount"),
            col("total_amount"),
            col("fare_per_mile"),
            col("tip_percentage"),
            
            # Time attributes
            col("pickup_hour"),
            col("pickup_day_of_week"),
            col("pickup_month"),
            col("pickup_year"),
            col("is_weekend"),
            col("time_of_day")
        )
        
        # Save
        target_path = f"{self.gold_path}/fact_trips"
        fact_trips.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("pickup_year", "pickup_month") \
            .save(target_path)
        
        print(f"✅ fact_trips created: {fact_trips.count():,} records")
        return fact_trips
    
    def create_daily_metrics(self, fact_trips):
        """
        Aggregate to daily level
        Perfect for trend analysis and dashboards
        """
        print(f"\n💎 Creating daily_metrics...")
        
        daily_metrics = fact_trips \
            .groupBy("pickup_year", "pickup_month", "pickup_day_of_week") \
            .agg(
                count("*").alias("total_trips"),
                spark_sum("total_amount").alias("total_revenue"),
                spark_sum("fare_amount").alias("total_fare"),
                spark_sum("tip_amount").alias("total_tips"),
                avg("trip_distance").alias("avg_trip_distance"),
                avg("trip_duration_minutes").alias("avg_trip_duration"),
                avg("speed_mph").alias("avg_speed"),
                avg("fare_per_mile").alias("avg_fare_per_mile"),
                avg("tip_percentage").alias("avg_tip_percentage"),
                avg("passenger_count").alias("avg_passengers")
            ) \
            .select(
                "pickup_year",
                "pickup_month",
                "pickup_day_of_week",
                "total_trips",
                spark_round("total_revenue", 2).alias("total_revenue"),
                spark_round("total_fare", 2).alias("total_fare"),
                spark_round("total_tips", 2).alias("total_tips"),
                spark_round("avg_trip_distance", 2).alias("avg_trip_distance"),
                spark_round("avg_trip_duration", 2).alias("avg_trip_duration"),
                spark_round("avg_speed", 2).alias("avg_speed"),
                spark_round("avg_fare_per_mile", 2).alias("avg_fare_per_mile"),
                spark_round("avg_tip_percentage", 2).alias("avg_tip_percentage"),
                spark_round("avg_passengers", 2).alias("avg_passengers")
            )
        
        # Save
        target_path = f"{self.gold_path}/daily_metrics"
        daily_metrics.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_path)
        
        print(f"✅ daily_metrics created: {daily_metrics.count():,} records")
        return daily_metrics
    
    def create_hourly_demand(self, fact_trips):
        """
        Hourly demand patterns
        Used for: Demand forecasting, surge pricing analysis
        """
        print(f"\n💎 Creating hourly_demand...")
        
        hourly_demand = fact_trips \
            .groupBy("pickup_hour", "is_weekend", "time_of_day") \
            .agg(
                count("*").alias("total_trips"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("trip_distance").alias("avg_trip_distance"),
                avg("fare_amount").alias("avg_fare")
            ) \
            .select(
                "pickup_hour",
                "is_weekend",
                "time_of_day",
                "total_trips",
                spark_round("total_revenue", 2).alias("total_revenue"),
                spark_round("avg_trip_distance", 2).alias("avg_trip_distance"),
                spark_round("avg_fare", 2).alias("avg_fare")
            ) \
            .orderBy("pickup_hour")
        
        # Save
        target_path = f"{self.gold_path}/hourly_demand"
        hourly_demand.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_path)
        
        print(f"✅ hourly_demand created: {hourly_demand.count():,} records")
        return hourly_demand
    
    def create_zone_analytics(self, fact_trips):
        """
        Analysis by pickup/dropoff zones
        Used for: Route optimization, hot zone identification
        """
        print(f"\n💎 Creating zone_analytics...")
        
        # Pickup zone stats
        pickup_stats = fact_trips \
            .groupBy("pickup_location_id") \
            .agg(
                count("*").alias("total_pickups"),
                spark_sum("total_amount").alias("total_revenue_from_zone"),
                avg("trip_distance").alias("avg_trip_distance_from_zone"),
                avg("fare_amount").alias("avg_fare_from_zone")
            ) \
            .select(
                col("pickup_location_id").alias("location_id"),
                "total_pickups",
                spark_round("total_revenue_from_zone", 2).alias("total_revenue_from_zone"),
                spark_round("avg_trip_distance_from_zone", 2).alias("avg_trip_distance_from_zone"),
                spark_round("avg_fare_from_zone", 2).alias("avg_fare_from_zone")
            )
        
        # Dropoff zone stats
        dropoff_stats = fact_trips \
            .groupBy("dropoff_location_id") \
            .agg(
                count("*").alias("total_dropoffs"),
                avg("trip_distance").alias("avg_trip_distance_to_zone"),
                avg("fare_amount").alias("avg_fare_to_zone")
            ) \
            .select(
                col("dropoff_location_id").alias("location_id"),
                "total_dropoffs",
                spark_round("avg_trip_distance_to_zone", 2).alias("avg_trip_distance_to_zone"),
                spark_round("avg_fare_to_zone", 2).alias("avg_fare_to_zone")
            )
        
        # Combine
        zone_analytics = pickup_stats.join(
            dropoff_stats,
            on="location_id",
            how="outer"
        ).fillna(0)
        
        # Add zone activity score
        zone_analytics = zone_analytics.withColumn(
            "zone_activity_score",
            spark_round(col("total_pickups") + col("total_dropoffs"), 0)
        )
        
        # Save
        target_path = f"{self.gold_path}/zone_analytics"
        zone_analytics.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_path)
        
        print(f"✅ zone_analytics created: {zone_analytics.count():,} records")
        return zone_analytics
    
    def create_payment_analysis(self, fact_trips):
        """
        Payment type analysis
        """
        print(f"\n💎 Creating payment_analysis...")
        
        payment_analysis = fact_trips \
            .groupBy("payment_type") \
            .agg(
                count("*").alias("total_trips"),
                spark_sum("total_amount").alias("total_revenue"),
                avg("tip_amount").alias("avg_tip"),
                avg("tip_percentage").alias("avg_tip_percentage")
            ) \
            .select(
                "payment_type",
                "total_trips",
                spark_round("total_revenue", 2).alias("total_revenue"),
                spark_round("avg_tip", 2).alias("avg_tip"),
                spark_round("avg_tip_percentage", 2).alias("avg_tip_percentage")
            ) \
            .orderBy(col("total_trips").desc())
        
        # Save
        target_path = f"{self.gold_path}/payment_analysis"
        payment_analysis.write \
            .format("delta") \
            .mode("overwrite") \
            .save(target_path)
        
        print(f"✅ payment_analysis created: {payment_analysis.count():,} records")
        return payment_analysis
    
    def create_all_gold_tables(self, silver_df):
        """Create all Gold tables at once"""
        
        print(f"\n{'='*60}")
        print(f"💎 GOLD LAYER: Creating all analytics tables")
        print(f"{'='*60}\n")
        
        # 1. Fact table
        fact_trips = self.create_fact_trips(silver_df)
        
        # 2. Aggregate tables
        daily_metrics = self.create_daily_metrics(fact_trips)
        hourly_demand = self.create_hourly_demand(fact_trips)
        zone_analytics = self.create_zone_analytics(fact_trips)
        payment_analysis = self.create_payment_analysis(fact_trips)
        
        print(f"\n{'='*60}")
        print(f"✅ ALL GOLD TABLES CREATED!")
        print(f"{'='*60}\n")
        
        return {
            'fact_trips': fact_trips,
            'daily_metrics': daily_metrics,
            'hourly_demand': hourly_demand,
            'zone_analytics': zone_analytics,
            'payment_analysis': payment_analysis
        }


# 🤔 WHAT'S HAPPENING?
#
# We're creating specialized tables for different analyses:
#
# 1. fact_trips: Main transaction table (still detailed)
# 2. daily_metrics: Aggregated by day (trends over time)
# 3. hourly_demand: Aggregated by hour (demand patterns)
# 4. zone_analytics: Aggregated by location (geography)
# 5. payment_analysis: Aggregated by payment type
#
# Why multiple tables?
# - Faster queries (pre-aggregated)
# - Easier for analysts to use
# - Optimized for different questions


if __name__ == "__main__":
    from bronze_layer import create_spark_session
    from silver_layer import SilverLayer
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read Silver layer
    silver = SilverLayer(spark)
    silver_df = silver.read_silver()
    
    # Create Gold tables
    gold = GoldLayer(spark)
    tables = gold.create_all_gold_tables(silver_df)
    
    # Show samples
    print("\n📊 GOLD TABLE SAMPLES:\n")
    
    print("1. Daily Metrics (first 5 days):")
    tables['daily_metrics'].show(5)
    
    print("\n2. Hourly Demand (first 10 hours):")
    tables['hourly_demand'].show(10)
    
    print("\n3. Top 10 Zones by Activity:")
    tables['zone_analytics'] \
        .orderBy(col("zone_activity_score").desc()) \
        .show(10)
    
    print("\n4. Payment Type Analysis:")
    tables['payment_analysis'].show()
    
    spark.stop()