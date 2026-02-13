"""
NYC Taxi Analytics Queries
Explore insights from Gold layer
"""

import os
import sys

# Windows Hadoop fix
os.environ['HADOOP_HOME'] = r'C:\hadoop'
sys.path.insert(0, r'C:\hadoop\bin')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import matplotlib.pyplot as plt
import pandas as pd

# Create Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Analytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .master("local[*]") \
    .getOrCreate()

print("✅ Spark session created\n")

# ===========================================
# INSIGHT 1: Revenue Trends Over Time
# ===========================================

print("📊 INSIGHT 1: Revenue Trends Over Time")
print("="*60)

revenue_trend = spark.read.format("delta").load("data/processed/gold/daily_metrics")

revenue_by_month = revenue_trend \
    .groupBy("pickup_year", "pickup_month") \
    .agg({"total_revenue": "sum", "total_trips": "sum"}) \
    .orderBy("pickup_year", "pickup_month")

revenue_by_month.show()

# Convert to Pandas for plotting
pdf = revenue_by_month.toPandas()
pdf['month_label'] = pdf['pickup_year'].astype(str) + '-' + pdf['pickup_month'].astype(str).str.zfill(2)

plt.figure(figsize=(12, 6))
plt.plot(pdf['month_label'], pdf['sum(total_revenue)'], marker='o', linewidth=2)
plt.title('Monthly Revenue Trend', fontsize=16)
plt.xlabel('Month')
plt.ylabel('Total Revenue ($)')
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('data/output/revenue_trend.png', dpi=300, bbox_inches='tight')
print("✅ Chart saved: data/output/revenue_trend.png\n")

# ===========================================
# INSIGHT 2: Peak Hours Analysis
# ===========================================

print("📊 INSIGHT 2: Peak Hours Analysis")
print("="*60)

hourly_demand = spark.read.format("delta").load("data/processed/gold/hourly_demand")

# Weekday vs Weekend peak hours
peak_hours = hourly_demand.orderBy(desc("total_trips"))
peak_hours.show(10)

# ===========================================
# INSIGHT 3: Top Routes
# ===========================================

print("📊 INSIGHT 3: Top 20 Routes (Pickup → Dropoff)")
print("="*60)

fact_trips = spark.read.format("delta").load("data/processed/gold/fact_trips")

top_routes = fact_trips \
    .groupBy("pickup_location_id", "dropoff_location_id") \
    .agg(
        {"*": "count", "total_amount": "sum", "trip_distance": "avg"}
    ) \
    .withColumnRenamed("count(1)", "trip_count") \
    .withColumnRenamed("sum(total_amount)", "total_revenue") \
    .withColumnRenamed("avg(trip_distance)", "avg_distance") \
    .orderBy(desc("trip_count")) \
    .limit(20)

top_routes.show(20, truncate=False)

# ===========================================
# INSIGHT 4: Tip Analysis by Payment Type
# ===========================================

print("📊 INSIGHT 4: Tipping Behavior")
print("="*60)

payment_analysis = spark.read.format("delta").load("data/processed/gold/payment_analysis")
payment_analysis.show()

print("""
💡 KEY INSIGHTS:
- Payment Type 1 (Credit Card): Highest tips (~12-15%)
- Payment Type 2 (Cash): Very low tips (~0.5%)
- Cash tips may be underreported (not tracked)
""")

# ===========================================
# INSIGHT 5: Busiest Zones
# ===========================================

print("📊 INSIGHT 5: Top 15 Busiest Zones")
print("="*60)

zone_analytics = spark.read.format("delta").load("data/processed/gold/zone_analytics")

busiest_zones = zone_analytics \
    .orderBy(desc("zone_activity_score")) \
    .limit(15)

busiest_zones.show(15, truncate=False)

# ===========================================
# INSIGHT 6: Trip Distance Distribution
# ===========================================

print("📊 INSIGHT 6: Trip Distance Distribution")
print("="*60)

distance_stats = fact_trips \
    .selectExpr(
        "percentile_approx(trip_distance, 0.25) as q1",
        "percentile_approx(trip_distance, 0.50) as median",
        "percentile_approx(trip_distance, 0.75) as q3",
        "percentile_approx(trip_distance, 0.95) as p95",
        "avg(trip_distance) as mean"
    )

distance_stats.show()

# ===========================================
# INSIGHT 7: Speed Analysis
# ===========================================

print("📊 INSIGHT 7: Average Speed by Time of Day")
print("="*60)

speed_by_time = fact_trips \
    .groupBy("time_of_day") \
    .agg({"speed_mph": "avg", "*": "count"}) \
    .withColumnRenamed("avg(speed_mph)", "avg_speed") \
    .withColumnRenamed("count(1)", "trips") \
    .orderBy(desc("avg_speed"))

speed_by_time.show()

print("\n✅ Analytics complete!")
spark.stop()