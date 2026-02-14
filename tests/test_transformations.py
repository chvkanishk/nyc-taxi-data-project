"""
Unit Tests for Data Transformations
Run with: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder \
        .appName("tests") \
        .master("local[2]") \
        .getOrCreate()


def test_trip_distance_validation(spark):
    """Test that trip distances are validated correctly"""
    
    # Create test data
    test_data = [
        (1, 150.0, 10.0),  # Invalid: too long
        (2, -5.0, 20.0),   # Invalid: negative
        (3, 50.0, 15.0),   # Valid
    ]
    
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType())
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Apply validation
    valid_df = df.filter("(trip_distance >= 0) AND (trip_distance <= 100)")
    
    # Assert
    assert valid_df.count() == 1
    assert valid_df.first()['id'] == 3


def test_feature_engineering(spark):
    """Test that derived columns are created correctly"""
    
    test_data = [(
        datetime(2024, 1, 15, 14, 30),  # Pickup
        datetime(2024, 1, 15, 14, 45),  # Dropoff
        3.5,  # Distance
        15.0  # Fare
    )]
    
    schema = StructType([
        StructField("pickup_datetime", TimestampType()),
        StructField("dropoff_datetime", TimestampType()),
        StructField("trip_distance", DoubleType()),
        StructField("fare_amount", DoubleType())
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Add derived columns (simplified)
    from pyspark.sql.functions import hour, unix_timestamp
    
    result = df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60
    ).withColumn(
        "pickup_hour",
        hour("pickup_datetime")
    )
    
    # Assert
    row = result.first()
    assert row['trip_duration_minutes'] == 15.0
    assert row['pickup_hour'] == 14


def test_data_quality_stats(spark):
    """Test data quality metrics calculation"""
    
    test_data = [
        (1, 10.0, True),
        (2, 20.0, True),
        (3, None, False),  # Invalid
        (4, -5.0, False),  # Invalid
    ]
    
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("is_valid", BooleanType())
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    total = df.count()
    valid = df.filter("is_valid = true").count()
    
    quality_pct = (valid / total) * 100
    
    assert quality_pct == 50.0  # 2 out of 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])