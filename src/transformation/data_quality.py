# src/transformation/data_quality.py
"""
Data Quality Rules and Validation
FIXED VERSION - Corrected validation logic
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, expr
from typing import List, Tuple


class DataQualityChecker:
    """Data quality validation and cleaning"""
    
    @staticmethod
    def get_validation_rules() -> List[Tuple[str, str, str]]:
        """
        Define validation rules
        Returns: List of (rule_name, condition, description)
        """
        return [
            # Trip distance rules
            (
                "valid_trip_distance",
                "(trip_distance >= 0) AND (trip_distance <= 100)",
                "Trip distance between 0 and 100 miles"
            ),
            
            # Passenger count rules
            (
                "valid_passenger_count",
                "(passenger_count >= 1) AND (passenger_count <= 6)",
                "Passenger count between 1 and 6"
            ),
            
            # Fare amount rules
            (
                "valid_fare_amount",
                "(fare_amount >= 0) AND (fare_amount <= 500)",
                "Fare amount between $0 and $500"
            ),
            
            # Total amount rules
            (
                "valid_total_amount",
                "(total_amount >= 0) AND (total_amount <= 1000)",
                "Total amount between $0 and $1000"
            ),
            
            # Datetime rules
            (
                "valid_pickup_datetime",
                "tpep_pickup_datetime IS NOT NULL",
                "Pickup datetime must exist"
            ),
            
            (
                "valid_dropoff_datetime",
                "tpep_dropoff_datetime IS NOT NULL",
                "Dropoff datetime must exist"
            ),
            
            # Trip duration logic
            (
                "valid_trip_duration",
                "tpep_dropoff_datetime > tpep_pickup_datetime",
                "Dropoff must be after pickup"
            ),
            
            # Location rules
            (
                "valid_locations",
                "(PULocationID IS NOT NULL) AND (DOLocationID IS NOT NULL)",
                "Pickup and dropoff locations must exist"
            ),
        ]
    
    @staticmethod
    def apply_validation(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Apply validation rules and separate valid/invalid records
        
        Returns:
            (valid_df, invalid_df)
        """
        rules = DataQualityChecker.get_validation_rules()
        
        # Build combined validation condition
        all_conditions = [rule[1] for rule in rules]
        combined_condition = " AND ".join([f"({cond})" for cond in all_conditions])
        
        # Separate valid and invalid
        valid_df = df.filter(combined_condition)
        invalid_df = df.filter(f"NOT ({combined_condition})")
        
        # Add validation flags to invalid records
        # FIX: Use expr() to properly evaluate SQL expressions as columns
        for rule_name, condition, _ in rules:
            invalid_df = invalid_df.withColumn(
                f"failed_{rule_name}",
                when(~expr(condition), lit(True)).otherwise(lit(False))  # FIXED: Added expr()
            )
        
        return valid_df, invalid_df
    
    @staticmethod
    def clean_data(df: DataFrame) -> DataFrame:
        """
        Clean and standardize data
        """
        cleaned_df = df
        
        # 1. Handle nulls with defaults
        cleaned_df = cleaned_df \
            .fillna({
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
        
        # 2. Standardize categorical values
        cleaned_df = cleaned_df \
            .withColumn(
                "store_and_fwd_flag",
                when(col("store_and_fwd_flag").isin(["Y", "y", "1"]), "Y")
                .otherwise("N")
            )
        
        # 3. Cap extreme values (winsorization)
        cleaned_df = cleaned_df \
            .withColumn(
                "trip_distance",
                when(col("trip_distance") > 100, 100.0)
                .when(col("trip_distance") < 0, 0.0)
                .otherwise(col("trip_distance"))
            ) \
            .withColumn(
                "fare_amount",
                when(col("fare_amount") > 500, 500.0)
                .when(col("fare_amount") < 0, 0.0)
                .otherwise(col("fare_amount"))
            )
        
        return cleaned_df


# 🤔 WHAT WAS THE BUG?
#
# In the original code, we had:
#   when(~col(condition), lit(True))
#
# This tried to use the STRING "condition" as a column name, which failed.
#
# The FIX:
#   when(~expr(condition), lit(True))
#
# expr() evaluates the SQL expression properly, so it understands
# "(trip_distance >= 0) AND (trip_distance <= 100)" as a boolean condition,
# not as a column name.