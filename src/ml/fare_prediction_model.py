"""
Fare Prediction Model - Simple & Effective
Predict taxi fare based on trip characteristics
"""

import os
import sys

# Windows fix
os.environ['HADOOP_HOME'] = r'C:\hadoop'
sys.path.insert(0, r'C:\hadoop\bin')

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd


def create_spark():
    return SparkSession.builder \
        .appName("Fare Prediction") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .master("local[*]") \
        .getOrCreate()


def prepare_data(spark):
    """Load and prepare data for ML"""
    
    print("📊 Loading data...")
    df = spark.read.format("delta").load("data/processed/silver/cleaned_trips")
    
    # Select features
    features = [
        'trip_distance',
        'passenger_count',
        'pickup_hour',
        'pickup_day_of_week',
        'PULocationID',
        'DOLocationID'
    ]
    
    # Prepare ML dataset
    ml_df = df.select(features + ['fare_amount']) \
        .filter(col('fare_amount') > 0) \
        .filter(col('fare_amount') < 200) \
        .filter(col('trip_distance') > 0) \
        .filter(col('trip_distance') < 50) \
        .dropna()
    
    print(f"✅ Prepared {ml_df.count():,} rows for training")
    
    return ml_df, features


def train_model(train_df, features):
    """Train Random Forest model"""
    
    print("\n🤖 Training model...")
    
    # Create feature vector
    assembler = VectorAssembler(
        inputCols=features,
        outputCol='features'
    )
    
    # Random Forest
    rf = RandomForestRegressor(
        featuresCol='features',
        labelCol='fare_amount',
        numTrees=50,
        maxDepth=10,
        seed=42
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, rf])
    
    # Train
    model = pipeline.fit(train_df)
    
    print("✅ Model trained!")
    
    return model


def evaluate_model(model, test_df):
    """Evaluate model performance"""
    
    print("\n📈 Evaluating model...")
    
    # Make predictions
    predictions = model.transform(test_df)
    
    # Calculate metrics
    evaluator = RegressionEvaluator(
        labelCol='fare_amount',
        predictionCol='prediction'
    )
    
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: 'rmse'})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: 'mae'})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
    
    print(f"\n📊 MODEL PERFORMANCE:")
    print(f"   RMSE: ${rmse:.2f}")
    print(f"   MAE:  ${mae:.2f}")
    print(f"   R²:   {r2:.3f}")
    
    # Visualize predictions
    sample = predictions.select('fare_amount', 'prediction') \
        .sample(0.01) \
        .toPandas()
    
    plt.figure(figsize=(10, 6))
    plt.scatter(sample['fare_amount'], sample['prediction'], alpha=0.5, s=20)
    plt.plot([0, 100], [0, 100], 'r--', linewidth=2, label='Perfect Prediction')
    plt.xlabel('Actual Fare ($)')
    plt.ylabel('Predicted Fare ($)')
    plt.title(f'Fare Prediction Model (R² = {r2:.3f})')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    os.makedirs('data/output/ml', exist_ok=True)
    plt.savefig('data/output/ml/predictions.png', dpi=300, bbox_inches='tight')
    print("\n✅ Saved: data/output/ml/predictions.png")
    
    return predictions, {'rmse': rmse, 'mae': mae, 'r2': r2}


def save_model(model):
    """Save trained model"""
    
    model_path = "models/fare_prediction"
    model.write().overwrite().save(model_path)
    print(f"\n💾 Model saved to: {model_path}")


def main():
    # Create Spark
    spark = create_spark()
    
    # Prepare data
    ml_df, features = prepare_data(spark)
    
    # Split data
    train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"\n📚 Training set: {train_df.count():,} rows")
    print(f"🧪 Test set: {test_df.count():,} rows")
    
    # Train
    model = train_model(train_df, features)
    
    # Evaluate
    predictions, metrics = evaluate_model(model, test_df)
    
    # Save
    save_model(model)
    
    # Summary
    print("\n" + "="*60)
    print("🎉 ML MODEL COMPLETE")
    print("="*60)
    print(f"""
Features Used: {', '.join(features)}
Model Type: Random Forest Regressor
Performance: R² = {metrics['r2']:.3f}, RMSE = ${metrics['rmse']:.2f}

This model can predict taxi fares with ~{metrics['r2']*100:.0f}% accuracy!
Great for: surge pricing, revenue forecasting, anomaly detection
    """)
    
    spark.stop()


if __name__ == "__main__":
    main()