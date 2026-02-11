from pyspark.sql import SparkSession
import sys
print("Python used by script:", sys.executable)
import sys
print("Python used by pyspark:", sys.executable)

# Create Spark session (think of it as starting the Spark engine)
spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("local[*]") \
    .getOrCreate()

# Create a simple dataset
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Show the data
df.show()

# Stop Spark
spark.stop()