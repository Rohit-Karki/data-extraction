from pyspark.sql import SparkSession

# Create a Spark session
dataspark = SparkSession.builder.appName("example").getOrCreate()

df = dataspark.read.csv(
    "/home/rohitkarki/Downloads/loan.csv", header=True, inferSchema=True
)
print(df.schema)
