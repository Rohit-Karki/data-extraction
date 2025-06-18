from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    isnan,
    when,
    count,
    lit,
    split,
    to_date,
    hour,
    minute,
    second,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# from pyspark.sql.functions import mode
# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
import os
from datetime import datetime


# Create a Spark session
dataspark = SparkSession.builder.appName("example").getOrCreate()

schema = StructType(
    [
        StructField("Loan_id", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Married", StringType(), True),
        StructField("Dependents", IntegerType(), True),
        StructField("Education", StringType(), True),
        StructField("Self_Employed", StringType(), True),
        StructField("ApplicantIncome", IntegerType(), True),
        StructField("CoapplicantIncome", IntegerType(), True),
        StructField("LoanAmount", IntegerType(), True),
        StructField("Loan_Amount_Term", IntegerType(), True),
        StructField("Credit_History", IntegerType(), True),
        StructField("Property_Area", StringType(), True),
        StructField("Loan_Status", StringType(), True),
    ]
)

input_path = "hdfs://localhost:9000/user/hive/warehouse/loan.csv"
# Read the CSV file with the specified schema
raw_df = dataspark.read.csv(
    input_path,
    header=True,
)


df = dataspark.read.csv(
    "hdfs://localhost:9000/user/hive/warehouse/loan.csv", header=True, inferSchema=True
)
print(df.schema)
