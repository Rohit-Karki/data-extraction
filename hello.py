# from pyiceberg.catalog import load_catalog
# from iceberg_table_schema import SCHEMAS
# from pyiceberg.schema import Schema
# from pyiceberg.types import NestedField, IntegerType, StringType, FloatType
# import pyarrow as pa

# schema = Schema(
#     NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
#     NestedField(field_id=2, name="category", field_type=StringType(), required=True),
#     NestedField(field_id=3, name="amount", field_type=FloatType(), required=True),
# )

# catalog = load_catalog(
#     "default",  # Default catalog name
# )
# catalog.create_namespace_if_not_exists("hello")
# namespaces = catalog.list_namespaces()
# print("Namespaces:", namespaces)
# table_name = "Drivers"
# # schema = SCHEMAS["sales"][table_name] if table_name in SCHEMAS["sales"] else "sales"
# # iceberg_table = catalog.create_table(f"hello.{table_name}", schema=schema)

# iceberg_table = catalog.create_table_if_not_exists(
#     identifier="hello.sales_data", schema=schema
# )

# pa_table_data = pa.Table.from_pylist(
#     [
#         {"id": 1, "category": "electronics", "amount": 299.99},
#         {"id": 2, "category": "clothing", "amount": 79.99},
#         {"id": 3, "category": "groceries", "amount": 45.50},
#         {"id": 4, "category": "electronics", "amount": 999.99},
#         {"id": 5, "category": "clothing", "amount": 120.00},
#     ],
#     schema=iceberg_table.schema().as_arrow(),
# )

# print("Iceberg Table Schema:", iceberg_table.schema)

# iceberg_table.append(df=pa_table_data)
# print(iceberg_table.scan().to_arrow().to_string(preview_cols=10))


# iceberg_table = catalog.load_table(f"sales.sales_data")

# df = iceberg_table.scan(
#     row_filter="status == 'Incomplete' and app_date >= '04/01/2025'",
#     limit=40,
#     selected_fields=("app_no", "type", "status", "app_date"),
# ).to_pandas()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
spark = (
    SparkSession.builder.appName("IcebergLocalDevelopment")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,org.xerial:sqlite-jdbc:3.42.0.0",
    )
    .config("spark.sql.crossJoin.enabled", "true")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog")
    # .config("spark.sql.catalog.default.type", "jdbc")
    .config(
        "spark.sql.catalog.default.catalog-impl",
        "org.apache.iceberg.jdbc.JdbcCatalog",
    )
    .config(
        "spark.sql.catalog.default.uri",
        # "sqlite:////tmp/warehouse/pyiceberg_catalog.db",
        "jdbc:sqlite:/tmp/warehouse/pyiceberg_catalog.db",
    )
    .config(
        "spark.sql.catalog.default.warehouse",
        "hdfs://localhost:9000/user/hive/warehouse",
    )
    .config("spark.sql.catalog.default.jdbc.driver", "org.sqlite.JDBC")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)


# spark = (
#     SparkSession.builder.appName("IcebergLocalDevelopment")
#     .config(
#         "spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2"
#     )
#     .config("spark.sql.crossJoin.enabled", "true")
#     .config(
#         "spark.sql.extensions",
#         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#     )
#     .config(
#         "spark.sql.catalog.default", "org.apache.iceberg.spark.SparkCatalog"
#     )
#     .config("spark.sql.catalog.default.type", "hadoop")
#     .config(
#         "spark.sql.catalog.default.warehouse",
#         "hdfs://localhost:9000/user/hive/warehouse",
#     )
#     .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#     .getOrCreate()
# )

# Set default catalog to local for convenience
spark.sql("USE default")

# Verify Spark session creation and catalog
print("Available catalogs:")
spark.sql("SHOW CATALOGS").show()

# Create namespace/database
spark.sql("CREATE NAMESPACE IF NOT EXISTS hello")


# Show available namespaces
print("Available namespaces:")
spark.sql("SHOW NAMESPACES").show()


# Create sample data
data = [
    (1, "electronics", 299.99),
    (2, "clothing", 79.99),
    (3, "groceries", 45.50),
    (4, "electronics", 999.99),
    (5, "clothing", 120.00),
]

# Define schema
schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Create namespace if it doesn't exist
spark.sql("CREATE NAMESPACE IF NOT EXISTS hello")

# Write DataFrame to Iceberg table
df.write.format("iceberg").mode("overwrite").saveAsTable("hello.sales_data_df")

# Read the data back
print("Data written via DataFrame API:")
spark.table("hello.sales_data_df").show()

# Append more data
more_data = [
    (6, "books", 25.99),
    (7, "electronics", 599.99),
]

df_append = spark.createDataFrame(more_data, schema)
df_append.write.format("iceberg").mode("append").saveAsTable("hello.sales_data_df")

print("After appending more data:")
spark.table("hello.sales_data_df").show()

# Perform analytics
print("Category-wise totals:")
spark.sql(
    """
SELECT category, 
       COUNT(*) as count, 
       SUM(amount) as total_amount,
       AVG(amount) as avg_amount
FROM hello.sales_data_df 
GROUP BY category 
ORDER BY total_amount DESC
"""
).show()
