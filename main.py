from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, FloatType
import pandas as pd
import os
from pyiceberg.catalog import NessieCatalog
from pyspark.sql import SparkSession

# import pyarrow.parquet as pq
# import pyarrow.compute as pc


os.environ["PYICEBERG_HOME"] = os.getcwd()
# Set up the connection to Nessie's REST Catalog
# catalog = load_catalog(
#     "nessie",
#     **{
#         "uri": "http://localhost:19120/iceberg/main/",
#         "warehouse": "hdfs://datanode-new:9870/warehouse",  # Add warehouse location
#     },
# )

# catalog = load_catalog("default")

# catalog = load_catalog(
#     "hive",
#     **{
#         "uri": "thrift://localhost:9083",
#         "warehouse": "file:///tmp/iceberg-warehouse",  # Use local filesystem instead of HDFS
#     },
# )

# catalog = NessieCatalog(
#     name="nessie",
#     uri="http://localhost:19120/iceberg/main/",
#     ref="main",
#     warehouse="hdfs://namenode:9000/warehouse",
# )

# table = catalog.load_table("db1.products")
# print(table.schema())


# # # Verify connection by listing namespaces
# namespaces = catalog.list_namespaces()
# print("Namespaces:", namespaces)

# # Create namespace
# # catalog.create_namespace("sales")

# # Create table
# schema = Schema(
#     NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
#     NestedField(field_id=2, name="name", field_type=StringType(), required=True),
#     NestedField(field_id=3, name="value", field_type=FloatType(), required=True),
# )

# table = catalog.create_table("sales.my_table", schema=schema)

# df = pq.read_table("./yellow_tripdata_2023-01.parquet")


# table = catalog.create_table(
#     "sales.taxi_dataset",
#     schema=df.schema,
# )
# table.append(df)
# df = df.append_column("tip_per_mile", pc.divide(df["tip_amount"], df["trip_distance"]))

# with table.update_schema() as update_schema:
#     update_schema.union_by_name(df.schema)

# table.overwrite(df)

# Write data
# df = pd.DataFrame(
#     {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [1.0, 2.0, 3.0]}
# )

# table.append(df)

# # Read data
# scan = table.scan()
# df_read = scan.to_pandas()
# print(df_read)


def main():
    print("This is a simple Iceberg example using PyIceberg.")


if __name__ == "__main__":
    main()


# from database import create_mysql_connection, read_from_mysql


# def main():
#     print("This is a simple Iceberg example using PyIceberg.")

#     # MySQL operations
#     connection = create_mysql_connection(
#         host="localhost",
#         user="user",
#         password="password",
#         database="exampledb",
#         # port=3306,
#     )

#     if connection:
#         # Write the DataFrame to MySQL
#         # write_to_mysql(connection, df_read, 'iceberg_data')

#         # Read data from MySQL
#         query = "SELECT * FROM sales_data"
#         mysql_df = read_from_mysql(connection, query)
#         if mysql_df is not None:
#             print("\nData from MySQL:")
#             print(mysql_df)

#         connection.close()


# if __name__ == "__main__":
#     main()
