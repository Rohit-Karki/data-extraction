# from pyiceberg.catalog import load_catalog
# import pandas as pd
# import pyarrow as pa
# import os
from celery_app.tasks import extract_and_load_table

# catalog = load_catalog("default")

# # Verify connection by listing namespaces
# namespaces = catalog.list_namespaces()
# print("Namespaces:", namespaces)

# # Create namespace
# catalog.create_namespace_if_not_exists("sales")

# # Create table
# schema = Schema(
#     NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
#     NestedField(field_id=2, name="category", field_type=StringType(), required=True),
#     NestedField(field_id=3, name="amount", field_type=FloatType(), required=True),
# )

# # table = catalog.create_table_if_not_exists("sales.my_table", schema=schema)

# catalog.create_namespace_if_not_exists("transactions")

# # table = catalog.load_table("sales.my_table")
# # print(table.schema())


# iceberg_table = catalog.create_table_if_not_exists(
#     identifier="transactions.sales_data", schema=schema
# )
# # Create a PyArrow table with some sample transactions
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

# iceberg_table.append(df=pa_table_data)

# # Read data
# scan = table.scan()
# df_read = scan.to_pandas()
# print(df_read)


def main():

    result = extract_and_load_table.delay(
        table_name="transactions",
        # db_config=db_config,
        # iceberg_catalog_config=iceberg_catalog_config,
        # primary_key_column="id",
        # start_id=0,
        # end_id=10,
    )

    print("Task submitted:", result.id)
    print("This is a simple Iceberg example using PyIceberg.")

    # MySQL operations


if __name__ == "__main__":
    main()
