from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, FloatType
import pandas as pd

# Set up the connection to Nessie's REST Catalog
catalog = load_catalog(
    "nessie",
    **{
        "uri": "http://localhost:19120/iceberg/main/",
    },
)

# Verify connection by listing namespaces
namespaces = catalog.list_namespaces()
print("Namespaces:", namespaces)

# Create namespace
# catalog.create_namespace("sales")

# Create table
schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="value", field_type=FloatType(), required=True),
)

table = catalog.create_table("sales.my_table_", schema=schema)

# Write data
df = pd.DataFrame(
    {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [1.0, 2.0, 3.0]}
)

table.append(df)

# Read data
scan = table.scan()
df_read = scan.to_pandas()
print(df_read)


def main():
    print("This is a simple Iceberg example using PyIceberg.")


if __name__ == "__main__":
    main()
