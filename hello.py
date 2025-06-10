from pyiceberg.catalog import load_catalog
from iceberg_table_schema import SCHEMAS
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, FloatType
import pyarrow as pa

schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="category", field_type=StringType(), required=True),
    NestedField(field_id=3, name="amount", field_type=FloatType(), required=True),
)

catalog = load_catalog(
    "default",  # Default catalog name
)
catalog.create_namespace_if_not_exists("hello")
namespaces = catalog.list_namespaces()
print("Namespaces:", namespaces)
table_name = "Drivers"
# schema = SCHEMAS["sales"][table_name] if table_name in SCHEMAS["sales"] else "sales"
# iceberg_table = catalog.create_table(f"hello.{table_name}", schema=schema)

iceberg_table = catalog.create_table_if_not_exists(
    identifier="hello.sales_data", schema=schema
)

pa_table_data = pa.Table.from_pylist(
    [
        {"id": 1, "category": "electronics", "amount": 299.99},
        {"id": 2, "category": "clothing", "amount": 79.99},
        {"id": 3, "category": "groceries", "amount": 45.50},
        {"id": 4, "category": "electronics", "amount": 999.99},
        {"id": 5, "category": "clothing", "amount": 120.00},
    ],
    schema=iceberg_table.schema().as_arrow(),
)

print("Iceberg Table Schema:", iceberg_table.schema)

iceberg_table.append(df=pa_table_data)
print(iceberg_table.scan().to_arrow().to_string(preview_cols=10))


iceberg_table = catalog.load_table(f"sales.sales_data")

df = iceberg_table.scan(
    row_filter="status == 'Incomplete' and app_date >= '04/01/2025'",
    limit=40,
    selected_fields=("app_no", "type", "status", "app_date"),
).to_pandas()
