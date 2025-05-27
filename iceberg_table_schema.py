from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, DoubleType

SCHEMAS = {
    "sales": {
        "transactions": Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "category", StringType(), required=True),
            NestedField(3, "amount", DoubleType(), required=True),
        ),
        "orders": Schema(
            NestedField(1, "order_id", IntegerType(), required=True),
            NestedField(2, "customer_id", IntegerType(), required=True),
            NestedField(3, "order_date", StringType(), required=True),
        ),
    },
}
