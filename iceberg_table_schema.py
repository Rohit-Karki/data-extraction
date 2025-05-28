from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, DoubleType

SCHEMAS = {
    "sales": {
        "transactions": Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "category", StringType(), required=True),
            NestedField(3, "amount", DoubleType(), required=True),
        ),
        "Drivers": Schema(
            NestedField(
                field_id=1, name="App No", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=2, name="Type", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3, name="App Date", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=4, name="Status", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=5,
                name="FRU Interview Scheduled",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=6, name="Drug Test", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=7, name="WAV Course", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=8,
                name="Defensive Driving",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=9, name="Driver Exam", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=10,
                name="Medical Clearance Form",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=11,
                name="Last Updated",
                field_type=StringType(),
                required=False,
            ),
        ),
        # "orders": Schema(
        #     NestedField(1, "order_id", IntegerType(), required=True),
        #     NestedField(2, "customer_id", IntegerType(), required=True),
        #     NestedField(3, "order_date", StringType(), required=True),
        # ),
    },
}
