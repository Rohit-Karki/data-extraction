from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

SCHEMAS = {
    "sales": {
        "transactions": Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "category", StringType(), required=True),
            NestedField(3, "amount", DoubleType(), required=True),
        ),
        "Drivers": Schema(
            NestedField(
                field_id=1, name="app_no", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=2, name="type", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3, name="app_date", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=4, name="status", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=5,
                name="fru_interview_scheduled",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=6, name="drug_test", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=7, name="wav_course", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=8,
                name="defensive_driving",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=9, name="driver_exam", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=10,
                name="medical_clearance_form",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=11,
                name="last_updated",
                field_type=StringType(),
                required=False,
            ),
            NestedField(
                field_id=11,
                name="last_modified",
                field_type=TimestampType(),
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
