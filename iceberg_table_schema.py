from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
    FloatType,
)

SCHEMAS = {
    "sales": {
        "vehicles": Schema(
            NestedField(1, "license_number", StringType(), required=False),
            NestedField(2, "business_name", StringType(), required=False),
            NestedField(3, "business_category", StringType(), required=False),
            NestedField(4, "business_unique_id", StringType(), required=False),
            NestedField(5, "asset_type", StringType(), required=False),
            NestedField(6, "business_asset_id", StringType(), required=False),
            NestedField(
                7, "manufacture_identification_number", StringType(), required=False
            ),
            NestedField(8, "dmv_license_plate_number", StringType(), required=False),
            NestedField(9, "state_of_registration", StringType(), required=False),
            NestedField(10, "dcwp_plate_number", StringType(), required=False),
            NestedField(11, "decal_number", StringType(), required=False),
            NestedField(12, "latest_inspection_number", StringType(), required=False),
            NestedField(
                13, "latest_inspection_date", StringType(), required=False
            ),  # or DateType() if structured
            NestedField(14, "latest_inspection_result", StringType(), required=False),
            NestedField(
                15, "last_modified", TimestampType(), required=False
            ),  # or TimestampType(with_timezone=True)
        ),
        "employees": Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(
                field_id=2, name="category", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=3, name="amount", field_type=FloatType(), required=True
            ),
        ),
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
