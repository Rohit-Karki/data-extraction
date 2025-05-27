from pyiceberg.catalog import load_catalog
import mysql.connector
import pandas as pd
from .celery_config import app
from database import create_mysql_connection
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    IntegerType,
    StringType,
    DoubleType,
    LongType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType,
)
import pyarrow as pa
from decimal import Decimal


@app.task
def extract_and_load_table(
    table_name: str,
):
    """
    Extracts data from a MySQL table and loads it into an Iceberg table.
    Can handle partitioned extraction based on primary key ranges.
    """
    # --- 1. Connect to MySQL ---
    cnx = create_mysql_connection(
        host="localhost",
        user="root",
        password="rootpassword",
        database="mydb",
        # port=3306,
    )
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rootpassword",
        database="mydb",
    )

    # cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor(dictionary=True)  # Get results as dictionaries

    # --- 2. Load Iceberg Catalog ---
    catalog = load_catalog(
        "default",
    )

    # Ensure the namespace exists
    catalog.create_namespace_if_not_exists("sales")
    namespaces = catalog.list_namespaces()
    print("Namespaces:", namespaces)

    cursor1 = cnx.cursor(dictionary=True)
    schema1 = build_iceberg_schema_from_mysql(cursor1, table_name)
    # iceberg_table1 = catalog.create_table(f"sales.{table_name}", schema=schema1)
    print(f"Created Iceberg table: {schema1}")

    # Load or create the Iceberg table
    # try:
    #     iceberg_table = catalog.load_table(f"sales.{table_name}")
    #     scan = iceberg_table.scan()
    #     df_read = scan.to_pandas()
    #     print(df_read)

    # except Exception:
    #     # Basic table creation for demonstration, enhance with schema detection
    #     print(f"Table {table_name} not found, creating a basic one.")
    #     # You'll need to define the Iceberg schema based on MySQL table's schema.
    #     # This is crucial and might require pre-analysis or a schema inference step.
    #     schema = Schema(
    #         NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    #         NestedField(
    #             field_id=2, name="category", field_type=StringType(), required=True
    #         ),
    #         NestedField(
    #             field_id=3, name="amount", field_type=DoubleType(), required=True
    #         ),
    #     )
    #     iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)
    #     raise NotImplementedError(
    #         "Iceberg table creation/schema inference needs to be implemented."
    #     )

    # # --- 3. Extract Data in Chunks and Batch Insert ---
    # offset = 0
    # total_rows = 0
    # chunk_size = 100

    # while True:
    #     # Build query with optional partitioning
    #     query = f"SELECT * FROM `{table_name}`"
    #     where_clauses = []
    #     # if primary_key_column and start_id is not None and end_id is not None:
    #     #     where_clauses.append(
    #     #         f"`{primary_key_column}` >= {start_id} AND `{primary_key_column}` < {end_id}"
    #     #     )
    #     # Add other WHERE clauses for incremental/date-based if needed
    #     if where_clauses:
    #         query += " WHERE " + " AND ".join(where_clauses)
    #     query += f" LIMIT {chunk_size} OFFSET {offset}"

    #     cursor.execute(query)
    #     rows = cursor.fetchall()
    #     print(f"rows are: {rows}")
    #     # Convert all Decimal fields to float for Arrow compatibility
    #     for row in rows:
    #         for key, value in row.items():
    #             if isinstance(value, Decimal):
    #                 row[key] = float(value)

    #     if not rows:
    #         cursor.execute(
    #             "UPDATE ingestion_metadata SET is_running = FALSE, status = 'idle', last_run_at = NOW() WHERE table_name = %s",
    #             (table_name,),
    #         )
    #         cnx.commit()
    #         break

    #     df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())
    #     iceberg_table.append(df)

    #     total_rows += len(rows)
    #     offset += chunk_size
    #     print(f"Table: {table_name}, Processed: {total_rows} rows")

    cursor.close()
    cnx.close()
    return f"Finished extracting and loading {table_name}: {total_rows} rows."


def get_mysql_table_schema(cursor, table_name):
    cursor.execute(f"DESCRIBE {table_name}")
    return cursor.fetchall()


def map_mysql_type_to_iceberg(mysql_type):
    if "int" in mysql_type:
        return IntegerType()
    elif "bigint" in mysql_type:
        return LongType()
    elif "float" in mysql_type or "double" in mysql_type:
        return DoubleType()
    elif "decimal" in mysql_type:
        return DecimalType(precision=38, scale=10)
    elif "char" in mysql_type or "text" in mysql_type or "varchar" in mysql_type:
        return StringType()
    elif "date" in mysql_type:
        return DateType()
    elif "datetime" in mysql_type or "timestamp" in mysql_type:
        return TimestampType()
    elif "boolean" in mysql_type or "tinyint(1)" in mysql_type:
        return BooleanType()
    else:
        raise Exception(f"Unsupported MySQL type: {mysql_type}")


def build_iceberg_schema_from_mysql(cursor, table_name):
    mysql_schema = get_mysql_table_schema(cursor, table_name)

    fields = []
    for idx, col in enumerate(mysql_schema, start=1):
        field_name = col["Field"]
        mysql_type = col["Type"]
        nullable = col["Null"] == "YES"

        iceberg_type = map_mysql_type_to_iceberg(mysql_type)
        fields.append(
            NestedField(
                field_id=idx,
                name=field_name,
                field_type=iceberg_type,
                required=not nullable,
            )
        )

    return Schema(*fields)
