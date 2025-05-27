from pyiceberg.catalog import load_catalog
import mysql.connector
import pandas as pd
from .celery_config import app
from database import create_mysql_connection
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, DoubleType
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

    # Load or create the Iceberg table
    try:
        iceberg_table = catalog.load_table(f"sales.{table_name}")
        scan = iceberg_table.scan()
        df_read = scan.to_pandas()
        print(df_read)

    except Exception:
        # Basic table creation for demonstration, enhance with schema detection
        print(f"Table {table_name} not found, creating a basic one.")
        # You'll need to define the Iceberg schema based on MySQL table's schema.
        # This is crucial and might require pre-analysis or a schema inference step.
        # Example: from pyiceberg.schema import Schema, NestedField, PrimitiveType
        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(
                field_id=2, name="category", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=3, name="amount", field_type=DoubleType(), required=True
            ),
        )
        iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)
        raise NotImplementedError(
            "Iceberg table creation/schema inference needs to be implemented."
        )

    # --- 3. Extract Data in Chunks and Batch Insert ---
    offset = 0
    total_rows = 0
    chunk_size = 100

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
    #         break

    #     df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())
    #     iceberg_table.append(df)

    #     total_rows += len(rows)
    #     offset += chunk_size
    #     print(f"Table: {table_name}, Processed: {total_rows} rows")

    cursor.close()
    cnx.close()
    return f"Finished extracting and loading {table_name}: {1} rows."
