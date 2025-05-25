from celery import Celery
from pyiceberg.catalog import Catalog, load_catalog
import mysql.connector  # Or PyMySQL
import pandas as pd

app = Celery(
    "data_pull", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1"
)


@app.task
def extract_and_load_table(
    table_name: str,
    db_config: dict,
    iceberg_catalog_config: dict,
    chunk_size: int = 10000,
    primary_key_column: str = None,
    start_id: int = None,
    end_id: int = None,
):
    """
    Extracts data from a MySQL table and loads it into an Iceberg table.
    Can handle partitioned extraction based on primary key ranges.
    """
    # --- 1. Connect to MySQL ---
    cnx = mysql.connector.connect(**db_config)
    cursor = cnx.cursor(dictionary=True)  # Get results as dictionaries

    # --- 2. Load Iceberg Catalog ---
    catalog: Catalog = load_catalog(name="my_catalog", **iceberg_catalog_config)
    # Ensure the namespace exists
    catalog.create_namespace("your_namespace", ignore_exists=True)
    # Load or create the Iceberg table
    try:
        iceberg_table = catalog.load_table(f"your_namespace.{table_name}")
    except Exception:
        # Basic table creation for demonstration, enhance with schema detection
        print(f"Table {table_name} not found, creating a basic one.")
        # You'll need to define the Iceberg schema based on MySQL table's schema.
        # This is crucial and might require pre-analysis or a schema inference step.
        # Example: from pyiceberg.schema import Schema, NestedField, PrimitiveType
        # iceberg_schema = Schema(...)
        # iceberg_table = catalog.create_table(f"your_namespace.{table_name}", iceberg_schema)
        raise NotImplementedError(
            "Iceberg table creation/schema inference needs to be implemented."
        )

    # --- 3. Extract Data in Chunks and Batch Insert ---
    offset = 0
    total_rows = 0

    while True:
        # Build query with optional partitioning
        query = f"SELECT * FROM `{table_name}`"
        where_clauses = []
        if primary_key_column and start_id is not None and end_id is not None:
            where_clauses.append(
                f"`{primary_key_column}` >= {start_id} AND `{primary_key_column}` < {end_id}"
            )
        # Add other WHERE clauses for incremental/date-based if needed
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        query += f" LIMIT {chunk_size} OFFSET {offset}"

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            break

        df = pd.DataFrame(rows)
        # PyIceberg expects columns to match its schema.
        # You might need to cast/rename DataFrame columns here.

        # Batch write to Iceberg
        # PyIceberg write_dataframe is a convenient way to batch insert.
        # It will write Parquet files to the underlying storage (HDFS/S3).
        iceberg_table.append(df)  # or write_dataframe(iceberg_table, df)

        total_rows += len(rows)
        offset += chunk_size
        print(f"Table: {table_name}, Processed: {total_rows} rows")

    cursor.close()
    cnx.close()
    return f"Finished extracting and loading {table_name}: {total_rows} rows."
