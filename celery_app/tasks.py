from pyiceberg.catalog import load_catalog
import mysql.connector
import pandas as pd
from .celery_config import app
from database import create_mysql_connection
from pyiceberg.schema import Schema
import pyarrow as pa
from decimal import Decimal
from iceberg_table_schema import SCHEMAS
from celery import shared_task


@shared_task(bind=True)
def extract_and_load_table(
    self,
    table_name: str,
    primary_key_column,
    start_id: int,
    initial_load: bool = True,
):
    try:
        """
        Extracts data from a MySQL table and loads it into an Iceberg table.
        Can handle partitioned extraction based on primary key ranges.
        """

        cnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password="rootpassword",
            database="mydb",
            connection_timeout=60,
        )

        # cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor(dictionary=True)  # Get results as dictionaries
        cursor.execute(
            f"SELECT MIN({primary_key_column}), MAX({primary_key_column}) FROM `{table_name}`"
        )
        # min_max = cursor.fetchone()
        # min_id, max_id = (
        #     min_max[f"MIN({primary_key_column})"],
        #     min_max[f"MAX({primary_key_column})"],
        # )
        # print(f"Table: {table_name}, Primary Key Range: {min_id} to {max_id}")
        # --- 2. Load Iceberg Catalog ---
        catalog = load_catalog(
            "default",  # Default catalog name
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
            schema = (
                SCHEMAS["sales"][table_name]
                if table_name in SCHEMAS["sales"]
                else "sales"
            )
            # print(SCHEMAS["sales"][table_name])
            # print(f"Using schema: {schema}")
            iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)
            # raise NotImplementedError(
            #     "Iceberg table creation/schema inference needs to be implemented."
            # )

        # --- 3. Extract Data in Chunks and Batch Insert ---
        offset = 0
        total_rows = 0
        chunk_size = 100
        last_ingested_id = start_id if start_id is not None else 0

        while True:
            # Build query with optional partitioning
            query = f"SELECT * FROM `{table_name}`"
            where_clauses = []
            if primary_key_column and start_id is not None:
                where_clauses.append(f"`{primary_key_column}` >= {start_id}")
            # Add other WHERE clauses for incremental/date-based if needed
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += f" LIMIT {chunk_size} OFFSET {offset}"
            print(f"Executing query: {query}")

            cursor.execute(query)
            rows = cursor.fetchall()
            # print(f"rows are: {rows}")
            # Convert all Decimal fields to float for Arrow compatibility
            for row in rows:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)

            if not rows:
                break

            last_ingested_id = (
                rows[-1][primary_key_column] if rows else last_ingested_id
            )

            df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())
            iceberg_table.append(df)

            total_rows += len(rows)
            offset += chunk_size
            print(f"Table: {table_name}, Processed: {total_rows} rows")

        cursor.execute(
            "UPDATE ingestion_metadata SET last_ingested_id = %s, is_running = FALSE, updated_at = NOW() WHERE table_name = %s",
            (
                last_ingested_id,
                table_name,
            ),
        )
        cnx.commit()
        cursor.close()
        cnx.close()
        return f"Finished extracting and loading {table_name}: {total_rows} rows."
    except Exception as e:
        self.retry(exc=e, countdown=60, max_retries=3)


@shared_task(bind=True)
def extract_and_load_table_using_partitioning(
    self,
    table_name: str,
    primary_key_column,
    # start_id: int,
    # date_column: str,
    start_date: str,
    end_date: str,
    # parition_key: str,
    # incremental_date: str
    # initial_load: bool = True,
):
    try:
        """
        Extracts data from a MySQL table and loads it into an Iceberg table.
        Can handle partitioned extraction based on primary key ranges.
        """

        cnx = mysql.connector.connect(
            host="localhost",
            user="root",
            password="rootpassword",
            database="mydb",
            connection_timeout=60,
        )

        # cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor(dictionary=True)  # Get results as dictionaries
        # --- 2. Load Iceberg Catalog ---
        catalog = load_catalog(
            "default",  # Default catalog name
        )

        # Ensure the namespace exists
        catalog.create_namespace_if_not_exists("sales")
        namespaces = catalog.list_namespaces()
        # print("Namespaces:", namespaces)

        # Load or create the Iceberg table
        try:
            iceberg_table = catalog.load_table(f"sales.{table_name}")
            scan = iceberg_table.scan()
            df_read = scan.to_pandas()
            # print(df_read)

        except Exception:
            # Basic table creation for demonstration, enhance with schema detection
            print(f"Table {table_name} not found, creating a basic one.")
            # You'll need to define the Iceberg schema based on MySQL table's schema.
            # This is crucial and might require pre-analysis or a schema inference step.
            schema = (
                SCHEMAS["sales"][table_name]
                if table_name in SCHEMAS["sales"]
                else "sales"
            )
            # print(SCHEMAS["sales"][table_name])
            # print(f"Using schema: {schema}")
            iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)
            # raise NotImplementedError(
            #     "Iceberg table creation/schema inference needs to be implemented."
            # )

        # --- 3. Extract Data in Chunks and Batch Insert ---
        offset = 0
        total_rows = 0
        chunk_size = 100
        last_ingested_id = start_date if start_date is not None else 0

        while True:
            # Build query with optional partitioning
            query = f"SELECT * FROM `{table_name}`"
            where_clauses = []
            if primary_key_column and start_date is not None and end_date is not None:
                where_clauses.append(
                    f"`{primary_key_column}` >= '{start_date}' AND `{primary_key_column}` <= '{end_date}'"
                )

            # where_clauses.append(
            #     f"last_modified >= '{last_ingested_id}'"
            # )
            # Add other WHERE clauses for incremental/date-based if needed
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += f" LIMIT {chunk_size} OFFSET {offset}"
            print(f"Executing query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()

            # print(f"rows are: {rows}")

            # Convert all Decimal fields to float for Arrow compatibility
            for row in rows:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)

            if not rows:
                break

            last_ingested_id = (
                rows[-1][primary_key_column] if rows else last_ingested_id
            )

            df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())
            iceberg_table.append(df)

            total_rows += len(rows)
            offset += chunk_size
            print(f"Table: {table_name}, Processed: {total_rows} rows")

        cursor.execute(
            "UPDATE ingestion_metadata SET last_ingested_id = %s, is_running = FALSE, updated_at = NOW() WHERE table_name = %s",
            (
                0,
                table_name,
            ),
        )
        cnx.commit()
        cursor.close()
        cnx.close()
        return f"Finished extracting and loading {table_name}: {total_rows} rows."
    except Exception as e:
        self.retry(exc=e, countdown=60, max_retries=3)
