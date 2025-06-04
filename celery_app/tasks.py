from pyiceberg.catalog import load_catalog
import pandas as pd
from .celery_config import app
from database import read_from_db, engine, text
from pyiceberg.schema import Schema
import pyarrow as pa
from decimal import Decimal
from iceberg_table_schema import SCHEMAS
from celery import shared_task
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


@shared_task(bind=True)
def extract_and_load_table(
    self,
    table_name: str,
    primary_key_column,
    incremental_date: str,
):
    try:
        """
        Extracts data from a MySQL table and loads it into an Iceberg table.
        Can handle partitioned extraction based on primary key ranges.
        """

        # Get min/max values using SQLAlchemy
        # query = f"SELECT MIN({primary_key_column}), MAX({primary_key_column}) FROM `{table_name}`"
        # Get database metadata from ingestion_metadata table
        metadata_query = f"""
            SELECT database_name, database_url 
            FROM ingestion_metadata 
            WHERE table_name = '{table_name}'
        """
        with engine.connect() as connection:
            result = connection.execute(text(metadata_query))
            metadata = result.fetchone()

        if not metadata:
            raise ValueError(f"No metadata found for table: {table_name}")

        database_name, database_url = metadata

        # Create engine with specific database URL
        # Database configuration
        DB_CONFIG = {
            "host": "localhost",
            "user": "root",
            "password": "rootpassword",
            "database": "mydb",
        }

        # Create engine with connection pooling
        DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        source_engine = create_engine(
            DATABASE_URL,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,  # Recycle connections every hour
            pool_timeout=30,
        )

        # # Get min/max values from source database
        # with source_engine.connect() as connection:
        #     result = connection.execute(text(query))
        #     min_max = result.fetchone()

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
            # print(df_read)

        except Exception:
            # Basic table creation for demonstration, enhance with schema detection
            print(f"Table {table_name} not found, creating a basic one.")
            # You'll need to define the Iceberg schema based on the source table's schema.
            # This is crucial and might require pre-analysis or a schema inference step.
            schema = (
                SCHEMAS["sales"][table_name]
                if table_name in SCHEMAS["sales"]
                else "sales"
            )
            iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)

        # --- 3. Extract Data in Chunks and Batch Insert ---
        offset = 0
        total_rows = 0
        chunk_size = 100
        last_ingested_id = 0

        while True:
            # Build query with optional partitioning
            query = f"SELECT * FROM `{table_name}`"
            where_clauses = []
            # WHERE clauses for incremental/date-based
            if incremental_date:
                where_clauses.append(f"`last_modified` >= '{incremental_date}'")
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += f" LIMIT {chunk_size} OFFSET {offset}"
            print(f"Executing query: {query}")

            # Execute query using SQLAlchemy
            with source_engine.connect() as connection:
                result = connection.execute(text(query))
                rows = result.fetchall()
                # Convert SQLAlchemy Row objects to dictionaries
                rows = [dict(row._mapping) for row in rows]

            # Convert all Decimal fields to float for Arrow compatibility
            for row in rows:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)

            if not rows:
                break

            # incremental_date = rows[-1]["last_modified"] if rows else incremental_date

            df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())
            iceberg_table.upsert(df)

            total_rows += len(rows)
            offset += chunk_size
            print(f"Table: {table_name}, Processed: {total_rows} rows")

        # Update ingestion metadata using SQLAlchemy
        with engine.connect() as connection:
            query = f"UPDATE ingestion_metadata SET last_ingested_time = '{incremental_date}', is_running = FALSE, updated_at = NOW() WHERE table_name = '{table_name}'"
            connection.execute(text(query))
            connection.commit()
        return f"Finished extracting and loading {table_name}: {total_rows} rows."
    except Exception as e:
        self.retry(exc=e, countdown=60, max_retries=3)


@shared_task(bind=True)
def extract_and_load_table_using_partitioning(
    self,
    table_name: str,
    primary_key_column,
    start_date: str,
    end_date: str,
    incremental_date: str = None,
    initial_load: bool = True,
):
    try:
        """
        Extracts data from a database table and loads it into an Iceberg table.
        Handles incremental extraction based on timestamps.
        """

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
            print(f"exists {catalog.table_exists(f'sales.{table_name}')}")
            if catalog.table_exists(f"sales.{table_name}") is True:
                iceberg_table = catalog.load_table(f"sales.{table_name}")
            else:
                schema = (
                    SCHEMAS["sales"][table_name]
                    if table_name in SCHEMAS["sales"]
                    else "sales"
                )
                # Basic table creation for demonstration, enhance with schema detection
                iceberg_table = catalog.create_table(
                    f"sales.{table_name}", schema=schema
                )
        except Exception as e:
            print(f"exception is {e}")
            schema = (
                SCHEMAS["sales"][table_name]
                if table_name in SCHEMAS["sales"]
                else "sales"
            )
            # Basic table creation for demonstration, enhance with schema detection
            iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)
            print(f"Table {table_name} not found, creating a basic one.")

        # --- 3. Extract Data in Chunks and Batch Insert ---
        offset = 0
        total_rows = 0
        chunk_size = 100
        last_ingested_id = start_date if start_date is not None else 0

        while True:
            # Build query with optional partitioning
            query = f"SELECT * FROM `{table_name}`"
            where_clauses = []
            # if done partitioning by primary key or date column
            if primary_key_column and start_date is not None and end_date is not None:
                where_clauses.append(
                    f"`{primary_key_column}` >= '{start_date}' AND `{primary_key_column}` <= '{end_date}'"
                )

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += f" LIMIT {chunk_size} OFFSET {offset}"
            print(f"Executing query: {query}")

            # Execute query using SQLAlchemy instead of cursor
            with engine.connect() as connection:
                result = connection.execute(text(query))
                rows = result.fetchall()
                # Convert SQLAlchemy Row objects to dictionaries
                rows = [dict(row._mapping) for row in rows]

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

        # Update ingestion metadata using SQLAlchemy
        with engine.connect() as connection:
            query = f"UPDATE ingestion_metadata SET last_ingested_time = '{end_date}', is_running = FALSE, updated_at = NOW() WHERE table_name = '{table_name}'"
            connection.execute(text(query))
            connection.commit()
        return f"Finished extracting and loading {table_name}: {total_rows} rows."
    except Exception as e:
        self.retry(exc=e, countdown=60, max_retries=3)
