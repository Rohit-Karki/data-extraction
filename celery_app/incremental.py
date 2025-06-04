from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
import pyarrow as pa
from decimal import Decimal
from iceberg_table_schema import SCHEMAS
from celery import shared_task
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from database import update_metadata


@shared_task(bind=True)
def extract_and_load_table_incremental(
    self,
    table_name: str,
    primary_key_column,
    incremental_date,
    database_url: str,
    initial_load: bool = True,
    database_name: str = "postgres",
):
    try:
        # Database configuration
        DB_CONFIG = {
            "host": "localhost",
            "user": "root",
            "password": "rootpassword",
            "database": "mydb",
        }
        # Create engine with connection pooling
        DATABASE_URL = database_url
        engine = create_engine(
            DATABASE_URL,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,  # Recycle connections every hour
            pool_timeout=30,
        )
        """
        Extracts data from a MySQL table and loads it into an Iceberg table.
        Handles incremental extraction based on timestamps.
        """

        # Convert incremental_date to datetime if it's a string
        if initial_load is False:
            print("Incremental load detected")
            if isinstance(incremental_date, str):
                incremental_date = datetime.fromisoformat(incremental_date)

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

            # For testing only, remove in production
            # print(f"Loaded Iceberg table: {iceberg_table.name}")
            # Uncomment below lines to read data from Iceberg table
            # scan = iceberg_table.scan()
            # df_read = scan.to_pandas()
            # print(df_read)

        except Exception:
            # Basic table creation for demonstration, enhance with schema detection
            print(f"Table {table_name} not found, creating a basic one.")
            # You'll need to define the Iceberg schema based on MySQL table's schema.
            # This is crucial and might require pre-analysis or a schema inference step.
            # For now using from a schema dictionary
            schema = (
                SCHEMAS["sales"][table_name]
                if table_name in SCHEMAS["sales"]
                else "transactions"
            )
            # print(SCHEMAS["sales"][table_name])
            # print(f"Using schema: {schema}")
            iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)
            raise NotImplementedError(
                "Iceberg table creation/schema inference needs to be implemented."
            )

        # --- 3. Extract Data in Chunks and Batch Insert ---
        offset = 0
        total_rows = 0
        chunk_size = 100

        while True:
            # Build query with optional partitioning
            query = f"SELECT * FROM {table_name}"
            where_clauses = []
            if initial_load is False and incremental_date:
                where_clauses.append(f"last_modified >= '{incremental_date}'")
            # Add other WHERE clauses for incremental/date-based if needed
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += f" LIMIT {chunk_size} OFFSET {offset}"
            print(f"Executing query: {query}")

            # Use SQLAlchemy to execute the query
            with engine.connect() as connection:
                result = connection.execute(text(query))
                rows = result.fetchall()
                print(f"Query executed successfully, fetching results... {rows}")
                rows = [dict(row._mapping) for row in rows]

            # Convert all Decimal fields to float for Arrow compatibility

            # Convert all Decimal fields to float for Arrow compatibility
            for row in rows:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)

            if not rows:
                break

            # Process the rows
            # df = pd.DataFrame(rows)
            # print(f"Processing {len(rows)} rows")
            df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())
            # Convert DataFrame to Arrow table
            # arrow_table = pa.Table.from_pandas(df)

            # Write to Iceberg table
            iceberg_table.append(df)

            last_ingested_time = (
                max(row["last_modified"] for row in rows) if rows else incremental_date
            )
            print(f"Last ingested time: {last_ingested_time}")
            offset += chunk_size
            total_rows += len(rows)

        print(f"Total rows processed: {total_rows}")

        last_ingested_time = (
            max(row["last_modified"] for row in rows) if rows else incremental_date
        )
        print(f"Last ingested time: {last_ingested_time}")
        # Get the max last_modified time from this batch

        # Convert rows to Arrow table and write to Iceberg
        # df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())
        # iceberg_table.append(df)

        total_rows += len(rows)
        offset += chunk_size
        print(f"Table: {table_name}, Processed: {total_rows} rows")

        update_metadata(
            table_name,
            last_ingested_time=last_ingested_time,
            is_running=False,
        )
        # Update ingestion metadata using SQLAlchemy
        # with engine.connect() as connection:
        #     query = f"UPDATE ingestion_metadata SET last_ingested_time = '{last_ingested_time}', is_running = FALSE, updated_at = NOW() WHERE table_name = '{table_name}'"
        #     connection.execute(text(query))
        #     connection.commit()

        return f"Finished extracting and loading {table_name}: {total_rows} rows."
    except Exception as e:
        print(f"Error in incremental load: {e}")
        self.retry(exc=e, countdown=60, max_retries=3)
