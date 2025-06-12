from pyiceberg.catalog import load_catalog
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
        # Create engine with connection pooling
        engine = create_engine(
            database_url,
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
            if isinstance(incremental_date, str):
                incremental_date = datetime.fromisoformat(incremental_date)

        # --- 2. Load Iceberg Catalog ---
        catalog = load_catalog(
            "default",  # Default catalog name
        )

        # Ensure the namespace exists
        catalog.create_namespace_if_not_exists("sales")

        # Load or create the Iceberg table
        try:
            iceberg_table = catalog.load_table(f"sales.{table_name}")
        except Exception:
            # Todo: Implement check if schema exists in SCHEMAS
            schema = (
                SCHEMAS["sales"][table_name]
                # if table_name in SCHEMAS["sales"]
                # else "transactions"
            )
            iceberg_table = catalog.create_table(f"sales.{table_name}", schema=schema)
            raise NotImplementedError(
                "Iceberg table creation/schema inference needs to be implemented."
            )

        # --- 3. Extract Data in Chunks and Batch Insert ---
        offset = 0
        total_rows = 0
        chunk_size = 1000

        while True:
            # Build query with optional partitioning
            query = f"SELECT * FROM {table_name}"
            where_clauses = []
            if initial_load is False and incremental_date and database_name != "oracle":
                where_clauses.append(f"last_modified >= '{incremental_date}'")
            # Add other WHERE clauses for incremental/date-based if needed
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

            # Todo: Use primary key based incrementatl chunk extraction inplace of offset
            # if primary_key_column:
            #     query += f" ORDER BY {primary_key_column} ASC LIMIT {chunk_size}"
            # else:
            #     query += " ORDER BY last_modified ASC"
            if database_name == "oracle":
                # pass
                query += f" OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
            else:
                query += f" LIMIT {chunk_size} OFFSET {offset}"
            # print(f"Executing query: {query}")

            # Use SQLAlchemy to execute the query
            with engine.connect() as connection:
                result = connection.execute(text(query))
                rows = result.fetchall()

                rows = [dict(row._mapping) for row in rows]

            # Convert all Decimal fields to float for Arrow compatibility
            for row in rows:
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)
            # print(f"Fetched {len(rows)} rows from the database.")
            if len(rows) == 0:
                # No more rows to process
                break
            # print(f"Processing {len(rows)} rows")

            # In place of pandas-> Arrow conversion, directly use pyarrow
            df = pa.Table.from_pylist(rows, schema=iceberg_table.schema().as_arrow())

            # Write to Iceberg table
            iceberg_table.append(df)

            # iceberg_table.upsert(df)

            # Get the max last_modified time from this batch
            last_ingested_time = (
                max(row["last_modified"] for row in rows) if rows else incremental_date
            )
            # print(f"Last ingested time: {last_ingested_time}")
            offset += chunk_size
            total_rows += len(rows)

        print(f"Table: {table_name}, Processed: {total_rows} rows")

        last_ingested_time = incremental_date

        update_metadata(
            table_name,
            last_ingested_time=last_ingested_time,
            is_running=False,
        )
        return f"Finished extracting and loading {table_name}: {total_rows} rows."
    except Exception as e:
        print(f"Error in incremental load: {e}")
        self.retry(exc=e, countdown=60, max_retries=3)
