from celery import Celery
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema, NestedField, PrimitiveType
import mysql.connector
import pandas as pd
from datetime import datetime, timezone

# Todo Use redis
last_pulled_timestamps = {}  # {table_name: datetime_object}

app = Celery(
    "data_pull", broker="redis://localhost:6379/0", backend="redis://localhost:6379/1"
)


def get_last_pulled_timestamp(table_name: str) -> datetime | None:
    """Retrieve the last pulled timestamp for a table from a persistent store."""
    return last_pulled_timestamps.get(table_name)


def update_last_pulled_timestamp(table_name: str, timestamp: datetime):
    """Update the last pulled timestamp for a table in a persistent store."""
    # In a real application, this would store to a database, S3, etc.
    last_pulled_timestamps[table_name] = timestamp
    print(f"Updated last pulled timestamp for {table_name} to {timestamp}")


@app.task
def extract_and_load_incremental(
    table_name: str,
    timestamp_column: str,  # The column in MySQL to track changes (e.g., 'updated_at')
    db_config: dict,
    iceberg_catalog_config: dict,
    iceberg_namespace: str = "sales",
    chunk_size: int = 10000,
):
    """
    Extracts incremental data from a MySQL table based on a timestamp column
    and loads it into an Iceberg table.
    """
    cnx = None
    try:
        cnx = mysql.connector.connect(**db_config)
        cursor = cnx.cursor(dictionary=True)

        catalog: Catalog = load_catalog(name="sales", **iceberg_catalog_config)
        catalog.create_namespace(iceberg_namespace, ignore_exists=True)
        iceberg_table_name = f"{iceberg_namespace}.{table_name}"

        # Ensure Iceberg table exists and has the necessary schema
        try:
            iceberg_table = catalog.load_table(iceberg_table_name)
            # You might want to validate schema here
        except Exception:
            print(
                f"Iceberg table {iceberg_table_name} not found. Attempting to create a dummy schema."
            )
            dummy_schema = Schema(
                NestedField(1, "id", PrimitiveType.LONG(), required=True),
                NestedField(2, "name", PrimitiveType.STRING()),
                NestedField(
                    3, timestamp_column, PrimitiveType.TIMESTAMP_NTZ()
                ),  # Match your timestamp column type
                # Add other columns from your MySQL table
            )
            iceberg_table = catalog.create_table(iceberg_table_name, dummy_schema)
            print(f"Created Iceberg table {iceberg_table_name} with dummy schema.")

        last_ts = get_last_pulled_timestamp(table_name)
        current_run_max_ts = None  # To store the max timestamp seen in this run

        # Get current time as the upper bound for this pull to avoid race conditions
        # if data is being updated during the pull.
        pull_start_time = datetime.now(timezone.utc)

        query_base = f"SELECT * FROM `{table_name}` WHERE `{timestamp_column}` > %s ORDER BY `{timestamp_column}` ASC LIMIT %s"
        if last_ts is None:
            # First full load or no previous timestamp, pull all data
            query_base = f"SELECT * FROM `{table_name}` WHERE `{timestamp_column}` <= %s ORDER BY `{timestamp_column}` ASC LIMIT %s"
            # Use a very old timestamp or NULL if your column allows, or just remove the WHERE clause
            # For simplicity, if no last_ts, we'll pull everything up to pull_start_time
            print(
                f"No last timestamp found for {table_name}. Performing initial load up to {pull_start_time}."
            )
            params = (pull_start_time, chunk_size)
            initial_load = True
        else:
            print(
                f"Starting incremental load for {table_name} from {last_ts} up to {pull_start_time}."
            )
            params = (last_ts, chunk_size)
            initial_load = False

        offset = 0
        total_rows = 0

        while True:
            if initial_load:
                # Adjust query and params for the initial load which goes up to pull_start_time
                current_query = f"SELECT * FROM `{table_name}` WHERE `{timestamp_column}` <= %s ORDER BY `{timestamp_column}` ASC LIMIT %s OFFSET %s"
                current_params = (pull_start_time, chunk_size, offset)
            else:
                # Regular incremental query after the last_ts
                current_query = f"SELECT * FROM `{table_name}` WHERE `{timestamp_column}` > %s AND `{timestamp_column}` <= %s ORDER BY `{timestamp_column}` ASC LIMIT %s OFFSET %s"
                current_params = (last_ts, pull_start_time, chunk_size, offset)

            cursor.execute(current_query, current_params)
            rows = cursor.fetchall()

            if not rows:
                break

            df = pd.DataFrame(rows)

            # Update current_run_max_ts
            # Ensure the timestamp column exists and is datetime type in DataFrame
            if timestamp_column in df.columns and not df[timestamp_column].empty:
                max_in_chunk = df[timestamp_column].max()
                if current_run_max_ts is None or max_in_chunk > current_run_max_ts:
                    current_run_max_ts = max_in_chunk

            # Append to Iceberg
            iceberg_table.append(df)  # Appends data as new files

            total_rows += len(rows)
            offset += chunk_size
            print(
                f"Table: {table_name}, Processed: {total_rows} rows. Latest timestamp in chunk: {max_in_chunk}"
            )

        # After successful completion, update the last_pulled_timestamp
        if current_run_max_ts:
            update_last_pulled_timestamp(table_name, current_run_max_ts)
        else:
            # If no new rows were processed but it was an initial load, set to pull_start_time
            # to prevent re-processing the same range
            if initial_load and total_rows == 0:
                update_last_pulled_timestamp(table_name, pull_start_time)

        print(
            f"Finished incremental load for {table_name}: {total_rows} rows processed."
        )
        return f"Finished incremental load for {table_name}: {total_rows} rows."

    except Exception as e:
        print(f"Error during incremental load for {table_name}: {e}")
        raise  # Re-raise for Celery to mark as failed
    finally:
        if cnx:
            cnx.close()


# Example of how to call this task (from a client script or another Celery task)
# app.send_task('data_pull.extract_and_load_incremental',
#               args=('my_users_table', 'updated_at', my_mysql_config, my_iceberg_config))
