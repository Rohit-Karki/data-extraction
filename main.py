from celery_app.tasks import (
    extract_and_load_table,
    extract_and_load_table_using_partitioning,
)
from database import engine, text, read_from_db, update_metadata
import logging
from datetime import date
from timestamp_based import extract_and_load_table_incremental


def orchestrate_ingestion():
    try:
        # Get tables to ingest
        query = "SELECT * FROM ingestion_metadata WHERE is_running = FALSE"
        tables_to_ingest = read_from_db(query).to_dict("records")
        print("Tables to ingest:", tables_to_ingest)

        for entry in tables_to_ingest:
            print("Processing table:", entry["table_name"])
            table_name = entry["table_name"]
            last_ingested_time = entry["last_ingested_time"]
            primary_key = entry["primary_key"]

            # Mark job as running
            update_metadata(table_name, is_running=True)

            # Dispatch Celery task with starting point
            result = extract_and_load_table.apply_async(
                args=[table_name, primary_key, last_ingested_time]
            )

            logger = logging.getLogger(__name__)
            logger.info(f"Dispatching task for {table_name}")
            print("Task submitted:", result.id)

    except Exception as e:
        print(f"Error in orchestration: {e}")


def orchestrate_incremental_ingestion():
    try:
        # Get tables to ingest
        query = "SELECT * FROM ingestion_metadata WHERE is_running = FALSE"
        tables_to_ingest = read_from_db(query).to_dict("records")
        print("Tables to ingest:", tables_to_ingest)

        for entry in tables_to_ingest:
            print("Processing table:", entry["table_name"])
            table_name = entry["table_name"]
            incremental_date = entry["last_ingested_time"]
            primary_key = entry["primary_key"]
            # Mark job as running
            update_metadata(table_name, is_running=True)

            # Dispatch Celery task with starting point
            result = extract_and_load_table_incremental.apply_async(
                args=[table_name, primary_key, incremental_date, False]
            )

            logger = logging.getLogger(__name__)
            logger.info(
                f"Dispatching task for {table_name} and task submitted {result.id}"
            )

    except Exception as e:
        print(f"Error in incremental orchestration: {e}")


def generate_year_partitions(start_year, end_year):
    partitions = []
    for year in range(start_year, end_year + 1):
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        partitions.append((start_date, end_date))
    return partitions


def main():

    orchestrate_incremental_ingestion()
    # year_ranges = generate_year_partitions(2015, 2025)
    # for start_date, end_date in year_ranges:
    #     print(f"Partition: {start_date} to {end_date}")

    #     extract_and_load_table_using_partitioning.apply_async(
    #         args=[
    #             "Drivers",
    #             "app_date_clean",  # optional
    #             start_date,
    #             end_date,
    #         ]
    #     )

    # orchestrate_ingestion()
    print("This is a simple Iceberg example using PyIceberg.")


if __name__ == "__main__":
    main()
