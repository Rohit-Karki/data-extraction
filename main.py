from celery_app.tasks import (
    extract_and_load_table,
    extract_and_load_table_using_partitioning,
)
from celery_app.incremental import extract_and_load_table_incremental
from database import engine, text, read_from_db, update_metadata
import logging
from datetime import date


# The ingestion orchestration function that manages the ingestion process
# and dispatches tasks to Celery workers.
# def orchestrate_ingestion():
#     try:
#         # Get tables to ingest
#         query = "SELECT * FROM ingestion_metadata WHERE is_running = FALSE"
#         tables_to_ingest = read_from_db(query).to_dict("records")
#         print("Tables to ingest:", tables_to_ingest)

#         for entry in tables_to_ingest:
#             print("Processing table:", entry["table_name"])
#             database_name = entry["database_name"]
#             database_url = entry["database_url"]
#             table_name = entry["table_name"]
#             last_ingested_time = entry["last_ingested_time"]
#             primary_key = entry["primary_key"]

#             # Mark job as running
#             update_metadata(table_name, is_running=True)

#             # Dispatch Celery task with starting point
#             result = extract_and_load_table.apply_async(
#                 args=[table_name, primary_key, last_ingested_time]
#             )

#             logger = logging.getLogger(__name__)
#             logger.info(f"Dispatching task for {table_name}")
#             print("Task submitted:", result.id)

#     except Exception as e:
#         print(f"Error in orchestration: {e}")
#     finally:
#         update_metadata(None, is_running=False)


# def orchestrate_partitioned_ingestion():
#     try:
#         # Get tables to ingest
#         query = "SELECT * FROM ingestion_metadata WHERE is_running = FALSE"
#         tables_to_ingest = read_from_db(query).to_dict("records")
#         # print("Tables to ingest:", tables_to_ingest)

#         for entry in tables_to_ingest:
#             table_name = entry["table_name"]
#             # if table_name == "ai_job_dataset" or table_name == "transactions":
#             #     continue
#             print("Processing table:", entry["table_name"])
#             database_name = entry["database_name"]
#             database_url = entry["database_url"]
#             incremental_date = entry["last_ingested_time"]
#             primary_key = entry["primary_key"]
#             # Mark job as running
#             update_metadata(table_name, is_running=True)

#             # Dispatch Celery task with starting point
#             year_ranges = generate_year_partitions(2015, 2025)
#             for start_date, end_date in year_ranges:
#                 print(f"Partition: {start_date} to {end_date}")

#             result = extract_and_load_table_using_partitioning.apply_async(
#                 args=[
#                     table_name,
#                     primary_key,
#                     start_date,
#                     end_date,
#                 ]
#             )

#             logger = logging.getLogger(__name__)
#             logger.info(
#                 f"Dispatching task for {table_name} and task submitted {result.id}"
#             )

#     except Exception as e:
#         print(f"Error in incremental orchestration: {e}")
#     finally:
#         update_metadata(None, is_running=False)


def orchestrate_incremental_ingestion():
    try:
        # Get tables to ingest
        query = "SELECT * FROM ingestion_metadata WHERE is_running = FALSE"
        tables_to_ingest = read_from_db(query).to_dict("records")
        # print("Tables to ingest:", tables_to_ingest)

        for entry in tables_to_ingest:
            table_name = entry["table_name"]
            # if table_name == "ai_job_dataset" or table_name == "transactions":
            #     continue
            print("Processing table:", entry["table_name"])
            database_name = entry["database_name"]
            database_url = entry["database_url"]
            incremental_date = entry["last_ingested_time"]
            primary_key = entry["primary_key"]
            # Mark job as running
            update_metadata(table_name, is_running=True)

            # Dispatch Celery task with starting point
            result = extract_and_load_table_incremental.apply_async(
                args=[
                    table_name,
                    primary_key,
                    incremental_date,
                    database_url,
                    False,
                    database_name,
                ]
            )

            logger = logging.getLogger(__name__)
            logger.info(
                f"Dispatching task for {table_name} and task submitted {result.id}"
            )

    except Exception as e:
        print(f"Error in incremental orchestration: {e}")
    finally:
        update_metadata(None, is_running=False)


def generate_year_partitions(start_year, end_year):
    partitions = []
    for year in range(start_year, end_year + 1):
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        partitions.append((start_date, end_date))
    return partitions


def main():
    orchestrate_incremental_ingestion()
    # orchestrate_ingestion()


if __name__ == "__main__":
    main()
