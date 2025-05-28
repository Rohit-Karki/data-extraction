from celery_app.tasks import (
    extract_and_load_table,
    extract_and_load_table_using_partitioning,
)
from database import create_mysql_connection
import logging
from datetime import date


def orchestrate_ingestion():
    cnx = create_mysql_connection(
        # host="localhost",
        # user="root",
        # password="rootpassword",
        # database="mydb",
        # port=3306,
    )
    cursor = cnx.cursor(dictionary=True)

    cursor.execute("SELECT * FROM ingestion_metadata WHERE is_running = FALSE")
    tables_to_ingest = cursor.fetchall()
    print("Tables to ingest:", tables_to_ingest)

    for entry in tables_to_ingest:
        print("Processing table:", entry["table_name"])
        table_name = entry["table_name"]

        # if table_name == "transactions":
        #     continue  # Skip the transactions table

        last_id = entry["last_ingested_id"]
        primary_key = entry["primary_key"]

        # primary_key = "App Date"
        # Mark job as running
        cursor.execute(
            "UPDATE ingestion_metadata SET is_running = TRUE, updated_at = NOW() WHERE table_name = %s",
            (table_name,),
        )
        cnx.commit()

        # Dispatch Celery task with starting point
        result = extract_and_load_table.apply_async(
            args=[table_name, primary_key, last_id]
        )
        # db_config=db_config,
        # iceberg_catalog_config=iceberg_catalog_config,
        # end_id=10,

        logger = logging.getLogger(__name__)
        logger.info(f"Dispatching task for {table_name}")
        print("Task submitted:", result.id)

    cursor.close()
    cnx.close()


def generate_year_partitions(start_year, end_year):
    partitions = []
    for year in range(start_year, end_year + 1):
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        partitions.append((start_date, end_date))
    return partitions


def main():
    cnx = create_mysql_connection(
        # host="localhost",
        # user="root",
        # password="rootpassword",
        # database="mydb",
        # port=3306,
    )
    cursor = cnx.cursor(dictionary=True)

    year_ranges = generate_year_partitions(1998, 2025)
    for start_date, end_date in year_ranges:
        print(f"Partition: {start_date} to {end_date}")

        extract_and_load_table_using_partitioning.apply_async(
            args=[
                "Drivers",
                "app_date_clean",  # optional
                start_date,
                end_date,
            ]
        )

    # orchestrate_ingestion()
    print("This is a simple Iceberg example using PyIceberg.")


if __name__ == "__main__":
    main()
