from celery_app.tasks import extract_and_load_table
from database import create_mysql_connection


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
        last_id = entry["last_ingested_id"]

        # Mark job as running
        cursor.execute(
            "UPDATE ingestion_metadata SET is_running = TRUE, last_ingested_id = NOW() WHERE table_name = %s",
            (table_name,),
        )
        cnx.commit()

        # Dispatch Celery task with starting point
        result = extract_and_load_table.delay(
            table_name=table_name,
            # db_config=db_config,
            # iceberg_catalog_config=iceberg_catalog_config,
            # primary_key_column="id",
            start_id=last_id,
            primary_key_column="id",
            # end_id=10,
        )
        print("Task submitted:", result.id)

    cursor.close()
    cnx.close()


def main():
    orchestrate_ingestion()
    print("This is a simple Iceberg example using PyIceberg.")


if __name__ == "__main__":
    main()
