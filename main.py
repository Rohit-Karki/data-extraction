from celery_app.tasks import extract_and_load_table


def main():
    result = extract_and_load_table.delay(
        table_name="transactions",
        # db_config=db_config,
        # iceberg_catalog_config=iceberg_catalog_config,
        # primary_key_column="id",
        # start_id=0,
        # end_id=10,
    )

    print("Task submitted:", result.id)
    print("This is a simple Iceberg example using PyIceberg.")


if __name__ == "__main__":
    main()
