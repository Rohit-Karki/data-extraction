from sqlalchemy import create_engine
import pandas as pd

oracle_url = "oracle+oracledb://system:testpassword@0.0.0.0:1521/?service_name=free"

oracle_engine = create_engine(oracle_url)
with oracle_engine.connect() as conn:
    df = pd.read_sql(
        "SELECT * FROM employees",
        conn,
    )
    print("Connected to Oracle database successfully!")
    print("Data from employees table:", df.head())
