from sqlalchemy import create_engine
import pandas as pd

oracle_url = (
    "oracle+oracledb://TEST_USER:strong_pass@127.0.0.1:1521/?service_name=freepdb1"
)
# oracle_url = "oracle+oracledb://TEST_USER:strong_pass@127.0.0.1:1521/XE"

oracle_engine = create_engine(oracle_url)
with oracle_engine.connect() as conn:
    # df = pd.read_sql(
    #     "SELECT username, account_status FROM dba_users",
    #     conn,
    # )
    print("Connected to Oracle database successfully!")
    # print("Data from employees table:", df.head())
