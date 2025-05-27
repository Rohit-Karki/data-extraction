import mysql.connector
from mysql.connector import Error
import pandas as pd
from mysql.connector.pooling import MySQLConnectionPool

dbconfig = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "mydb",
}

try:
    cnx_pool = MySQLConnectionPool(pool_name="mypool", pool_size=5, **dbconfig)
    print("Connection pool created successfully")

    # Get a connection from the pool
    conn = cnx_pool.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT VERSION();")
    print("MySQL Version:", cursor.fetchone()[0])
    cursor.close()

    # Release the connection back to the pool
    conn.close()  # This returns the connection to the pool
    print("Connection released back to pool")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    # No explicit close for MySQLConnectionPool, connections are managed by the pool.
    # When the pool object goes out of scope, connections are closed.
    pass


def create_mysql_connection(
    # host="localhost",
    # user="your_username",
    # password="your_password",
    # database="your_database",
):
    """Create MySQL connection"""
    try:
        # connection = mysql.connector.connect(
        #     host=host, user=user, password=password, database=database
        # )
        # # Use the connection pool to get a connection
        global cnx_pool
        if not cnx_pool:
            raise ValueError("Connection pool is not initialized.")
        connection = cnx_pool.get_connection()
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None


def read_from_mysql(connection, query):
    """Read data from MySQL using pandas"""
    try:
        return pd.read_sql(query, connection)
    except Error as e:
        print(f"Error while reading data: {e}")
        return None
