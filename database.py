import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "mydb",
}

# Create engine with connection pooling
DATABASE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # Verify connections before use
    pool_recycle=3600,  # Recycle connections every hour
    pool_timeout=30,
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for declarative models
Base = declarative_base()

try:
    # Test connection
    def test_connection():
        """Test database connection"""
        try:
            with engine.connect() as connection:
                result = connection.execute(text("SELECT VERSION()"))
                print("MySQL Version:", result.fetchone()[0])
            print("Connection pool created successfully")
        except Exception as e:
            print(f"Error: {e}")

    test_connection()

    def get_db():
        """Get database session"""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def read_from_db(query: str):
        """Read data from database using pandas"""
        try:
            with engine.connect() as connection:
                return pd.read_sql_query(text(query), connection)
        except Exception as e:
            print(f"Error while reading data: {e}")
            return None

    def execute_query(query: str):
        """Execute a raw SQL query and return results"""
        try:
            with engine.connect() as connection:
                result = connection.execute(text(query))
                return result.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def update_metadata(
        table_name: str, last_ingested_time=None, is_running: bool = False
    ):
        """Update ingestion metadata for a table"""
        try:
            with engine.connect() as connection:
                if last_ingested_time:
                    query = f"UPDATE ingestion_metadata SET last_ingested_time = '{last_ingested_time}', is_running = {is_running}, updated_at = NOW() WHERE table_name = '{table_name}'"
                else:
                    query = f"UPDATE ingestion_metadata SET is_running = {is_running}, updated_at = NOW() WHERE table_name = '{table_name}'"
                connection.execute(text(query))
                connection.commit()
        except Exception as e:
            print(f"Error updating metadata: {e}")

            # Example of how to use the session in a context manager:
            # with SessionLocal() as session:
            #     result = session.execute(text("SELECT * FROM your_table"))
            #     data = result.fetchall()
            return pd.read_sql(query, connection)
        except Error as e:
            print(f"Error while reading data: {e}")
            return None

finally:
    pass
