import os
import urllib.request
from pathlib import Path

import duckdb
from dotenv import load_dotenv

# --------------------------------
# 1. LOAD PROJECT CONFIGURATION
# --------------------------------

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"

DB_PATH = Path(
    os.getenv("DUCKDB_PATH", "data/yellow_taxi.duckdb")
)

FILE_NAME = "yellow_tripdata_2025-01.parquet"

URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    + FILE_NAME
)

FILE_PATH = RAW_DIR / FILE_NAME

# --------------------------------
# 2. CREATE REQUIRED DIRECTORIES
# --------------------------------

RAW_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


# --------------------------------
# 3. DOWNLOAD TAXI DATA
# --------------------------------

def download_data():

    if FILE_PATH.exists():
        print("Taxi data already exists.")
        return

    print("Downloading Yellow Taxi data...")

    try:
        urllib.request.urlretrieve(URL, FILE_PATH)
        print("Download completed successfully.")

    except Exception as error:
        print("Download failed:", error)
        raise


# --------------------------------
# 4. CONNECT TO DUCKDB
# --------------------------------

def connect_database():

    print("Connecting to DuckDB database...")

    connection = duckdb.connect(str(DB_PATH))

    print("Database connected successfully.")

    return connection


# --------------------------------
# 5. CREATE SOURCE VIEW
# --------------------------------

def create_source(connection):

    print("Reading Parquet data...")

    connection.from_parquet(
        str(FILE_PATH)
    ).create_view(
        "source",
        replace=True
    )

    print("Source data loaded successfully.")


# --------------------------------
# 6. COUNT INPUT RECORDS
# --------------------------------

def count_records(connection):

    rows_in = connection.execute(
        "SELECT COUNT(*) FROM source"
    ).fetchone()[0]

    print("Total input records:", rows_in)

    return rows_in


# --------------------------------
# 7. DATA VALIDATION RULES
# --------------------------------

VALIDATION_CONDITION = """
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND passenger_count >= 0
    AND trip_distance >= 0
    AND fare_amount >= 0
    AND tpep_dropoff_datetime >= tpep_pickup_datetime
"""


# --------------------------------
# 8. LOAD VALID RECORDS
# --------------------------------

def load_valid_records(connection):

    print("Validating taxi records...")

    connection.execute(f"""
        CREATE OR REPLACE TABLE loaded AS
        SELECT *
        FROM source
        WHERE {VALIDATION_CONDITION}
    """)

    print("Valid records loaded successfully.")


# --------------------------------
# 9. QUARANTINE INVALID RECORDS
# --------------------------------

def quarantine_invalid_records(connection):

    connection.execute(f"""
        CREATE OR REPLACE TABLE quarantined AS
        SELECT *
        FROM source
        WHERE NOT COALESCE(
            ({VALIDATION_CONDITION}),
            FALSE
        )
    """)

    print("Invalid records stored separately.")


# --------------------------------
# 10. GENERATE VALIDATION REPORT
# --------------------------------

def generate_report(connection, rows_in):

    loaded = connection.execute(
        "SELECT COUNT(*) FROM loaded"
    ).fetchone()[0]

    quarantined = connection.execute(
        "SELECT COUNT(*) FROM quarantined"
    ).fetchone()[0]

    print("\n")
    print("=" * 45)
    print("       YELLOW TAXI DATA PIPELINE REPORT")
    print("=" * 45)

    print("Total input records :", rows_in)
    print("Valid records       :", loaded)
    print("Invalid records     :", quarantined)

    print("-" * 45)

    if rows_in == loaded + quarantined:
        print("Data validation check: PASSED")
    else:
        print("Data validation check: FAILED")

    print("=" * 45)


# --------------------------------
# 11. MAIN PIPELINE EXECUTION
# --------------------------------

def main():

    connection = None

    try:

        print("\nStarting Yellow Taxi Data Pipeline...\n")

        download_data()

        connection = connect_database()

        create_source(connection)

        rows_in = count_records(connection)

        load_valid_records(connection)

        quarantine_invalid_records(connection)

        generate_report(connection, rows_in)

        print("\nPipeline completed successfully!")

    except Exception as error:

        print("\nPipeline execution failed!")
        print("Error:", error)

        raise

    finally:

        if connection is not None:
            connection.close()
            print("Database connection closed.")


# --------------------------------
# 12. RUN PROJECT
# --------------------------------

if __name__ == "__main__":
    main()
