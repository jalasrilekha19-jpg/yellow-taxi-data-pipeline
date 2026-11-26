import os
import urllib.request
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

# Project configuration
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
DB_PATH = Path(os.getenv("DUCKDB_PATH", "data/yellow_taxi.duckdb"))

URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2025-01.parquet"
)

RAW_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

FILE_PATH = RAW_DIR / "yellow_tripdata_2025-01.parquet"

# Step 1: Download taxi data
if not FILE_PATH.exists():
    print("Downloading Yellow Taxi data...")
    urllib.request.urlretrieve(URL, FILE_PATH)

print("Download completed!")

# Step 2: Connect to DuckDB
con = duckdb.connect(str(DB_PATH))

# Step 3: Read source data
con.execute(
    "CREATE OR REPLACE VIEW source AS SELECT * FROM read_parquet(?)",
    [str(FILE_PATH)]
)

# Step 4: Count input rows
rows_in = con.execute(
    "SELECT COUNT(*) FROM source"
).fetchone()[0]

# Step 5: Validate and load records
condition = """
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND passenger_count >= 0
    AND trip_distance >= 0
    AND fare_amount >= 0
    AND tpep_dropoff_datetime >= tpep_pickup_datetime
"""

con.execute(f"""
    CREATE OR REPLACE TABLE loaded AS
    SELECT * FROM source
    WHERE {condition}
""")

# Step 6: Store invalid records separately
con.execute(f"""
    CREATE OR REPLACE TABLE quarantined AS
    SELECT * FROM source
    WHERE NOT COALESCE(({condition}), FALSE)
""")

# Step 7: Print validation results
loaded = con.execute(
    "SELECT COUNT(*) FROM loaded"
).fetchone()[0]

quarantined = con.execute(
    "SELECT COUNT(*) FROM quarantined"
).fetchone()[0]

print("\n--- YELLOW TAXI VALIDATION REPORT ---")
print("Rows in:", rows_in)
print("Loaded:", loaded)
print("Quarantined:", quarantined)

con.close()
