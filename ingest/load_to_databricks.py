# ingest/load_to_databricks.py
# Reads the CSV files saved by fetch_tlc.py
# Loads them directly into Databricks as tables
# Run AFTER fetch_tlc.py has been run

import pandas as pd
import os
from databricks import sql
from dotenv import load_dotenv

load_dotenv()


# ── Connect to Databricks ─────────────────────────────────────────
def get_connection():
    print("Connecting to Databricks...")
    conn = sql.connect(
        server_hostname = os.getenv("DBT_HOST"),
        http_path       = os.getenv("DBT_HTTP_PATH"),
        access_token    = os.getenv("DBT_TOKEN")
    )
    print("  Connected!")
    return conn


# ── Create schema ─────────────────────────────────────────────────
def create_schema(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS nyc_raw")
    print("  Schema nyc_raw ready")


# ── Load one CSV into a Databricks table ──────────────────────────
def load_table(cursor, df, table_name):
    print(f"\nLoading nyc_raw.{table_name}...")
    print(f"  Rows    : {len(df):,}")
    print(f"  Columns : {list(df.columns)}")

    # Drop and recreate for clean load
    cursor.execute(f"DROP TABLE IF EXISTS nyc_raw.{table_name}")

    # Map pandas dtypes → Databricks SQL types
    type_map = {
        "object":  "STRING",
        "float64": "DOUBLE",
        "int64":   "BIGINT",
        "bool":    "BOOLEAN"
    }

    col_defs = []
    for col, dtype in df.dtypes.items():
        sql_type = type_map.get(str(dtype), "STRING")
        col_defs.append(f"`{col}` {sql_type}")

    cursor.execute(f"""
        CREATE TABLE nyc_raw.{table_name} (
            {', '.join(col_defs)}
        )
    """)
    print(f"  Table created")

    # Insert in batches of 500 rows
    batch_size = 500
    total      = len(df)
    inserted   = 0

    for start in range(0, total, batch_size):
        batch = df.iloc[start:start + batch_size]
        rows  = []

        for _, row in batch.iterrows():
            vals = []
            for val in row:
                if pd.isna(val):
                    vals.append("NULL")
                elif isinstance(val, str):
                    escaped = str(val).replace("'", "\\'")
                    vals.append(f"'{escaped}'")
                else:
                    vals.append(str(val))
            rows.append(f"({', '.join(vals)})")

        cursor.execute(
            f"INSERT INTO nyc_raw.{table_name} VALUES {', '.join(rows)}"
        )
        inserted += len(batch)

        # Progress every 5000 rows
        if inserted % 5000 == 0 or inserted == total:
            pct = (inserted / total) * 100
            print(f"  Progress: {inserted:,} / {total:,} rows ({pct:.0f}%)")

    print(f"  nyc_raw.{table_name} — DONE!")


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("Load CSV files into Databricks")
    print("=" * 50)

    # Check CSV files exist
    trips_path = os.path.join("ingest", "raw_trips.csv")
    zones_path = os.path.join("ingest", "raw_zones.csv")

    if not os.path.exists(trips_path):
        print(f"ERROR: {trips_path} not found!")
        print("Run fetch_tlc.py first.")
        exit(1)

    if not os.path.exists(zones_path):
        print(f"ERROR: {zones_path} not found!")
        print("Run fetch_tlc.py first.")
        exit(1)

    # Read CSVs
    print("\nReading CSV files...")
    trips = pd.read_csv(trips_path)
    zones = pd.read_csv(zones_path)
    print(f"  raw_trips : {len(trips):,} rows")
    print(f"  raw_zones : {len(zones):,} rows")

    # Connect and load
    conn   = get_connection()
    cursor = conn.cursor()

    create_schema(cursor)

    # Load zones first (265 rows — fast)
    load_table(cursor, zones, "raw_zones")

    # Load trips second (50,000 rows — takes 3-5 min)
    load_table(cursor, trips, "raw_trips")

    cursor.close()
    conn.close()

    # Done
    print("\n" + "=" * 50)
    print("ALL DONE!")
    print("=" * 50)
    print("Tables ready in Databricks:")
    print("  nyc_raw.raw_zones  —    265 rows")
    print("  nyc_raw.raw_trips  — 50,000 rows")
    print("\nVerify in Databricks SQL Editor:")
    print("  SELECT COUNT(*) FROM nyc_raw.raw_trips;")
    print("  SELECT COUNT(*) FROM nyc_raw.raw_zones;")