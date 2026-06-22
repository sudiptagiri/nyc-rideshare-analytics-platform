# ingest/fetch_tlc.py
# Pulls REAL NYC Yellow Taxi trip data from Socrata API
# Pulls NYC taxi zone lookup from official TLC CDN
# Saves both as CSV files ready to load into Databricks

import requests
import pandas as pd
import os
from io import StringIO
from dotenv import load_dotenv

load_dotenv()


# ── Step 1: Pull 50,000 real taxi trips from Socrata API ─────────
def fetch_trips():
    print("Fetching NYC Yellow Taxi trips from Socrata API...")

    url = "https://data.cityofnewyork.us/resource/qp3b-zxtp.json"
    params = {
        "$limit": 50000,
        "$where": "tpep_pickup_datetime >= '2022-01-01T00:00:00' AND "
                  "tpep_pickup_datetime <  '2022-02-01T00:00:00'",
        "$select": "vendorid,tpep_pickup_datetime,tpep_dropoff_datetime,"
                   "passenger_count,trip_distance,pulocationid,dolocationid,"
                   "payment_type,fare_amount,tip_amount,total_amount,"
                   "congestion_surcharge"
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    trips = pd.DataFrame(response.json())
    print(f"  Got {len(trips):,} trips")
    return trips


# ── Step 2: Pull 265 NYC taxi zones from TLC CDN ─────────────────
def fetch_zones():
    print("Fetching NYC taxi zone names...")

    # Official NYC TLC zone lookup — hosted on AWS CloudFront CDN
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    zones = pd.read_csv(StringIO(response.text))
    zones.columns = [c.lower() for c in zones.columns]  # lowercase all column names
    print(f"  Got {len(zones)} zones")
    return zones


# ── Step 3: Save as CSV files ────────────────────────────────────
def save_csv(df, filename):
    path = os.path.join("ingest", filename)
    df.to_csv(path, index=False)
    print(f"  Saved {path} ({len(df):,} rows)")


# ── Step 4: Preview the data ─────────────────────────────────────
def preview(trips, zones):
    print("\n" + "=" * 50)
    print("PREVIEW — first 3 trips")
    print("=" * 50)
    print(trips[["tpep_pickup_datetime", "trip_distance",
                 "fare_amount", "tip_amount",
                 "pulocationid", "dolocationid"]].head(3).to_string())

    print("\n" + "=" * 50)
    print("PREVIEW — first 5 zones")
    print("=" * 50)
    print(zones.head(5).to_string())


# ── Main ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("NYC Taxi Data Ingestion")
    print("=" * 50)

    # Fetch from API and CDN
    trips = fetch_trips()
    zones = fetch_zones()

    # Save to ingest/ folder
    save_csv(trips, "raw_trips.csv")
    save_csv(zones, "raw_zones.csv")

    # Summary
    print("\n" + "=" * 50)
    print("DONE!")
    print("=" * 50)
    print(f"Trips  : {trips.shape[0]:,} rows  x  {trips.shape[1]} columns")
    print(f"Zones  : {zones.shape[0]:,} rows  x  {zones.shape[1]} columns")
    print(f"\nTrip columns : {list(trips.columns)}")
    print(f"Zone columns : {list(zones.columns)}")

    # Preview
    preview(trips, zones)