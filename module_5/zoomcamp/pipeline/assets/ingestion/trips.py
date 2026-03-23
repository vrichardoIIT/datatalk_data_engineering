"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
# columns:
#   - name: TODO_col1
#     type: TODO_type
#     description: TODO

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import json, os, pandas as pd
from datetime import datetime
from typing import List, Tuple
from dateutil.relativedelta import relativedelta

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def generate_months(start_date, end_date):
    """Generate monthly intervals between start and end date."""
    months = []

    current = start_date.replace(day=1)

    while current <= end_date:
        months.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)

    return months


def build_urls(months, taxi_types):
    """Construct source URLs."""
    base = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    urls = []

    for taxi in taxi_types:
        for m in months:
            urls.append(
                (
                    taxi,
                    f"{base}/{taxi}_tripdata_{m}.parquet"
                )
            )

    return urls


def materialize():
    """
    Ingest NYC taxi trips based on Bruin runtime variables.
    """

    # Bruin runtime window
    start_date = datetime.fromisoformat(os.environ["BRUIN_START_DATE"])
    end_date = datetime.fromisoformat(os.environ["BRUIN_END_DATE"])

    # Pipeline variables
    vars_json = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_json.get("taxi_types", ["yellow"])

    months = generate_months(start_date, end_date)
    urls = build_urls(months, taxi_types)

    dfs = []

    for taxi, url in urls:
        try:
            df = pd.read_parquet(url)

            df["taxi_type"] = taxi
            df["extracted_at"] = datetime.utcnow()

            dfs.append(df)

        except Exception as e:
            print(f"Skipping {url} -> {e}")

    if not dfs:
        return pd.DataFrame()

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


