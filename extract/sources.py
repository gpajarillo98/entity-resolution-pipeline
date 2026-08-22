"""
Extract layer.

For this local build, both sources are read from CSV exports (already
filtered to the last 2 years). In the Airflow version of this pipeline,
these functions get swapped for live pulls against the Socrata SODA API:

    https://data.cityofchicago.org/resource/r5kz-chrr.json   (Business Licenses)
    https://data.cityofchicago.org/resource/4ijn-s7e5.json   (Food Inspections)

Both are free, no API key required for reasonable request volumes.
A live pull looks like:

    import requests
    resp = requests.get(
        "https://data.cityofchicago.org/resource/r5kz-chrr.json",
        params={"$where": "date_issued > '2024-08-19'", "$limit": 50000},
    )

Keeping the CSV-reading and API-reading paths behind the same function
signatures below means the rest of the pipeline doesn't care which one
is active -- that swap is the whole point of an extract layer.
"""

import pandas as pd

BUSINESS_LICENSES_PATH = "data/raw/Business_Licenses_-_Current_Active_20260819.csv"
FOOD_INSPECTIONS_PATH = "data/raw/Food_Inspections_20260819.csv"


def load_business_licenses(path: str = BUSINESS_LICENSES_PATH) -> pd.DataFrame:
    """Load raw business license records. One row per license event
    (issue, renewal, change of location, etc.) -- NOT one row per business.
    Collapsing to one-row-per-business happens in transform/clean.py.
    """
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return df


def load_food_inspections(path: str = FOOD_INSPECTIONS_PATH) -> pd.DataFrame:
    """Load raw food inspection records. One row per inspection event --
    a single restaurant can have many rows."""
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    return df


if __name__ == "__main__":
    bl = load_business_licenses()
    fi = load_food_inspections()
    print(f"business licenses: {len(bl):,} rows")
    print(f"food inspections:  {len(fi):,} rows")
