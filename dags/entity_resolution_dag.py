"""
Weekly-scheduled Airflow DAG wrapping the entity resolution pipeline.

This mirrors run_pipeline.py exactly -- extract+clean both sources, match,
validate -- just split into Airflow tasks so each stage gets its own
retries, logging, and pass/fail status in the UI instead of living inside
one long-running script.

DESIGN NOTE -- how data moves between tasks:
Each task writes its output to data/processed/ and returns the file PATH
(a small string) through XCom, not the dataframe itself. XCom is backed
by Airflow's metadata database and isn't meant for megabyte-scale
payloads -- passing ~15k-43k row dataframes through it directly is a
common beginner mistake that quietly degrades the scheduler. Writing to
disk and passing paths is the standard pattern here.

DESIGN NOTE -- task isolation:
Each task independently loads whatever raw/clean data it needs rather
than trusting another task's in-memory state, since tasks can run in
different worker processes. That means the raw source files get read a
few times across one DAG run (small cost, ~15MB of CSV) in exchange for
each task being safely retryable/rerunnable on its own -- the trade-off
worth making for a pipeline this size.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Airflow's dags_folder is often configured separately from where the
# project actually lives, so make the project's own packages (extract/,
# transform/) importable regardless of where this file gets picked up from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pendulum
from airflow.sdk import dag, task

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Same floor used implicitly throughout tuning (v1/v2/v3 all cleared this).
# If a future data or code change drops precision below this, fail the run
# loudly instead of silently shipping a worse matcher downstream.
MIN_ACCEPTABLE_PRECISION = 0.90


@dag(
    dag_id="entity_resolution_pipeline",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["portfolio", "entity-resolution"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def entity_resolution_pipeline():

    @task()
    def extract_and_clean_business_licenses() -> str:
        from extract.sources import load_business_licenses
        from transform.clean import clean_business_licenses

        df = clean_business_licenses(load_business_licenses())
        out_path = PROCESSED_DIR / "business_licenses_clean.csv"
        df.to_csv(out_path, index=False)
        print(f"cleaned {len(df):,} business licenses -> {out_path}")
        return str(out_path)

    @task()
    def extract_and_clean_food_inspections() -> str:
        from extract.sources import load_food_inspections
        from transform.clean import clean_food_inspections

        # exclude_facility_types defaults to True -- see transform/clean.py
        df = clean_food_inspections(load_food_inspections())
        out_path = PROCESSED_DIR / "food_inspections_clean.csv"
        df.to_csv(out_path, index=False)
        print(f"cleaned {len(df):,} food inspections -> {out_path}")
        return str(out_path)

    @task()
    def run_matching(bl_path: str, fi_path: str) -> str:
        import pandas as pd
        from transform.match import match_records, _BACKEND

        print(f"fuzzy matching backend: {_BACKEND}")
        bl_clean = pd.read_csv(bl_path, dtype=str)
        fi_clean = pd.read_csv(fi_path, dtype=str)

        results = match_records(fi_clean, bl_clean)
        out_path = PROCESSED_DIR / "match_results.csv"
        results.to_csv(out_path, index=False)
        print(results["match_status"].value_counts().to_string())
        return str(out_path)

    @task()
    def validate_results(results_path: str) -> dict:
        import pandas as pd
        from extract.sources import load_business_licenses, load_food_inspections
        from transform.validate import build_ground_truth, score

        # dtype=str matters here: without it, pandas infers inspection_id
        # as int64 on the round-trip through CSV, which then fails to
        # merge against the ground truth's string-typed Inspection ID.
        # This didn't show up when everything ran in one in-memory script
        # (run_pipeline.py) -- it only appears once data crosses a disk
        # boundary between tasks, which is exactly the kind of bug task
        # isolation is worth catching before it reaches production.
        results = pd.read_csv(results_path, dtype=str)
        ground_truth = build_ground_truth(load_food_inspections(), load_business_licenses())
        metrics = score(results, ground_truth)
        print(f"validation metrics: {metrics}")

        if metrics["precision"] < MIN_ACCEPTABLE_PRECISION:
            raise ValueError(
                f"Precision {metrics['precision']:.2%} fell below the "
                f"{MIN_ACCEPTABLE_PRECISION:.0%} floor -- failing this run "
                "instead of silently shipping a worse matcher downstream."
            )
        return metrics

    bl_path = extract_and_clean_business_licenses()
    fi_path = extract_and_clean_food_inspections()
    results_path = run_matching(bl_path, fi_path)
    validate_results(results_path)


entity_resolution_pipeline()
