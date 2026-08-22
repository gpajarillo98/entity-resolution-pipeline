"""
Run the full pipeline locally: extract -> clean -> match -> validate.

    python run_pipeline.py
    python run_pipeline.py --sample 500     # faster iteration while developing

This is the script Airflow will eventually call from a DAG task -- kept
runnable standalone so the matching logic can be developed and tested
without standing up Airflow first.
"""

import argparse
import time

from extract.sources import load_business_licenses, load_food_inspections
from transform.clean import clean_business_licenses, clean_food_inspections
from transform.match import match_records, _BACKEND
from transform.validate import build_ground_truth, score


def main(sample: int | None, include_facility_types: bool):
    print(f"fuzzy matching backend: {_BACKEND}"
          + ("" if _BACKEND == "rapidfuzz" else
             "  (install rapidfuzz for real runtime -- see requirements.txt)"))

    bl_raw = load_business_licenses()
    fi_raw = load_food_inspections()
    print(f"loaded {len(bl_raw):,} license rows, {len(fi_raw):,} inspection rows")

    bl_clean = clean_business_licenses(bl_raw)
    fi_clean = clean_food_inspections(fi_raw, exclude_facility_types=not include_facility_types)
    print(f"collapsed to {len(bl_clean):,} businesses, {len(fi_clean):,} inspection records"
          + ("" if not include_facility_types else " (institutional facility types included)"))

    if sample:
        fi_clean = fi_clean.sample(sample, random_state=42).reset_index(drop=True)
        print(f"sampling {sample} inspection records for this run")

    t0 = time.time()
    results = match_records(fi_clean, bl_clean)
    print(f"matched {len(results):,} records in {time.time() - t0:.1f}s")
    print(results["match_status"].value_counts().to_string())

    ground_truth = build_ground_truth(fi_raw, bl_raw)
    metrics = score(results, ground_truth)
    print(f"validation (against {len(ground_truth):,} held-out ground-truth pairs): {metrics}")

    out_path = "data/processed/match_results.csv"
    results.to_csv(out_path, index=False)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=None,
                         help="run on N inspection records instead of the full file")
    parser.add_argument("--include-facility-types", action="store_true",
                         help="keep schools/daycares/hospitals/long-term care in the match run "
                              "(excluded by default -- see README Results section)")
    args = parser.parse_args()
    main(args.sample, args.include_facility_types)
