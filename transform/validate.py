"""
Validate the fuzzy matcher against a held-out ground truth.

The matcher never sees `License #` / `LICENSE NUMBER` -- it only sees
normalized name + address, the way it would have to work in a world
where no shared ID existed. This script uses the ~72% of records that
DO share a license number purely to grade the matcher after the fact,
by checking whether the account/site it picked actually corresponds to
the correct license.

This is the difference between "I wrote a fuzzy matcher" and "I wrote a
fuzzy matcher and measured that it's 96% precise at 90% recall" --
the second is a claim you can defend in an interview.
"""

import pandas as pd


def build_ground_truth(fi_raw: pd.DataFrame, bl_raw: pd.DataFrame) -> pd.DataFrame:
    """Map Inspection ID -> correct (ACCOUNT NUMBER, SITE NUMBER) using the
    shared License # field, for records where that ID exists on both sides.
    """
    bl_lookup = (
        bl_raw[["LICENSE NUMBER", "ACCOUNT NUMBER", "SITE NUMBER"]]
        .dropna(subset=["LICENSE NUMBER"])
        .drop_duplicates(subset=["LICENSE NUMBER"], keep="last")
        .rename(columns={"LICENSE NUMBER": "License #"})
    )
    truth = fi_raw[["Inspection ID", "License #"]].dropna(subset=["License #"])
    truth = truth[truth["License #"] != "0"]
    truth = truth.merge(bl_lookup, on="License #", how="inner")
    return truth[["Inspection ID", "ACCOUNT NUMBER", "SITE NUMBER"]]


def score(match_results: pd.DataFrame, ground_truth: pd.DataFrame) -> dict:
    evaluable = match_results.merge(
        ground_truth, left_on="inspection_id", right_on="Inspection ID",
        suffixes=("", "_true"),
    )

    confident = evaluable[evaluable["match_status"] == "matched"]
    correct = confident[
        (confident["account_number"] == confident["ACCOUNT NUMBER"])
        & (confident["site_number"] == confident["SITE NUMBER"])
    ]

    precision = len(correct) / len(confident) if len(confident) else 0.0
    recall = len(correct) / len(evaluable) if len(evaluable) else 0.0

    return {
        "evaluable_records": len(evaluable),
        "confident_matches": len(confident),
        "correct_matches": len(correct),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
    }


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from extract.sources import load_business_licenses, load_food_inspections
    from transform.clean import clean_business_licenses, clean_food_inspections
    from transform.match import match_records

    bl_raw, fi_raw = load_business_licenses(), load_food_inspections()
    bl_clean, fi_clean = clean_business_licenses(bl_raw), clean_food_inspections(fi_raw)

    ground_truth = build_ground_truth(fi_raw, bl_raw)
    print(f"ground truth pairs available for validation: {len(ground_truth):,}")

    results = match_records(fi_clean, bl_clean)
    metrics = score(results, ground_truth)
    print(metrics)
