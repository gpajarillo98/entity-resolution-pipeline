"""
Entity resolution: match food inspection records against business license
records using fuzzy name + address similarity.

Design mirrors a source-priority reconciliation pattern: Business Licenses
is treated as the reference table (it's the authoritative record of who's
legally operating), and Food Inspections records are reconciled against it.
A high-confidence match updates/confirms the existing business; anything
below threshold is flagged as unmatched -- in a full pipeline that's where
new-record insertion logic would go.

Uses rapidfuzz when available (fast, C-accelerated) and falls back to the
stdlib difflib so this runs anywhere -- rapidfuzz is the recommended
runtime dependency (see requirements.txt) and is what to install locally.
"""

from dataclasses import dataclass
import pandas as pd

try:
    from rapidfuzz import fuzz
    _BACKEND = "rapidfuzz"

    def _base_name_ratio(a: str, b: str) -> float:
        return fuzz.token_sort_ratio(a, b)

except ImportError:
    import difflib
    _BACKEND = "difflib"

    def _base_name_ratio(a: str, b: str) -> float:
        tokens_a, tokens_b = " ".join(sorted(a.split())), " ".join(sorted(b.split()))
        return difflib.SequenceMatcher(None, tokens_a, tokens_b).ratio() * 100


def name_similarity(a: str, b: str) -> float:
    """Base token-sort similarity, with a deterministic containment boost:
    if every WORD in the shorter name literally appears in the longer
    name (e.g. "CAD RESTAURANT GROUP" fully inside "CAD RESTAURANT GROUP
    DBS JERSEY MIKE'S"), treat it as strong evidence even though the
    extra words drag down plain token-sort scoring.

    This replaced an earlier version that used rapidfuzz's WRatio for
    this job. WRatio's partial-ratio component matches on *any* shared
    substring, not just whole shared words -- which caught real cases
    like the CAD Restaurant Group example, but also matched "BURGER KING"
    to "47TH KING DRIVE CURRENCY EXCHANGE" (both contain "KING") and
    "ETTA" to "NETTARE" (character coincidence: N-ETTA-RE). On the full
    dataset, 1,104 of 1,628 review-bucket rows landed on the exact same
    score (85.5) from this pattern -- a strong signal it was matching on
    noise, not signal. Whole-word-set containment is stricter and fixes
    both examples above while still catching the sub-brand-suffix cases
    it was meant for.
    """
    base = _base_name_ratio(a, b)
    words_a, words_b = set(a.split()), set(b.split())
    if words_a and words_b:
        shorter, longer = (words_a, words_b) if len(words_a) <= len(words_b) else (words_b, words_a)
        if shorter < longer and shorter.issubset(longer):
            base = max(base, 92.0)
    return base


if _BACKEND == "rapidfuzz":
    def address_similarity(a: str, b: str) -> float:
        return fuzz.ratio(a, b)
else:
    def address_similarity(a: str, b: str) -> float:
        return difflib.SequenceMatcher(None, a, b).ratio() * 100


# Weighting: name carries more signal than address here because Chicago
# addresses are fairly standardized already post-normalization, while
# names vary more (DBA vs legal name, punctuation, abbreviations).
NAME_WEIGHT = 0.6
ADDRESS_WEIGHT = 0.4

MATCH_THRESHOLD = 85       # composite score >= this -> confident match
REVIEW_THRESHOLD = 70      # composite score in [REVIEW, MATCH) -> needs manual review


@dataclass
class MatchResult:
    inspection_id: str
    fi_name: str
    fi_address: str
    account_number: str
    site_number: str
    bl_name: str
    bl_address: str
    name_score: float
    address_score: float
    composite_score: float
    match_status: str  # "matched" | "review" | "unmatched"


def composite_score(name_score: float, addr_score: float) -> float:
    return NAME_WEIGHT * name_score + ADDRESS_WEIGHT * addr_score


def classify(score: float) -> str:
    if score >= MATCH_THRESHOLD:
        return "matched"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "unmatched"


def match_records(fi_clean: pd.DataFrame, bl_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Blocking strategy: only compare records within the same ZIP code.
    Without blocking this is an O(n * m) comparison -- ~36k x ~44k here,
    over a billion comparisons. Blocking on zip cuts each candidate's
    search space to a few hundred records instead of tens of thousands,
    which is what makes this runnable without a distributed job.
    """
    bl_records = bl_clean.to_dict("records")
    bl_by_zip: dict[str, list[dict]] = {}
    for rec in bl_records:
        bl_by_zip.setdefault(rec["zip"], []).append(rec)

    results = []
    for fi_row in fi_clean.to_dict("records"):
        candidates = bl_by_zip.get(fi_row["zip"])

        if not candidates or not fi_row["name_norm"]:
            results.append(MatchResult(
                inspection_id=fi_row["Inspection ID"],
                fi_name=fi_row["name_norm"],
                fi_address=fi_row["address_norm"],
                account_number="", site_number="", bl_name="", bl_address="",
                name_score=0.0, address_score=0.0, composite_score=0.0,
                match_status="unmatched",
            ))
            continue

        best_score, best_n, best_a, best_rec = -1.0, 0.0, 0.0, None
        for bl_row in candidates:
            n_score = name_similarity(fi_row["name_norm"], bl_row["name_norm"])
            a_score = address_similarity(fi_row["address_norm"], bl_row["address_norm"])
            score = composite_score(n_score, a_score)
            if score > best_score:
                best_score, best_n, best_a, best_rec = score, n_score, a_score, bl_row

        results.append(MatchResult(
            inspection_id=fi_row["Inspection ID"],
            fi_name=fi_row["name_norm"],
            fi_address=fi_row["address_norm"],
            account_number=best_rec["ACCOUNT NUMBER"],
            site_number=best_rec["SITE NUMBER"],
            bl_name=best_rec["name_norm"],
            bl_address=best_rec["address_norm"],
            name_score=round(best_n, 1),
            address_score=round(best_a, 1),
            composite_score=round(best_score, 1),
            match_status=classify(best_score),
        ))

    return pd.DataFrame([r.__dict__ for r in results])


if __name__ == "__main__":
    print(f"fuzzy matching backend: {_BACKEND}")
