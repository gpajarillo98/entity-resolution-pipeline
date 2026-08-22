# Business Entity Resolution Pipeline

Fuzzy-matches Chicago Food Inspection records against Chicago Business
License records using only business name and address text — no shared ID
required. Built to demonstrate the same entity-resolution/reconciliation
pattern used in production data pipelines, end to end: extract, clean,
match, validate, and (in later phases) orchestrate and warehouse.

## Why this problem

Two City of Chicago open datasets describe overlapping sets of
restaurants, but only ~72% of records share a reliable `License #`
between them — the rest have no common ID at all, and even where the ID
matches, the name and address text differs (DBA name vs. legal name,
inconsistent address formatting, truncated fields). That's a realistic
entity-resolution problem, not a toy one.

## Results

Validated against a held-out set of ~27,500 record pairs known to be
correct via the shared `License #` field (which the matcher itself never
sees — it matches on name + address alone). Four iterations, in order:

| Metric | v1: token_sort_ratio | v2: WRatio | v3: word-set containment (current) |
|---|---|---|---|
| Precision | 97.45% | 96.41% | **97.03%** |
| Recall | 91.06% | 92.82% | **92.27%** |
| Review queue | 6.4% of records | 14.0% | **5.7%** |

*(v3 measured on the full 13,275-record dataset with facility-type
exclusion applied — same run condition as v1/v2 for a fair comparison)*

**v2 (WRatio) was a false start, kept here in the history because the
failure mode is worth understanding.** WRatio's partial-ratio component
matches on *any* shared substring, not whole shared words. It correctly
fixed cases like `CAD RESTAURANT GROUP` vs `CAD RESTAURANT GROUP DBS
JERSEY MIKE'S` (real sub-brand suffix), but it also matched `BURGER KING`
to `47TH KING DRIVE CURRENCY EXCHANGE` (both contain "KING") and `ETTA`
to `NETTARE` (pure character coincidence: N-**ETTA**-RE). On the full
15,224-record dataset, 1,104 of 1,628 review-bucket rows landed on the
exact same score (85.5) — a strong tell that the boost was firing on
noise, not signal.

**v3 replaces that with a deterministic rule:** boost name similarity
only when every *word* in the shorter name is literally present in the
longer name (not just an overlapping character fragment). This still
catches the sub-brand-suffix pattern it was meant for, but rejects
single-shared-word coincidences like "KING" or "CHI" — see
`name_similarity()` in `transform/match.py` for the full rationale and
worked examples.

Excluding institutional facility types (schools, daycares, hospitals,
long-term care) from Food Inspections before matching is **on by
default** — it was a clear, low-risk win found in an earlier tuning pass
(smaller review queue, no cost to precision/recall). Pass
`--include-facility-types` to opt back in.

## Architecture

```
Chicago Business Licenses ─┐
                            ├──> clean/normalize ──> fuzzy match (blocked by ZIP) ──> validate ──> results
Chicago Food Inspections  ─┘
```

- **extract/sources.py** — loads both sources. Currently reads local CSV
  exports; designed to swap in live Socrata API calls without touching
  downstream code.
- **transform/clean.py** — normalizes names (strips legal suffixes,
  punctuation) and addresses (standardizes street/directional
  abbreviations), and collapses each source to one row per business.
- **transform/match.py** — fuzzy-matches on a composite score (60% name
  similarity, 40% address similarity), blocked by ZIP code to keep the
  comparison space tractable (~36k × ~44k records is >1B unblocked
  comparisons; blocking cuts each candidate's search space to a few
  hundred).
- **transform/validate.py** — grades the matcher against the held-out
  `License #` ground truth. This is what turns "I wrote a matcher" into
  a measured, defensible claim.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# place source CSVs in data/raw/ (see extract/sources.py for filenames,
# or pull fresh from data.cityofchicago.org)

python run_pipeline.py --sample 1000   # fast iteration
python run_pipeline.py                 # full run
```

## Performance note

This was built and validated in a sandboxed environment without internet
access, so `rapidfuzz` couldn't be installed there — `transform/match.py`
falls back to a pure-Python `difflib` implementation automatically when
`rapidfuzz` isn't available (see the try/except import at the top of that
file). The results above were validated on a sample using that fallback.
With `rapidfuzz` installed (recommended for real use — it's C-accelerated
and 10-100x faster), the full 15k × 43k matching run should complete in
well under a minute.

## Running with Airflow

The DAG lives in `dags/entity_resolution_dag.py` and wraps the same
extract -> clean -> match -> validate flow as `run_pipeline.py`, split
into four tasks so each stage gets its own retries, logs, and pass/fail
status instead of living inside one script. It also adds a data-quality
gate: the `validate_results` task **fails the DAG run** if precision
drops below 90%, instead of silently shipping a worse matcher downstream.

Airflow pins a lot of its own dependency versions, so install it into a
**separate virtual environment** from the one used for `run_pipeline.py`
above — mixing them is a reliable way to hit a resolver conflict.

```bash
# from the project root, in a NEW terminal/venv (not the one from Setup)
python -m venv .venv-airflow
source .venv-airflow/bin/activate        # .venv-airflow\Scripts\activate on Windows

export AIRFLOW_HOME=~/airflow            # or wherever you'd like Airflow's own files to live
AIRFLOW_VERSION=3.1.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# this DAG's tasks import pandas/rapidfuzz directly, so they need to be
# installed into THIS SAME venv (Airflow runs task code in its own
# process, not the project's other venv)
pip install pandas rapidfuzz

# point Airflow at this project's dags/ folder instead of the default
# ~/airflow/dags -- easiest way is editing dags_folder in
# $AIRFLOW_HOME/airflow.cfg after the first run generates it, or symlink:
mkdir -p "$AIRFLOW_HOME"
ln -s "$(pwd)/dags" "$AIRFLOW_HOME/dags"

airflow standalone
```

`airflow standalone` initializes the metadata database, creates an admin
user, and starts the scheduler + webserver — it prints the generated
admin password to the terminal. Open `http://localhost:8080`, log in,
find `entity_resolution_pipeline` in the DAG list, and trigger it
manually (the ▶ button) for a first run rather than waiting for the
`@weekly` schedule.

Watch each task go green in the UI — that's the payoff of splitting this
into tasks instead of one script: if `run_matching` fails, you can see
exactly that, re-run just that task, and the two upstream `clean` tasks
don't re-run unnecessarily.

## Roadmap

- [x] Extract + clean both sources
- [x] Fuzzy matching with ZIP blocking
- [x] Validation against held-out ground truth
- [x] Tune matcher based on manual review of the `review`/`unmatched`
      buckets (word-set containment for sub-brand names, facility-type
      filter defaulted on) — see Results section for the full v1->v3
      history, including the WRatio false start
- [x] Confirm the fix holds at full scale, not just on a sample
- [x] Orchestrate with Airflow (scheduled DAG, retries, logging, a
      precision data-quality gate) — see "Running with Airflow" above
- [ ] Model matched output into a star schema with dbt
      (`dim_business`, `dim_location`, `fact_license_status`)
- [ ] Load into Snowflake
- [ ] Lightweight dashboard on match rate / review queue
