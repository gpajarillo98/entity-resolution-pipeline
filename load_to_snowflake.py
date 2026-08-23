"""
Load the pipeline's cleaned CSVs into Snowflake as raw tables, landing them
in a schema named "raw" -- the same role `dbt seed` plays for local DuckDB
development. dbt's staging models read from source('raw', <table>), so
they work unchanged regardless of which of these populated the schema.

Authenticates via KEY-PAIR authentication, not a password -- Snowflake now
requires either MFA or key-pair auth, and MFA can't answer a prompt from an
unattended script. Key-pair is also the standard, correct approach for any
automated pipeline in production, not just a workaround for this project.
See README "Loading into Snowflake" for how to generate and register a key.

Requires these environment variables (never hardcode credentials):
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PRIVATE_KEY_PATH        path to your rsa_key.p8 file
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE                optional, defaults to ENTITY_RESOLUTION
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  optional, only if your key is encrypted

Usage:
    python run_pipeline.py          # regenerates data/processed/*.csv first
    python load_to_snowflake.py     # then load them into Snowflake
"""

import os
import sys

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE",
]

# table name -> source CSV. Uppercase because Snowflake stores unquoted
# identifiers uppercase by default -- dbt's source() reference is
# case-insensitive against this by default too, so no special handling
# needed on the dbt side.
TABLES = {
    "BUSINESS_LICENSES_CLEAN": "data/processed/business_licenses_clean.csv",
    "FOOD_INSPECTIONS_CLEAN": "data/processed/food_inspections_clean.csv",
    "MATCH_RESULTS": "data/processed/match_results.csv",
}


def load_private_key_bytes(path: str, passphrase: str | None) -> bytes:
    """Read a PEM private key file and return it in the DER-encoded PKCS8
    bytes format snowflake-connector-python's `private_key` parameter
    expects (not the raw PEM text)."""
    with open(path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode() if passphrase else None,
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def main():
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        print("Set these before running -- see README 'Loading into Snowflake'.")
        sys.exit(1)

    database = os.environ.get("SNOWFLAKE_DATABASE", "ENTITY_RESOLUTION")
    private_key_bytes = load_private_key_bytes(
        os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
    )

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_bytes,
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
    )

    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")
        cur.execute("USE SCHEMA RAW")

        for table_name, csv_path in TABLES.items():
            if not os.path.exists(csv_path):
                print(f"Missing {csv_path} -- run `python run_pipeline.py` first.")
                sys.exit(1)

            # dtype=str everywhere, same convention as the rest of this
            # pipeline -- protects things like ZIP codes from silently
            # losing a leading zero to numeric type inference.
            df = pd.read_csv(csv_path, dtype=str)

            success, num_chunks, num_rows, _ = write_pandas(
                conn, df, table_name,
                auto_create_table=True,
                overwrite=True,  # full refresh each run, not incremental -- see README note
            )
            status = "ok" if success else "FAILED"
            print(f"loaded {num_rows:,} rows -> RAW.{table_name} ({status})")

        cur.close()
    finally:
        conn.close()

    print("done. Run `dbt run --target prod` next to build the star schema on top of this.")


if __name__ == "__main__":
    main()
