-- Staging: business licenses.
--
-- Reads from source('raw', 'business_licenses_clean') -- landed either by
-- `dbt seed` (local DuckDB dev) or load_to_snowflake.py (Snowflake prod).
-- Both paths land data in a schema literally named "raw" with this exact
-- table name (see macros/generate_schema_name.sql), so this model needs
-- zero changes to run against either target.
--
-- NOTE on quoting AND aliasing: every column below gets an explicit
-- unquoted `as alias`, even columns that already had "the right" name
-- (like ADDRESS -> address_raw). This isn't just style -- it's what
-- makes every downstream reference (dim_business.sql, dim_location.sql,
-- fact_license_status.sql) safe on Snowflake. Selecting a quoted source
-- column WITHOUT an alias carries its exact-case quoted identity into
-- this view; selecting it WITH an unquoted alias normalizes it, so
-- everything downstream can reference it unquoted and get Snowflake's
-- normal case-folding behavior consistently. The original bug here:
-- name_norm/address_norm/zip had no alias, so they kept the lowercase
-- quoted identity write_pandas gave them -- fine on DuckDB (case-
-- insensitive by default), broken on Snowflake (unquoted `name_norm`
-- elsewhere got folded to a `NAME_NORM` that didn't actually exist).

with source as (
    select * from {{ source('raw', 'business_licenses_clean') }}
)

select
    "ACCOUNT NUMBER"          as account_number,
    "SITE NUMBER"             as site_number,
    "LICENSE NUMBER"          as license_number,
    "LEGAL NAME"              as legal_name,
    "DOING BUSINESS AS NAME"  as dba_name,
    "name_norm"               as name_norm,
    "legal_name_norm"         as legal_name_norm,
    "ADDRESS"                 as address_raw,
    "address_norm"            as address_norm,
    "zip"                     as zip,
    "LICENSE DESCRIPTION"     as license_description,
    "LICENSE STATUS"          as license_status,
    cast("DATE ISSUED" as date) as date_issued
from source
