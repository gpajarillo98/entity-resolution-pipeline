-- Staging: food inspections. Reads from source('raw', 'food_inspections_clean')
-- -- see stg_business_licenses.sql for the full explanation of why every
-- column below gets an explicit unquoted alias, not just the ones with
-- "wrong" names -- it's what keeps every downstream reference safe on
-- Snowflake specifically (DuckDB never had this problem).

with source as (
    select * from {{ source('raw', 'food_inspections_clean') }}
)

select
    "Inspection ID"    as inspection_id,
    "License #"        as license_number,
    "DBA Name"         as dba_name,
    "AKA Name"         as aka_name,
    "name_norm"        as name_norm,
    "aka_name_norm"    as aka_name_norm,
    "Address"          as address_raw,
    "address_norm"     as address_norm,
    "zip"              as zip,
    "Facility Type"    as facility_type,
    "Results"          as inspection_result,
    cast("Inspection Date" as date) as inspection_date
from source
