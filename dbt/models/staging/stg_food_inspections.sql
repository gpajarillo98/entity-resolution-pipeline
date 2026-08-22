-- Staging: food inspections. Same swap note as stg_business_licenses.sql
-- applies here: ref('food_inspections_clean') -> source('raw', 'food_inspections')
-- once Snowflake loading is live.

with source as (
    select * from {{ ref('food_inspections_clean') }}
)

select
    "Inspection ID"    as inspection_id,
    "License #"        as license_number,
    "DBA Name"         as dba_name,
    "AKA Name"         as aka_name,
    name_norm,
    aka_name_norm,
    "Address"          as address_raw,
    address_norm,
    zip,
    "Facility Type"    as facility_type,
    "Results"          as inspection_result,
    cast("Inspection Date" as date) as inspection_date
from source
