-- Dimension: one row per unique normalized (address, zip), pulled from
-- BOTH sources and de-duplicated. This is deliberately not scoped to just
-- Business Licenses addresses -- fact_license_status needs a location_key
-- for every food inspection record, including the ones that never matched
-- a license, so a location coming only from the inspection side still
-- needs to resolve to a row here.
--
-- coalesce(zip, '') matters: 3 real food inspection records have an
-- address but a NULL zip in the source data (a genuine small gap in
-- Chicago's dataset, not a bug in this pipeline). Without the coalesce,
-- `a.zip = b.zip` evaluates to NULL (not true) whenever zip is NULL on
-- either side of the join in fact_license_status.sql -- even though this
-- CTE faithfully includes a NULL-zip row via UNION, the fact table's join
-- would never match it, silently leaving location_key null downstream.
-- Coalescing to '' here and in the join gives those 3 records a real,
-- consistent (if less specific) location instead.

with license_locations as (
    select distinct address_norm, coalesce(zip, '') as zip from {{ ref('stg_business_licenses') }}
),

inspection_locations as (
    select distinct address_norm, coalesce(zip, '') as zip from {{ ref('stg_food_inspections') }}
),

combined as (
    select * from license_locations
    union  -- UNION (not UNION ALL) is doing the de-duplication here
    select * from inspection_locations
)

select
    {{ dbt_utils.generate_surrogate_key(['address_norm', 'zip']) }} as location_key,
    address_norm,
    zip
from combined
