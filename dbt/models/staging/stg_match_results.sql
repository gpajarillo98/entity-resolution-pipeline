-- Staging: fuzzy-match output from transform/match.py. Reads from
-- source('raw', 'match_results') -- same reasoning as the other two
-- staging models. Columns already come out snake_case from the Python
-- pipeline, so this is close to a pass-through -- kept as its own
-- staging model anyway so every mart model has a consistent staging
-- layer to build on.
--
-- Every column gets an explicit unquoted alias below -- see
-- stg_business_licenses.sql for the full explanation. It's not enough to
-- just quote the source reference; selecting a quoted column WITHOUT an
-- alias carries its exact-case identity into this view too, which breaks
-- the same way one layer further downstream.

with source as (
    select * from {{ source('raw', 'match_results') }}
)

select
    "inspection_id"     as inspection_id,
    "fi_name"           as fi_name,
    "fi_address"        as fi_address,
    "account_number"    as account_number,
    "site_number"       as site_number,
    "bl_name"           as bl_name,
    "bl_address"        as bl_address,
    cast("name_score" as double)       as name_score,
    cast("address_score" as double)    as address_score,
    cast("composite_score" as double)  as composite_score,
    "match_status"      as match_status
from source
