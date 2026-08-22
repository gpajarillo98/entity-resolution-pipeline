-- Staging: fuzzy-match output from transform/match.py. Columns already
-- come out snake_case from the Python pipeline, so this is close to a
-- pass-through -- kept as its own staging model anyway (rather than
-- referencing the seed directly from the mart) so every mart model has
-- a consistent, single-source-of-truth staging layer to build on, and so
-- the eventual source() swap (see stg_business_licenses.sql) is uniform
-- across all three inputs.

with source as (
    select * from {{ ref('match_results') }}
)

select
    inspection_id,
    fi_name,
    fi_address,
    account_number,
    site_number,
    bl_name,
    bl_address,
    cast(name_score as double)       as name_score,
    cast(address_score as double)    as address_score,
    cast(composite_score as double)  as composite_score,
    match_status
from source
