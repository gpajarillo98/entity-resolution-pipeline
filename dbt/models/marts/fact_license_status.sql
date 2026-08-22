-- Fact: one row per food inspection record that went through matching.
-- Grain = inspection_id (confirmed not_null + unique in
-- _staging__schema.yml, and confirmed no fan-out from either join below
-- against real data before this SQL was written).
--
-- business_key is intentionally populated for review/unmatched rows too,
-- not just confident matches -- it points at whatever the matcher's best
-- candidate was, even when that candidate didn't clear the threshold.
-- match_status is the field to filter/trust on; business_key here is kept
-- as a full record of what the matcher considered, which is useful for
-- anyone triaging the review queue. It's only nulled out for the rare
-- case where no candidate existed at all (empty account_number/site_number
-- -- see match_records() in transform/match.py for when that happens).

with match_results as (
    select * from {{ ref('stg_match_results') }}
),

inspections as (
    select * from {{ ref('stg_food_inspections') }}
),

businesses as (
    select * from {{ ref('stg_business_licenses') }}
),

locations as (
    select * from {{ ref('dim_location') }}
),

keyed as (
    select
        *,
        case
            when account_number != '' and site_number != ''
                then {{ dbt_utils.generate_surrogate_key(['account_number', 'site_number']) }}
            else null
        end as business_key
    from match_results
)

select
    keyed.inspection_id,
    keyed.business_key,
    inspection_locations.location_key,
    keyed.match_status,
    keyed.name_score,
    keyed.address_score,
    keyed.composite_score,
    inspections.facility_type,
    inspections.inspection_result,
    inspections.inspection_date,
    businesses.license_status,
    businesses.license_description

from keyed
left join inspections
    on keyed.inspection_id = inspections.inspection_id
left join locations as inspection_locations
    on inspections.address_norm = inspection_locations.address_norm
    and coalesce(inspections.zip, '') = inspection_locations.zip
left join businesses
    on keyed.account_number = businesses.account_number
    and keyed.site_number = businesses.site_number
