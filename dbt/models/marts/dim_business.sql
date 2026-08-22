-- Dimension: one row per resolved business, straight from Business Licenses.
-- Grain = (account_number, site_number), confirmed unique in
-- transform/clean.py's clean_business_licenses() (renewals already
-- collapsed to the latest record per business before this model runs).

with businesses as (
    select * from {{ ref('stg_business_licenses') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['account_number', 'site_number']) }} as business_key,
    account_number,
    site_number,
    license_number,
    legal_name,
    dba_name,
    license_description,
    license_status,
    date_issued
from businesses
