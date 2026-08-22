-- Staging: business licenses.
--
-- Reads from a SEED right now (dbt/seeds/business_licenses_clean.csv) since
-- Snowflake loading isn't wired up yet. Once it is, the only change needed
-- here is swapping the ref() below for a source macro call pointing at
-- the raw Snowflake table -- everything downstream (dim_business,
-- fact_license_status) stays exactly the same, since they're built on
-- this staging model's output shape, not on the seed directly.
--
-- NOTE: don't put literal Jinja delimiters in comments in this file --
-- dbt renders Jinja across the whole file before the database ever sees
-- it as a SQL comment, so even an empty tag breaks compilation. Learned
-- this the hard way getting this exact comment block right.

with source as (
    select * from {{ ref('business_licenses_clean') }}
)

select
    "ACCOUNT NUMBER"          as account_number,
    "SITE NUMBER"             as site_number,
    "LICENSE NUMBER"          as license_number,
    "LEGAL NAME"              as legal_name,
    "DOING BUSINESS AS NAME"  as dba_name,
    name_norm,
    legal_name_norm,
    "ADDRESS"                 as address_raw,
    address_norm,
    zip,
    "LICENSE DESCRIPTION"     as license_description,
    "LICENSE STATUS"          as license_status,
    cast("DATE ISSUED" as date) as date_issued
from source
