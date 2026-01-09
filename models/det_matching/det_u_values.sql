{{ config(
    materialized='table',
    tags=['full_refresh_only', 'recal'],
    alias= this.name ~ var('table_suffix', '')
) }}

with a_rows as (
    SELECT *
    FROM {{ ref('silver_empi_input') }}
    SAMPLE (5E3 ROWS)
)

, b_rows as (
    SELECT *
    FROM {{ ref('silver_empi_input') }}
    SAMPLE (5E3 ROWS)
)

, bucketed as (
SELECT {{ field_levels() }}
FROM a_rows a
JOIN b_rows b
ON a.source_system_id < b.source_system_id
)

, tot as (
select count(first_name_l) as first_name_ct
, count(last_name_l) as last_name_ct
, count(ssid2_l) as ssid2_ct
, count(phone_l) as phone_ct
, count(email_l) as email_ct
, count(ssn_l) as ssn_ct
, count(dob_l) as dob_ct
, count(sex_l) as sex_ct
, count(zip_l) as zip_ct
, count(death_date_l) as death_date_ct
, count(full_address_l) as full_address_ct
from bucketed
)

(
{{ field_level_counts('u') }}
)
order by field, u_value desc