{{ config(
    materialized='table',
    tags=['full_refresh_only', 'recal'],
    alias= this.name ~ var('table_suffix', '')
) }}

with d_ssn as ({{ d_ssn() }})

, d_name_dob_sex as ({{ d_name_dob_sex() }})

, unioned as (
SELECT id1, id2, '1' AS "label", deterministic_rule, '1' AS rule_num from d_ssn
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, '2' AS rule_num from d_name_dob_sex
)

, deduped as (
SELECT *
FROM unioned
QUALIFY row_number() OVER(PARTITION BY id1, id2 ORDER BY rule_num) = 1
)

, bucketed as (
SELECT deduped.deterministic_rule
, deduped."label"
, {{ field_levels() }}
FROM deduped 
JOIN {{ ref('silver_empi_input') }} a
ON deduped.id1 = a.SOURCE_SYSTEM_ID
JOIN {{ ref('silver_empi_input') }} b
ON deduped.id2 = b.SOURCE_SYSTEM_ID
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
{{ field_level_counts('m') }}
)
order by field, m_value desc