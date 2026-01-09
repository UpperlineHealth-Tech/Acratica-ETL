{{ config (
    materialized = 'table',
    alias= this.name ~ var('table_suffix', '')
)}}

{% set rels = [
    ref('dq_silver_bar_address_1'),
    ref('dq_silver_bar_address_2'),
    ref('dq_silver_bar_city'),
    ref('dq_silver_bar_state'),
    ref('dq_silver_bar_birth_year'),
    ref('dq_silver_bar_first_name'),
    ref('dq_silver_bar_last_name'),
    ref('dq_silver_bar_death_date'),
    ref('dq_silver_bar_dob'),
    ref('dq_silver_bar_ssid'),
    ref('dq_silver_bar_prev_ssid'),
    ref('dq_silver_bar_zip_4'),
    ref('dq_silver_bar_zip_5'),
    ref('dq_silver_cclf8_address_1'),
    ref('dq_silver_cclf8_address_2'),
    ref('dq_silver_cclf8_city'),
    ref('dq_silver_cclf8_state'),
    ref('dq_silver_cclf8_birth_year'),
    ref('dq_silver_cclf8_first_name'),
    ref('dq_silver_cclf8_last_name'),
    ref('dq_silver_cclf8_death_date'),
    ref('dq_silver_cclf8_dob'),
    ref('dq_silver_cclf8_ssid'),
    ref('dq_silver_cclf8_prev_ssid'),
    ref('dq_silver_cclf8_zip_4'),
    ref('dq_silver_cclf8_zip_5'),
    ref('dq_silver_patient_address_1'),
    ref('dq_silver_patient_address_2'),
    ref('dq_silver_patient_city'),
    ref('dq_silver_patient_state'),
    ref('dq_silver_patient_birth_year'),
    ref('dq_silver_patient_first_name'),
    ref('dq_silver_patient_last_name'),
    ref('dq_silver_patient_death_date'),
    ref('dq_silver_patient_dob'),
    ref('dq_silver_patient_ssid'),
    ref('dq_silver_patient_ssid_2'),
    ref('dq_silver_patient_zip_4'),
    ref('dq_silver_patient_zip_5'),
    ref('dq_silver_patient_phone'),
    ref('dq_silver_patient_email'),
    ref('dq_silver_patient_ssn')
] %}

{%- for r in rels %}
select * from {{ r }}
{%- if not loop.last %}
UNION ALL
{%- endif %}
{%- endfor %}