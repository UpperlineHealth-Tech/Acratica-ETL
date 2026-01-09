{{ config(
    materialized='table',
    tags=['full_refresh_only'],
    alias= this.name ~ var('table_suffix', '')
) }}

select *
from {{ ref('det_pairs') }}
where (deterministic_rule <> 'name_state' and total_log_lr > {{ var('clerical_log_lr_threshold') }} and total_log_lr <= {{ var('log_lr_threshold') }})
or (deterministic_rule = 'name_state' and total_log_lr > {{ var('clerical_log_lr_threshold') }} and total_log_lr <= {{ var('clerical_name_state_log_lr_threshold') }})
or deterministic_rule = 'name_dob_sex_exact_clerical'