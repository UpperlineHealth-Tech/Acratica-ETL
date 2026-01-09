{{ config(
    materialized='table',
    alias= this.name ~ var('table_suffix', '')
) }}


select 
    empi_id
    , source_id
    , source_id2
    , program_type
    , program_type_2
    , status_type
    , status_value
    , custom_field_operation
    , effective_from_date
    , username
    , Change_Timestamp
    , case
        when row_number() over (
            partition by empi_id, source_id, source_id2, program_type, program_type_2, status_type
            order by effective_from_date desc, change_timestamp desc) = 1
            then TRUE
        else FALSE
    end as Is_Currently_Valid
    , source_system
from {{ ref('silver_status_updates_all')}}