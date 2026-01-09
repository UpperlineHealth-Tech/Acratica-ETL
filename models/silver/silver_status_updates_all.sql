{{ config(
    materialized='table',
    alias= this.name ~ var('table_suffix', '')
) }}

SELECT
    x.empi_id,  -- Map to enterprise master patient index
    a.source_id,
    a.source_id2,
    a.program_type,
    a.program_type_2,
    a.status_type,
    a.status_value,
    a.custom_field_operation,
    a.effective_from_date,
    a.username,
    current_timestamp()::timestamp_ntz AS Change_Timestamp,
    a.source_system
FROM (
    -- Union all status updates from BAR and Athena
    SELECT * FROM {{ref('silver_status_updates_bar')}}
    UNION ALL
    SELECT * FROM {{ref('silver_status_updates_athena')}}
) a
JOIN {{ref('empi_crosswalk_gold')}} x
  ON x.source_system_id = a.source_id
-- Group by all output columns to prevent duplicate rows
GROUP BY
    x.empi_id,
    a.source_id,
    a.source_id2,
    a.program_type,
    a.program_type_2,
    a.status_type,
    a.status_value,
    a.custom_field_operation,
    a.effective_from_date,
    a.username,
    a.source_system
ORDER BY x.empi_id, a.effective_from_date ASC