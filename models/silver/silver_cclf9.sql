{{config(
    materialized='incremental',
    unique_key='CURRENT_MBI',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    alias= this.name ~ var('table_suffix', '')
    )}}

WITH src AS (
SELECT *
FROM {{ source('CCLF9', 'CCLF_9_RAW') }}
WHERE CRNT_NUM != PRVS_NUM
{% if is_incremental() %}
AND TRY_TO_DATE(LOAD_DATE) > (SELECT COALESCE(MAX(LOAD_DATE), DATE '1900-01-01') FROM {{ this }})
{% endif %}
)
SELECT UPPER(TRIM(CRNT_NUM)) AS CURRENT_MBI
, UPPER(TRIM(PRVS_NUM)) AS PREVIOUS_MBI 
, PRVS_ID_EFCTV_DT AS PREVIOUS_MBI_EFFECTIVE_DATE
, PRVS_ID_OBSLT_DT AS PREVIOUS_MBI_OBSOLETE_DATE
, TRY_TO_DATE(LOAD_DATE) AS LOAD_DATE
, "SOURCE"
FROM src
QUALIFY ROW_NUMBER() OVER(PARTITION BY CURRENT_MBI ORDER BY LOAD_DATE DESC, PREVIOUS_MBI_EFFECTIVE_DATE DESC) = 1