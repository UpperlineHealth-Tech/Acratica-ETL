{{ config(
    materialized = 'incremental',
    unique_key = 'SOURCE_SYSTEM_ID',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    alias= this.name ~ var('table_suffix', '')
)}}

WITH src AS (
SELECT 'CCLF8' AS SOURCE_SYSTEM
, UPPER(TRIM(BENE_MBI_ID)) AS SOURCE_SYSTEM_ID
, 'MBI' AS SOURCE_SYSTEM_ID_TYPE
, NULL AS SOURCE_SYSTEM_ID_2
, NULL AS SOURCE_SYSTEM_ID_2_TYPE
, UPPER(REGEXP_REPLACE(TRIM(BENE_1ST_NAME), '\s+', ' ')) AS FIRST_NAME
, UPPER(REGEXP_REPLACE(TRIM(BENE_LAST_NAME), '\s+', ' ')) AS LAST_NAME
, TRY_TO_DATE(BENE_DOB) AS DOB
, CASE
    WHEN TRIM(BENE_SEX_CD) = '1' THEN 'M'
    WHEN TRIM(BENE_SEX_CD) = '2' THEN 'F'
    ELSE 'U'
END AS SEX
, TRIM(BENE_RACE_CD) AS RACE_CODE
, UPPER(REGEXP_REPLACE(TRIM(BENE_LINE_1_ADR), '\s+', ' ')) AS ADDRESS_LINE_1
, NULLIF(UPPER(REGEXP_REPLACE(TRIM(BENE_LINE_2_ADR), '\s+', ' ')), '') AS ADDRESS_LINE_2
, UPPER(REGEXP_REPLACE(TRIM(GEO_ZIP_PLC_NAME), '\s+', ' ')) AS CITY
, UPPER(REGEXP_REPLACE(TRIM(GEO_USPS_STATE_CD), '\s+', ' ')) AS STATE
, TRIM(GEO_ZIP5_CD) AS ZIP_5
, LPAD(TRIM(GEO_ZIP4_CD), 4, '0') AS ZIP_4
, NULL AS PHONE
, NULL AS EMAIL
, TRY_TO_DATE(BENE_DEATH_DT) AS DEATH_DATE
, NULL AS SSN
, TRY_TO_DATE(LOAD_DATE) AS LOAD_DATE
FROM {{ source('CCLF8', 'CCLF_8_PROCESSED') }}
{% if is_incremental() %}
WHERE TRY_TO_DATE(LOAD_DATE) > (SELECT COALESCE(MAX(LOAD_DATE), DATE '1900-01-01') FROM {{ this }})
{% endif %}
)

, hashed_cte as (
select *
, {{ dbt_utils.generate_surrogate_key(['FIRST_NAME', 'LAST_NAME', 'DOB', 'SEX', 'ADDRESS_LINE_1', 'ZIP_5', 'DEATH_DATE']) }} AS DEMO_HASH
from src
)

, staged as (
   SELECT h.*
    {% if is_incremental() %}
    , CASE
        WHEN t.load_date IS NULL THEN h.load_date
        ELSE t.first_load_date
    END AS first_load_date 
    {% else %}
    , h.load_date AS first_load_date
    {% endif %} 
    , convert_timezone('UTC', current_timestamp())::timestamp_ntz AS last_updated
    FROM hashed_cte h
    {% if is_incremental() %}
    LEFT JOIN {{ this }} t
    ON h.source_system_id = t.source_system_id
    WHERE h.demo_hash <> t.demo_hash
    OR t.source_system_id IS NULL
    {% endif %}
)
SELECT SOURCE_SYSTEM
, SOURCE_SYSTEM_ID
, SOURCE_SYSTEM_ID_TYPE
, cclf9.PREVIOUS_MBI AS PREV_SOURCE_SYSTEM_ID
, 'MBI' AS PREV_SOURCE_SYSTEM_ID_TYPE
, SOURCE_SYSTEM_ID_2
, SOURCE_SYSTEM_ID_2_TYPE
, FIRST_NAME
, LAST_NAME
, LEFT(FIRST_NAME, 1) AS FIRST_INITIAL
, LEFT(LAST_NAME, 1) AS LAST_INITIAL
, SOUNDEX(FIRST_NAME) AS FIRST_NAME_SOUNDEX
, SOUNDEX(LAST_NAME) AS LAST_NAME_SOUNDEX
, DOB
, YEAR(DOB) AS BIRTH_YEAR
, SEX
, CASE
    WHEN RACE_CODE = '1' THEN 'WHITE'
    WHEN RACE_CODE = '2' THEN 'BLACK'
    WHEN RACE_CODE = '3' THEN 'OTHER'
    WHEN RACE_CODE = '4' THEN 'ASIAN'
    WHEN RACE_CODE = '5' THEN 'HISPANIC'
    WHEN RACE_CODE = '6' THEN 'NORTH AMERICAN NATIVE'
    ELSE 'UNKNOWN'
END AS RACE
, ADDRESS_LINE_1
, ADDRESS_LINE_2
, CONCAT(ADDRESS_LINE_1, COALESCE(CONCAT(' ', ADDRESS_LINE_2), '')) AS FULL_ADDRESS
, CITY
, STATE
, ZIP_5
, ZIP_4
, PHONE
, EMAIL
, DEATH_DATE
, SSN
, s.LOAD_DATE
, DEMO_HASH
, FIRST_LOAD_DATE
, LAST_UPDATED
FROM staged s
LEFT JOIN {{ ref('silver_cclf9') }} cclf9
ON s.SOURCE_SYSTEM_ID = cclf9.CURRENT_MBI