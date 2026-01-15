{{ config(
    materialized = 'incremental',
    unique_key = 'SOURCE_SYSTEM_ID',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    alias= this.name ~ var('table_suffix', '')
)}}

-- Template: CAREMGMT (non-Athena) -> normalized EMPI silver model
WITH src AS (
    SELECT
        'CAREMGMT' AS SOURCE_SYSTEM,
        UPPER(TRIM(MEMBER_ID)) AS SOURCE_SYSTEM_ID,
        'CAREMGMT_MEMBER_ID' AS SOURCE_SYSTEM_ID_TYPE,

        NULL AS PREV_SOURCE_SYSTEM_ID,
        NULL AS PREV_SOURCE_SYSTEM_ID_TYPE,

        NULL AS SOURCE_SYSTEM_ID_2,
        NULL AS SOURCE_SYSTEM_ID_2_TYPE,

        UPPER(REGEXP_REPLACE(TRIM(FIRST_NAME), '\\s+', ' ')) AS FIRST_NAME,
        UPPER(REGEXP_REPLACE(TRIM(LAST_NAME),  '\\s+', ' ')) AS LAST_NAME,
        TRY_TO_DATE(DOB) AS DOB,
        TRY_TO_NUMBER(YEAR(DOB)) AS BIRTH_YEAR,
        UPPER(TRIM(SEX)) AS SEX,
        NULL AS RACE,

        UPPER(TRIM(ADDRESS_LINE_1)) AS ADDRESS_LINE_1,
        UPPER(TRIM(ADDRESS_LINE_2)) AS ADDRESS_LINE_2,
        UPPER(TRIM(ADDRESS_LINE_1)) || ' ' || UPPER(TRIM(ADDRESS_LINE_2)) AS FULL_ADDRESS,
        UPPER(TRIM(CITY)) AS CITY,
        UPPER(TRIM(STATE)) AS STATE,
        LEFT(REGEXP_REPLACE(ZIP, '[^0-9]', ''), 5) AS ZIP_5,
        SUBSTR(REGEXP_REPLACE(ZIP, '[^0-9]', ''), 6, 4) AS ZIP_4,

        REGEXP_REPLACE(PHONE, '[^0-9]', '') AS PHONE,
        LOWER(TRIM(EMAIL)) AS EMAIL,

        NULL AS DEATH_DATE,
        NULL AS SSN,

        COALESCE(LOAD_DATE, CURRENT_DATE()) AS LOAD_DATE,
        COALESCE(LAST_UPDATED, CURRENT_DATE()) AS LAST_UPDATED
    FROM {{ source('CAREMGMT_RAW', 'MEMBER_DEMOGRAPHICS') }}
    {% if is_incremental() %}
    WHERE COALESCE(LAST_UPDATED, CURRENT_DATE()) >
          (SELECT COALESCE(MAX(LAST_UPDATED), DATE '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT * FROM src
