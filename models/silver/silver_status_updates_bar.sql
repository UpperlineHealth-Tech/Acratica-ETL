{{ config(
    materialized='ephemeral',
    alias= this.name ~ var('table_suffix', '')
) }}

-- Alignment and VOLUNTARY_ALIGNMENT_TYPE
SELECT
    BENEFICIARY_MBI_ID AS source_id,
    NULL AS source_id2,
    'ACO_REACH' AS program_type,
    SOURCE AS program_type_2,
    'Alignment' AS status_type,
    CASE
        WHEN VOLUNTARY_ALIGNMENT_TYPE = 'Paper' THEN 'Aligned - VOLUNTARY'
        WHEN VOLUNTARY_ALIGNMENT_TYPE = 'Electronic' THEN 'Aligned - MEDICARE VOLUNTARY'
        WHEN VOLUNTARY_ALIGNMENT_TYPE = 'No' AND CLAIM_BASED_ALIGNMENT_INDICATOR = 'Y' THEN 'Aligned - Claims'
        ELSE 'Aligned - Other'
    END AS status_value,
    NULL AS custom_field_operation,
    BENEFICIARY_ALIGNMENT_EFFECTIVE_START_DATE AS effective_from_date,
    NULL AS username,  -- Added for consistency
    MAX(FILE_DATE) AS lastmodifieddatetime,    
    'BAR' AS source_system
FROM {{ source('BAR', 'ALIGNMENT_REPORT_RAW') }}
GROUP BY
    BENEFICIARY_MBI_ID,
    BENEFICIARY_ALIGNMENT_EFFECTIVE_START_DATE,
    SOURCE,
    VOLUNTARY_ALIGNMENT_TYPE,
    CLAIM_BASED_ALIGNMENT_INDICATOR

UNION ALL

-- Terminated alignments (including deceased)
SELECT
    BENEFICIARY_MBI_ID AS source_id,
    NULL AS source_id2,
    'ACO_REACH' AS program_type,
    SOURCE AS program_type_2,
    'Alignment' AS status_type,
    CASE
        WHEN (beneficiary_date_of_death IS NOT NULL AND beneficiary_date_of_death<BENEFICIARY_ALIGNMENT_EFFECTIVE_TERMINATION_DATE) THEN 'Terminated - DECEASED'
        ELSE 'Terminated - OTHER'
    END AS status_value,
    NULL AS custom_field_operation,
    BENEFICIARY_ALIGNMENT_EFFECTIVE_TERMINATION_DATE AS effective_from_date,
    NULL AS username,  -- Added for consistency
    MAX(FILE_DATE) AS lastmodifieddatetime,
    'BAR' AS source_system
FROM {{ source('BAR', 'ALIGNMENT_REPORT_RAW') }}
WHERE BENEFICIARY_ALIGNMENT_EFFECTIVE_TERMINATION_DATE IS NOT NULL
GROUP BY
    BENEFICIARY_MBI_ID,
    BENEFICIARY_ALIGNMENT_EFFECTIVE_TERMINATION_DATE,
    SOURCE, 
    CASE
        WHEN (beneficiary_date_of_death IS NOT NULL AND beneficiary_date_of_death<BENEFICIARY_ALIGNMENT_EFFECTIVE_TERMINATION_DATE) THEN 'Terminated - DECEASED'
        ELSE 'Terminated - OTHER'
    END