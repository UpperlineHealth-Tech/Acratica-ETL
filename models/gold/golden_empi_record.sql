{{ config(
    materialized='table',
    alias= this.name ~ var('table_suffix', '')
) }}

WITH RankedRecords AS (
    SELECT
        -- Identifying Fields
        EMPI_ID,
        SOURCE_SYSTEM,
        LOAD_DATE,
        
        -- Data Fields
        FIRST_NAME,
        LAST_NAME,
        DOB,
        SEX,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        CITY,
        STATE,
        ZIP_5,
        PHONE,
        EMAIL,
        DEATH_DATE,
        SSN,
        
        -- ID Fields (UPDATED to include the ID type columns)
        SOURCE_SYSTEM_ID,
        SOURCE_SYSTEM_ID_TYPE,
        SOURCE_SYSTEM_ID_2,
        SOURCE_SYSTEM_ID_2_TYPE,
        
        -- --- 1. Define Source Priority Ranks ---
        -- A. CMS BAR Priority (Used for Name, DOB, Gender, MBI, Address, DoD)
        CASE
            WHEN SOURCE_SYSTEM = 'BAR' THEN 1
            WHEN SOURCE_SYSTEM = 'ATHENA' THEN 2
            ELSE 99
        END AS cms_bar_priority_rank,
        
        -- B. Athena EMR Priority (Used for Phone, Email, Athena_Enterprise_ID)
        CASE
            WHEN SOURCE_SYSTEM = 'ATHENA' THEN 1
            WHEN SOURCE_SYSTEM = 'BAR' THEN 2
            ELSE 99
        END AS athena_emr_priority_rank,
        
        -- --- 2. Field-Specific Survivorship Ranking (Lower Rank = Winner) ---
        
        -- MBI 
        -- Rule: Completeness -> Recency -> Source Priority (BAR)
        CASE WHEN SOURCE_SYSTEM_ID_TYPE = 'MBI' AND SOURCE_SYSTEM_ID IS NOT NULL AND TRIM(SOURCE_SYSTEM_ID) != ''
                    THEN (
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                -- 1. Completeness: Prioritize records where the ID is explicitly MBI and is present
                CASE
                    WHEN SOURCE_SYSTEM_ID_TYPE = 'MBI' AND SOURCE_SYSTEM_ID IS NOT NULL AND TRIM(SOURCE_SYSTEM_ID) != ''
                    THEN 0 ELSE 1
                END,
                LOAD_DATE DESC, -- 2. Recency
                cms_bar_priority_rank -- 3. Source Priority (CMS BAR)
        ) ) ELSE 0 END AS mbi_rank,

        -- Athena_Enterprise_ID 
        -- Rule: Presence + Recency + Source Priority (Athena)
        CASE WHEN SOURCE_SYSTEM_ID_2_TYPE = 'ENTERPRISEID' AND SOURCE_SYSTEM_ID_2 IS NOT NULL AND TRIM(SOURCE_SYSTEM_ID_2) != ''
        THEN (
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                -- 1. Completeness: Prioritize records where the ID is EnterpriseID and is present
                CASE
                    WHEN SOURCE_SYSTEM_ID_2_TYPE = 'ENTERPRISEID' AND SOURCE_SYSTEM_ID_2 IS NOT NULL AND TRIM(SOURCE_SYSTEM_ID_2) != ''
                    THEN 0 ELSE 1
                END,
                LOAD_DATE DESC, -- 2. Recency
                athena_emr_priority_rank -- 3. Source Priority (Athena EMR)
        ) ) ELSE 0 END AS athena_id_rank,

        -- First Name
        -- Rule: Completeness (Length > 2, to avoid initials) -> Recency -> Source Priority (BAR)
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                CASE WHEN LENGTH(TRIM(FIRST_NAME)) > 2 THEN 0 ELSE 1 END, -- Completeness Check
                LOAD_DATE DESC, -- Recency
                cms_bar_priority_rank -- Source Priority (CMS BAR)
        ) AS first_name_rank,

        -- Last Name
        -- Rule: Completeness (Length > 2, to avoid initials) -> Recency -> Source Priority (BAR)
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                CASE WHEN LENGTH(TRIM(LAST_NAME)) > 2 THEN 0 ELSE 1 END, -- Completeness Check
                LOAD_DATE DESC, -- Recency
                cms_bar_priority_rank -- Source Priority (CMS BAR)
        ) AS last_name_rank,

        -- DOB / Gender (High-Confidence Fields)
        -- Rule: Source Priority (BAR) + Recency (Consistency check is a post-process manual validation)
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                cms_bar_priority_rank, -- Source Priority (CMS BAR is Gold Standard)
                LOAD_DATE DESC -- Recency (Tie-breaker)
        ) AS high_confidence_rank,

        -- Current Address (all components)
        -- Rule: Completeness (Addr1, City, ZIP5 present) -> Recency -> Source Priority (BAR)
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                CASE
                    -- Strong Address Completeness Check
                    WHEN ADDRESS_LINE_1 IS NOT NULL AND TRIM(ADDRESS_LINE_1) != '' AND
                         CITY IS NOT NULL AND TRIM(CITY) != '' AND
                         ZIP_5 IS NOT NULL AND TRIM(ZIP_5) != ''
                    THEN 0 ELSE 1
                END,
                LOAD_DATE DESC, -- Recency (Critical for outreach)
                cms_bar_priority_rank -- Source Priority (CMS BAR)
        ) AS address_rank,

        -- Phone Number
        -- Rule: Completeness (Valid 10-digit) -> Recency -> Source Priority (Athena)
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                -- Completeness: Check for 10 characters (assuming clean data or cleaned PHONE field)
                CASE WHEN LENGTH(REGEXP_REPLACE(PHONE, '[^0-9]', '')) = 10 THEN 0 ELSE 1 END,
                LOAD_DATE DESC, -- Recency
                athena_emr_priority_rank -- Source Priority (Athena EMR is Primary)
        ) AS phone_rank,

        -- Email
        -- Rule: Recency -> Source Priority (Athena)
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                CASE WHEN EMAIL IS NOT NULL AND TRIM(EMAIL) != '' THEN 0 ELSE 1 END, -- Completeness (Email exists)
                LOAD_DATE DESC, -- Recency
                athena_emr_priority_rank -- Source Priority (Athena EMR is Primary)
        ) AS email_rank,

        -- Date of Death
        -- Rule: Completeness (Presence) -> Source Priority (BAR)
        ROW_NUMBER() OVER (
            PARTITION BY EMPI_ID
            ORDER BY
                CASE WHEN DEATH_DATE IS NOT NULL THEN 0 ELSE 1 END, -- Presence
                cms_bar_priority_rank, -- Source Priority (CMS BAR)
                LOAD_DATE DESC -- Recency (Tie-breaker)
        ) AS death_date_rank

    FROM
       (SELECT empi_id,i.* from {{ref('silver_empi_input')}} i
JOIN {{ref('empi_crosswalk_gold')}} x ON x.source_system_id = i.source_system_id) x
)
-- --- 3. Final Aggregation (Selecting the Winning Value for each field) ---
SELECT
    EMPI_ID, -- System Generated/EMPI ID
    
    -- MBI (Now correctly pulling from SOURCE_SYSTEM_ID based on mbi_rank)
    MAX(CASE WHEN mbi_rank = 1 THEN SOURCE_SYSTEM_ID END) AS MBI,
    
    -- Athena Enterprise ID (Now correctly pulling from SOURCE_SYSTEM_ID_2 based on athena_id_rank)
    MAX(CASE WHEN athena_id_rank = 1 THEN SOURCE_SYSTEM_ID_2 END) AS ATHENA_ENTERPRISE_ID,
    
    -- First Name
    MAX(CASE WHEN first_name_rank = 1 THEN FIRST_NAME END) AS FIRST_NAME,
    
    -- Last Name
    MAX(CASE WHEN last_name_rank = 1 THEN LAST_NAME END) AS LAST_NAME,
    
    -- DOB (Source Priority Winner)
    MAX(CASE WHEN high_confidence_rank = 1 THEN DOB END) AS DOB,
    
    -- Gender (Source Priority Winner)
    MAX(CASE WHEN high_confidence_rank = 1 THEN SEX END) AS GENDER,
    
    -- Address (Selected from the highest-ranked address record)
    MAX(CASE WHEN address_rank = 1 THEN ADDRESS_LINE_1 END) AS CURRENT_ADDRESS_LINE_1,
    MAX(CASE WHEN address_rank = 1 THEN ADDRESS_LINE_2 END) AS CURRENT_ADDRESS_LINE_2,
    MAX(CASE WHEN address_rank = 1 THEN CITY END) AS CURRENT_CITY,
    MAX(CASE WHEN address_rank = 1 THEN STATE END) AS CURRENT_STATE,
    MAX(CASE WHEN address_rank = 1 THEN ZIP_5 END) AS CURRENT_ZIP_5,

    -- Phone Number
    MAX(CASE WHEN phone_rank = 1 THEN PHONE END) AS PHONE_NUMBER,
    
    -- Email
    MAX(CASE WHEN email_rank = 1 THEN EMAIL END) AS EMAIL,
    
    -- Date of Death
    MAX(CASE WHEN death_date_rank = 1 THEN DEATH_DATE END) AS DATE_OF_DEATH,

    -- Capture the time the Golden Record was calculated
    CURRENT_TIMESTAMP()::timestamp_ntz AS GOLDEN_RECORD_LAST_UPDATED

FROM
    RankedRecords
GROUP BY
    EMPI_ID
