-- QA checks for ref('silver_status_updates_all')
-- Run this in the same environment after populating {{ ref('silver_status_updates_all') }}
-- These checks are read-only and return counts and small samples for inspection.

-- 1) Total rows in unified SILVER table
SELECT 'total_rows' AS check_name, COUNT(*) AS check_value
FROM {{ ref('silver_status_updates_all') }};

-- 2) Distinct EMPI count
SELECT 'distinct_empi_count' AS check_name, COUNT(DISTINCT empi_id) AS check_value
FROM {{ ref('silver_status_updates_all') }};

-- 3) Distinct source_id count
SELECT 'distinct_source_id_count' AS check_name, COUNT(DISTINCT source_id) AS check_value
FROM {{ ref('silver_status_updates_all') }};

-- 4) Rows by source_system
SELECT source_system, COUNT(*) AS row_count
FROM {{ ref('silver_status_updates_all') }}
GROUP BY source_system
ORDER BY row_count DESC;

-- 5) Rows by program_type
SELECT COALESCE(program_type,'<NULL>') AS program_type, COUNT(*) AS row_count
FROM {{ ref('silver_status_updates_all') }}
GROUP BY COALESCE(program_type,'<NULL>')
ORDER BY row_count DESC;

-- 6) Null/empty EMPI IDs
SELECT 'null_empi_count' AS check_name, COUNT(*) AS check_value
FROM {{ ref('silver_status_updates_all') }}
WHERE empi_id IS NULL OR TRIM(COALESCE(empi_id,'')) = '';

-- 7) Duplicate detection (same empi_id, status_type, effective_from_date, status_value)
WITH dup_keys AS (
  SELECT
    empi_id,
    status_type,
    effective_from_date,
    status_value,
    COUNT(*) AS cnt
  FROM {{ ref('silver_status_updates_all') }}
  GROUP BY empi_id, status_type, effective_from_date, status_value
  HAVING COUNT(*) > 1
)
SELECT 'duplicate_key_groups' AS check_name, COUNT(*) AS groups_with_duplicates
FROM dup_keys;

-- 8) Sample duplicate groups (up to 20) for manual inspection
SELECT empi_id, status_type, effective_from_date, status_value, cnt
FROM (
  SELECT
    empi_id,
    status_type,
    effective_from_date,
    status_value,
    COUNT(*) AS cnt
  FROM {{ ref('silver_status_updates_all') }}
  GROUP BY empi_id, status_type, effective_from_date, status_value
  HAVING COUNT(*) > 1
) t
ORDER BY cnt DESC
LIMIT 20;

-- 9) Parity: compare unified count with sum of source tables (BAR + Athena)
SELECT
  (SELECT COUNT(*) FROM {{ ref('silver_status_updates_all') }}) AS unified_count,
  (SELECT COUNT(*) FROM {{ ref('silver_status_updates_bar') }}) AS bar_count,
  (SELECT COUNT(*) FROM {{ ref('silver_status_updates_athena') }}) AS athena_count,
  (SELECT COUNT(*) FROM {{ ref('silver_status_updates_bar') }}) + (SELECT COUNT(*) FROM {{ ref('silver_status_updates_athena') }}) AS sum_sources;

-- 10) Quick check: rows in unified table that do not have a matching source row (should be none)
-- This verifies the UNION ALL + join did not create extra rows unexpectedly.
WITH src_union AS (
  SELECT source_id, source_id2, program_type, status_type, status_value, effective_from_date, lastmodifieddatetime, username, source_system
  FROM {{ ref('silver_status_updates_bar') }}
  UNION ALL
  SELECT source_id, source_id2, program_type, status_type, status_value, effective_from_date, lastmodifieddatetime, username, source_system
  FROM {{ ref('silver_status_updates_athena') }}
)
SELECT 'unmatched_in_sources' AS check_name, COUNT(*) AS check_value
FROM {{ ref('silver_status_updates_all') }} a
LEFT JOIN src_union s
  ON a.source_id = s.source_id
  AND a.source_id2 = s.source_id2
  AND COALESCE(a.program_type,'') = COALESCE(s.program_type,'')
  AND COALESCE(a.status_type,'') = COALESCE(s.status_type,'')
  AND COALESCE(a.status_value,'') = COALESCE(s.status_value,'')
  AND COALESCE(a.effective_from_date, '1900-01-01') = COALESCE(s.effective_from_date, '1900-01-01')
  AND COALESCE(a.lastmodifieddatetime, '1900-01-01') = COALESCE(s.lastmodifieddatetime, '1900-01-01')
WHERE s.source_id IS NULL;

-- 11) Spot-check sample rows (latest 10) for manual review
SELECT *
FROM {{ ref('silver_status_updates_all') }}
ORDER BY lastmodifieddatetime DESC NULLS LAST
LIMIT 10;

-- End of QA checks