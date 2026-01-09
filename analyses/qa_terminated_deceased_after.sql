-- QA: Count EMPI IDs that have records dated after 'Terminated - DECEASED'
-- Reason: Detect patients that have status updates after a recorded deceased termination event.
-- Run this after populating `{{ ref('silver_status_updates_all') }}`.

-- 1) Determine latest 'Terminated - DECEASED' date per EMPI
WITH terminated AS (
  SELECT
    empi_id,
    MAX(effective_from_date) AS term_dt
  FROM {{ ref('silver_status_updates_all') }}
  WHERE TRIM(status_value) = 'Terminated - DECEASED'
    AND effective_from_date IS NOT NULL
  GROUP BY empi_id
),

-- 2) Find any rows for the same EMPI that occur after the termination date
after_term AS (
  SELECT
    s.empi_id,
    s.status_type,
    s.status_value,
    s.effective_from_date,
    t.term_dt,
    s.source_system,
    s.lastmodifieddatetime,
    s.username
  FROM {{ ref('silver_status_updates_all') }} s
  JOIN terminated t
    ON s.empi_id = t.empi_id
  WHERE s.effective_from_date > t.term_dt
)

-- 3) Summary: how many distinct EMPI have any records after deceased termination
SELECT
  COUNT(DISTINCT empi_id) AS empi_with_records_after_deceased
FROM after_term;

-- 4) Optional: details per EMPI (first and last post-termination dates, counts)
SELECT
  empi_id,
  COUNT(*) AS records_after_count,
  MIN(effective_from_date) AS first_after_date,
  MAX(effective_from_date) AS last_after_date
FROM after_term
GROUP BY empi_id
ORDER BY records_after_count DESC
LIMIT 200;

-- 5) Optional: sample rows (up to 100) for manual inspection
SELECT *
FROM after_term
ORDER BY effective_from_date ASC
LIMIT 100;

-- Notes:
-- - This script uses the LATEST (MAX) 'Terminated - DECEASED' per EMPI as the reference termination date.
-- - The check compares effective dates strictly greater than the termination date; adjust operator if '>= ' is desired.