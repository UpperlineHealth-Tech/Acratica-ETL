{{ config(
    materialized='ephemeral',
    alias= this.name ~ var('table_suffix', '')
) }}

WITH client_values AS (
  -- Get current value for each relevant custom field for each patient
  SELECT
    c.patientid,
    c.clientrecordnumberid,
    c.recordnumbercategoryid,
    c.recordnumberselectid,
    c.createddatetime,
    c.createdby,
    c.lastmodifieddatetime,
    c.deleteddatetime,
    c.deletedby,
    r.name AS customfield_name,
    COALESCE(
      c.clientrecordnumber::STRING,
      c.clientrecordvalue::STRING,
      TO_CHAR(c.clientrecorddatedatetime, 'YYYY-MM-DD'),
      s.selecttext::STRING,
      s.selectvalue::STRING,
      TO_CHAR(s.selectdatedatetime, 'YYYY-MM-DD')
    ) AS currentvalue
  FROM {{ source('ATHENA_EMR', 'CLIENTRECORDNUMBER') }} c
  LEFT JOIN {{ source('ATHENA_EMR', 'RECORDNUMBERCATEGORY') }} r
    ON c.recordnumbercategoryid = r.recordnumbercategoryid
  LEFT JOIN {{ source('ATHENA_EMR', 'RECORDNUMBERSELECT') }} s
    ON c.recordnumberselectid = s.recordnumberselectid
  WHERE r.name IN (
    'Medicare Upperline Plus Status',
    'Enrollment Location',
    'RCM Claims Flag',
    'APCM Status',
    'High Risk Flag',
    'APCM Flag',
    'MA',
    'Value Based Flag'
  )
),

audit_prelim AS (
  -- Pull audit log for relevant custom fields, decode select-list values
  SELECT
    a.sourceid,
    a.operation,
    a.fieldname,
    a.oldvalue,
    DECODE(
      a.fieldname,
      'RECORDNUMBERSELECTID', COALESCE(s.selecttext::STRING, s.selectvalue::STRING, TO_CHAR(s.selectdatedatetime, 'YYYY-MM-DD')),
      a.oldvalue
    ) AS oldvalue_text,
    a.created,
    a.createdby,
    LEAD(a.oldvalue) OVER (PARTITION BY a.sourceid ORDER BY a.created) AS newvalue
  FROM {{ source('ATHENA_EMR', 'CLIENTRECORDNUMBERAUDR') }} a
  LEFT JOIN {{ source('ATHENA_EMR', 'RECORDNUMBERSELECT') }} s
    ON a.oldvalue = CAST(s.recordnumberselectid AS STRING)
  WHERE a.fieldname IN (
    'CLIENTRECORDNUMBER', 'CLIENTRECORDVALUE', 'CLIENTRECORDDATE', 'RECORDNUMBERSELECTID'
  )
    AND a.sourceid IN (SELECT DISTINCT clientrecordnumberid FROM client_values)
),

audit_final AS (
  -- Map audit operations to readable values, decode newvalue for select-list fields
  SELECT
    ap.sourceid,
    DECODE(ap.operation, 'U', 'UPDATED', 'D', 'DELETED', ap.operation) AS custom_field_operation,
    ap.fieldname,
    ap.oldvalue_text,
    ap.newvalue,
    DECODE(
      ap.fieldname,
      'RECORDNUMBERSELECTID', COALESCE(s.selecttext::STRING, s.selectvalue::STRING, TO_CHAR(s.selectdatedatetime, 'YYYY-MM-DD')),
      ap.newvalue
    ) AS newvalue_text,
    ap.created,
    ap.createdby
  FROM audit_prelim ap
  LEFT JOIN {{ source('ATHENA_EMR', 'RECORDNUMBERSELECT') }} s
    ON ap.newvalue = CAST(s.recordnumberselectid AS STRING)
  WHERE ap.sourceid IN (SELECT DISTINCT clientrecordnumberid FROM client_values)
),

custom_hist AS (
  -- Build audit history: one row per change (CREATED, UPDATED, DELETED)
  SELECT
    cv.patientid,
    aud.custom_field_operation,
    cv.recordnumbercategoryid,
    cv.recordnumberselectid,
    aud.sourceid AS audit_sourceid,
    cv.createddatetime,
    cv.createdby,
    aud.createdby AS aud_createdby,
    cv.lastmodifieddatetime,
    cv.deleteddatetime,
    cv.deletedby,
    cv.customfield_name,
    aud.fieldname AS auditfield_name,
    cv.currentvalue,
    IFF(TRY_TO_DATE(aud.oldvalue_text) IS NOT NULL,
      TO_CHAR(TRY_TO_DATE(aud.oldvalue_text)),
      aud.oldvalue_text
    ) AS oldvalue,
    IFF(TRY_TO_DATE(COALESCE(aud.newvalue_text, cv.currentvalue)) IS NOT NULL,
      TO_CHAR(TRY_TO_DATE(COALESCE(aud.newvalue_text, cv.currentvalue))),
      COALESCE(aud.newvalue_text, cv.currentvalue)
    ) AS newvalue,
    aud.created AS audit_record_created
  FROM client_values cv
  LEFT JOIN audit_final aud
    ON cv.clientrecordnumberid = aud.sourceid
  WHERE aud.sourceid IS NOT NULL

  UNION ALL

  -- CREATED events: first audit or creation per record
  SELECT
    cv.patientid,
    'CREATED' AS custom_field_operation,
    cv.recordnumbercategoryid,
    cv.recordnumberselectid,
    aud.sourceid AS audit_sourceid,
    cv.createddatetime,
    cv.createdby,
    aud.createdby AS aud_createdby,
    cv.lastmodifieddatetime,
    cv.deleteddatetime,
    cv.deletedby,
    cv.customfield_name,
    aud.fieldname AS auditfield_name,
    cv.currentvalue,
    NULL AS oldvalue,
    COALESCE(
      IFF(TRY_TO_DATE(aud.oldvalue_text) IS NOT NULL, TO_CHAR(TRY_TO_DATE(aud.oldvalue_text)), aud.oldvalue_text),
      cv.currentvalue
    ) AS newvalue,
    aud.created AS audit_record_created
  FROM client_values cv
  LEFT JOIN audit_final aud
    ON cv.clientrecordnumberid = aud.sourceid
  QUALIFY ROW_NUMBER() OVER (PARTITION BY clientrecordnumberid ORDER BY COALESCE(aud.created, cv.createddatetime)) = 1
)

-- Final output: Athena audit history for status fields
, final as (
SELECT
  c.patientid,
  pt.enterpriseid,
  c.recordnumbercategoryid,
  c.recordnumberselectid,
  c.audit_sourceid,
  c.customfield_name,
  c.auditfield_name,
  c.custom_field_operation,
  c.currentvalue,
  c.oldvalue AS Old_Status_Value,
  c.newvalue AS Status_Value_Update,
  DECODE(
    c.custom_field_operation,
    'CREATED', c.createddatetime,
    'UPDATED', c.audit_record_created,
    'DELETED', c.audit_record_created
  ) AS Effective_From_Date,
  DECODE(
    c.custom_field_operation,
    'CREATED', c.createdby,
    'UPDATED', c.aud_createdby,
    'DELETED', c.deletedby
  ) AS Athena_user,
  c.createddatetime,
  c.lastmodifieddatetime,
  c.deleteddatetime,
  c.audit_record_created
FROM custom_hist c
INNER JOIN {{ source('ATHENA_EMR', 'PATIENT') }} pt
  ON c.patientid = pt.patientid
ORDER BY c.patientid, c.audit_record_created
)

-- Insert status updates for program-specific flags
SELECT
    PATIENTID::string AS source_id,
    ENTERPRISEID::string AS source_id2,
    CASE
      WHEN customfield_name IN ('Medicare Upperline Plus Status','High Risk Flag','Value Based Flag') THEN 'ACO_REACH'
      WHEN customfield_name IN ('APCM Status','APCM Flag') THEN 'APCM'
      WHEN customfield_name = 'MA' THEN 'MA'
      ELSE NULL
    END AS program_type,
    NULL AS program_type2,
    customfield_name AS status_type,
    Status_Value_Update AS status_value,
    custom_field_operation,
    Effective_From_Date,
    Athena_user AS username,
    lastmodifieddatetime,
    'ATHENA' AS source_system
FROM final

UNION ALL

-- Insert status updates for document uploads
SELECT
    PAT.PATIENTID::string AS source_id,
    PAT.ENTERPRISEID::string AS source_id2,
    CASE
      WHEN CLINICALORDERTYPE = 'ACCOUNTABLE CARE ORGANIZATION (ACO) CONTRACT' THEN 'ACO_REACH'
      WHEN CLINICALORDERTYPE = 'CHRONIC CARE MANAGEMENT AGREEMENT' THEN 'APCM'
      ELSE NULL
    END AS program_type,
    NULL AS program_type2,
    CASE
      WHEN CLINICALORDERTYPE = 'ACCOUNTABLE CARE ORGANIZATION (ACO) CONTRACT' THEN 'Medicare Upperline Plus Status - Document'
      WHEN CLINICALORDERTYPE = 'CHRONIC CARE MANAGEMENT AGREEMENT' THEN 'APCM Status - Document'
      ELSE NULL
    END AS status_type,
    'Document - ' || STATUS AS status_value,
    NULL AS custom_field_operation,
    DOCUMENT.CREATEDDATETIME AS effective_from_date,
    createdby AS username,
    NULL AS lastmodifieddatetime,
    'ATHENA' AS source_system
FROM {{ source('ATHENA_EMR', 'DOCUMENT') }} DOCUMENT
INNER JOIN {{ source('ATHENA_EMR', 'PATIENT') }} PAT ON DOCUMENT.PATIENTID = PAT.PATIENTID
WHERE CLINICALORDERTYPE IN ('ACCOUNTABLE CARE ORGANIZATION (ACO) CONTRACT', 'CHRONIC CARE MANAGEMENT AGREEMENT')
