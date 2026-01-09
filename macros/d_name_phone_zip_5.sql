{% macro d_name_phone_zip_5(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
SELECT
  LEAST(a.source_system_id, b.source_system_id)    AS id1,
  GREATEST(a.source_system_id, b.source_system_id) AS id2,
  'name_phone_zip_5' AS deterministic_rule
FROM {{ left_table }} a
JOIN {{ right_table }} b
  ON a.first_name = b.first_name
 AND a.last_name = b.last_name  
 AND a.phone = b.phone
 AND a.zip_5 = b.zip_5
 AND (
     a.dob = b.dob
  OR a.dob = DATEADD(day, 1, b.dob)
  OR a.dob = DATEADD(day, -1, b.dob)
  OR (DAY(a.dob) = MONTH(b.dob) AND MONTH(a.dob) = DAY(b.dob) AND a.birth_year = b.birth_year)
  OR (DAY(a.dob) = DAY(b.dob) AND MONTH(a.dob) = MONTH(b.dob) AND ABS(a.birth_year - b.birth_year) = 1)
 )
 AND NOT (a.ssn is not null and b.ssn is not null and a.ssn <> b.ssn)
 AND a.source_system_id <> b.source_system_id
{% endmacro %}