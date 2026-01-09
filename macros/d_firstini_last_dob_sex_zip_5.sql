{% macro d_firstini_last_dob_sex_zip_5(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
  SELECT
  LEAST(a.source_system_id, b.source_system_id)    AS id1,
  GREATEST(a.source_system_id, b.source_system_id) AS id2,
  'firstini_last_dob_sex_zip_5' AS deterministic_rule
FROM {{ left_table }} a
JOIN {{ right_table }} b
  ON a.first_initial = b.first_initial
 AND a.last_name = b.last_name 
 AND a.dob = b.dob
 AND a.sex = b.sex
 AND a.zip_5 = b.zip_5
 AND NOT (a.ssn is not null and b.ssn is not null and a.ssn <> b.ssn)
 AND a.source_system_id <> b.source_system_id
{% endmacro %}