{% macro d_ssn_dob_sex(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
SELECT
  LEAST(a.source_system_id, b.source_system_id)    AS id1,
  GREATEST(a.source_system_id, b.source_system_id) AS id2,
  'ssn_dob_sex' AS deterministic_rule
FROM {{ left_table }} a
JOIN {{ right_table }} b
  ON a.ssn = b.ssn
 AND a.dob = b.dob
 AND a.sex = b.sex
 AND a.source_system_id <> b.source_system_id
{% endmacro %}