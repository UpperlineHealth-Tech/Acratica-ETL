{% macro d_name_enterpriseid(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
SELECT
  LEAST(a.source_system_id, b.source_system_id)    AS id1,
  GREATEST(a.source_system_id, b.source_system_id) AS id2,
  'name_enterpriseid' AS deterministic_rule
FROM {{ left_table }} a
JOIN {{ right_table }} b
  ON a.first_name = b.first_name
 AND a.last_name = b.last_name
 AND a.source_system_id_2 = b.source_system_id_2
 AND a.source_system = 'ATHENA'
 AND b.source_system = 'ATHENA'
 AND NOT (a.ssn is not null and b.ssn is not null and a.ssn <> b.ssn)
 AND a.source_system_id <> b.source_system_id
{% endmacro %}