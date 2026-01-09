{% macro d_name_prev_id(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
SELECT
  LEAST(a.source_system_id, b.source_system_id)    AS id1,
  GREATEST(a.source_system_id, b.source_system_id) AS id2,
  'name_prev_id' AS deterministic_rule
FROM {{ left_table }} a
JOIN {{ right_table }} b
  ON (a.prev_source_system_id = b.prev_source_system_id 
    or a.source_system_id = b.prev_source_system_id
    or a.prev_source_system_id = b.source_system_id
    )
 AND a.source_system in ('BAR', 'CCLF8')
 AND b.source_system in ('BAR', 'CCLF8')
 AND NOT (a.ssn is not null and b.ssn is not null and a.ssn <> b.ssn)
 AND a.source_system_id <> b.source_system_id
{% endmacro %}