{% macro d_fuzzy_fullname_dob_sex(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
SELECT
  LEAST(a.source_system_id, b.source_system_id)    AS id1,
  GREATEST(a.source_system_id, b.source_system_id) AS id2,
  'fuzzy_fullname_dob_sex' AS deterministic_rule
FROM {{left_table}} a
JOIN {{right_table}} b
  ON a.dob = b.dob
  AND a.sex = b.sex
  AND EDITDISTANCE(CONCAT(COALESCE(a.first_name, ''), ' ', COALESCE(a.last_name, '')),
                          CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.last_name, ''))) <=3
 AND NOT (a.ssn is not null and b.ssn is not null and a.ssn <> b.ssn)
 AND a.source_system_id <> b.source_system_id
{% endmacro %}