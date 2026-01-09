{% macro d_first_name_dob_sex_zip_5(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
SELECT
  LEAST(a.source_system_id, b.source_system_id)    AS id1,
  GREATEST(a.source_system_id, b.source_system_id) AS id2,
  'first_name_dob_sex_zip_5' AS deterministic_rule
FROM {{ left_table }} a
JOIN {{ right_table }} b
  ON a.first_name = b.first_name
 AND (
    a.last_name_soundex = b.last_name_soundex
    OR
    split_part(split_part(a.last_name, '-', 1), ' ', 1) = split_part(split_part(b.last_name, '-', 1), ' ', 1)
    OR editdistance(a.last_name, b.last_name) <= 
        CASE
            WHEN LEAST(length(a.last_name), length(b.last_name)) <= 4 then 0
            WHEN LEAST(length(a.last_name), length(b.last_name)) <= 8 then 1
            ELSE 2
        END)
 AND a.dob = b.dob
 AND a.sex = b.sex
 AND a.zip_5 = b.zip_5
 AND NOT (a.ssn is not null and b.ssn is not null and a.ssn <> b.ssn)
 AND a.source_system_id <> b.source_system_id
{% endmacro %}