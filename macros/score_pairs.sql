{%- macro score_pairs(left_table = ref('silver_empi_input') , right_table = ref('silver_empi_input')) %}
with d_ssn_dob_sex as ({{ d_ssn_dob_sex(left_table, right_table) }})
, d_name_dob_sex as ({{ d_name_dob_sex(left_table, right_table) }})
, d_firstini_last_dob_sex_zip_5 as ({{ d_firstini_last_dob_sex_zip_5(left_table, right_table) }})
, d_name_phone_zip_5 as ({{ d_name_phone_zip_5(left_table, right_table) }})
, d_first_name_dob_sex_zip_5 as ({{ d_first_name_dob_sex_zip_5(left_table, right_table) }})
, d_name_email_zip_5 as ({{ d_name_email_zip_5(left_table, right_table) }})
, d_name_enterpriseid as ({{ d_name_enterpriseid(left_table, right_table) }})
, d_name_prev_id as ({{ d_name_prev_id(left_table, right_table) }})
, d_name_state as ({{ d_name_state(left_table, right_table) }})
, d_fuzzy_fullname_dob_sex as ({{ d_fuzzy_fullname_dob_sex(left_table, right_table) }})
, d_clerical_name_dob_sex as ({{ d_clerical_name_dob_sex(left_table, right_table) }})

, unioned as (
SELECT id1, id2, '1' AS "label", deterministic_rule, 1 AS rule_num from d_ssn_dob_sex
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 2 AS rule_num from d_name_dob_sex
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 3 AS rule_num from d_firstini_last_dob_sex_zip_5
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 4 AS rule_num from d_name_phone_zip_5
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 5 AS rule_num from d_first_name_dob_sex_zip_5
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 6 AS rule_num from d_name_email_zip_5
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 7 AS rule_num from d_name_enterpriseid
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 8 AS rule_num from d_name_prev_id
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 9 AS rule_num from d_name_state
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 10 AS rule_num from d_fuzzy_fullname_dob_sex
UNION ALL
SELECT id1, id2, '1' AS "label", deterministic_rule, 11 AS rule_num from d_clerical_name_dob_sex
)

, deduped as (
SELECT *
FROM unioned
QUALIFY row_number() OVER(PARTITION BY id1, id2 ORDER BY rule_num) = 1
)

, bucketed as (
  SELECT deduped.deterministic_rule
, deduped."label"
, {{ field_levels() }}
FROM deduped 
JOIN {{ ref('silver_empi_input') }} a
ON deduped.id1 = a.SOURCE_SYSTEM_ID
JOIN {{ ref('silver_empi_input') }} b
ON deduped.id2 = b.SOURCE_SYSTEM_ID
)

select b.* 
, coalesce(ss.log_likelihood_ratio, 0) as ssid2_log_lr
, coalesce(ph.log_likelihood_ratio, 0) as phone_log_lr
, coalesce(em.log_likelihood_ratio, 0) as email_log_lr
, coalesce(ssn.log_likelihood_ratio, 0) as ssn_log_lr
, coalesce(ln.log_likelihood_ratio, 0) as last_name_log_lr
, coalesce(fn.log_likelihood_ratio, 0) as first_name_log_lr
, coalesce(dob.log_likelihood_ratio, 0) as dob_log_lr
, coalesce(zip.log_likelihood_ratio, 0) as zip_log_lr
, coalesce(dd.log_likelihood_ratio, 0) as death_date_log_lr
, coalesce(sx.log_likelihood_ratio, 0) as sex_log_lr
, coalesce(fa.log_likelihood_ratio, 0) as full_address_log_lr
, ssid2_log_lr + phone_log_lr + email_log_lr + ssn_log_lr + last_name_log_lr + first_name_log_lr 
   + dob_log_lr + zip_log_lr + death_date_log_lr + sex_log_lr + full_address_log_lr as total_log_lr
, exp(total_log_lr)*{{var('blocked_set_prior')}} / (exp(total_log_lr)*{{var('blocked_set_prior')}} + 1 - {{var('blocked_set_prior')}}) as p_match
from bucketed b
left join {{ ref('det_probability') }} ss
on ss.field = 'ssid2'
and b.ssid2_l = ss.cmp_level
left join {{ ref('det_probability') }} ph
on ph.field = 'phone'
and b.phone_l = ph.cmp_level
left join {{ ref('det_probability') }} em
on em.field = 'email'
and b.email_l = em.cmp_level
left join {{ ref('det_probability') }} ssn
on ssn.field = 'ssn'
and b.ssn_l = ssn.cmp_level
left join {{ ref('det_probability') }} ln
on ln.field = 'last_name'
and b.last_name_l = ln.cmp_level
left join {{ ref('det_probability') }} fn
on fn.field = 'first_name'
and b.first_name_l = fn.cmp_level
left join {{ ref('det_probability') }} dob
on dob.field = 'dob'
and b.dob_l = dob.cmp_level
left join {{ ref('det_probability') }} zip
on zip.field = 'zip'
and b.zip_l = zip.cmp_level
left join {{ ref('det_probability') }} dd
on dd.field = 'death_date'
and b.death_date_l = dd.cmp_level
left join {{ ref('det_probability') }} sx
on sx.field = 'sex'
and b.sex_l = sx.cmp_level
left join {{ ref('det_probability') }} fa
on fa.field = 'full_address'
and b.full_address_l = fa.cmp_level
order by p_match desc
{% endmacro %}