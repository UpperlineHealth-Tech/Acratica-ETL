{{ config(
    severity='warn',
    tags=['dq', 'dq_invalid', 'cclf8']
) }}

{% set invalid_threshold = var('dq_invalid_pct_warn_threshold', 2) %}

with atomic_quality as (
select UPPER('{{ ref('silver_cclf8') }}')::{{ dbt.type_string() }} as table_name
, 'DOB' as column_name
, 'DOB format is YYYY-MM-DD' as quality_check
, case
    when DOB is null then 1
    else 0
end as is_null
, case
    when REGEXP_LIKE(TO_VARCHAR(DOB), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then 1
    else 0
end as is_valid
, case when BIRTH_YEAR is not null and not REGEXP_LIKE(TO_VARCHAR(DOB), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then 1
    else 0
end as is_invalid
from {{ ref('silver_cclf8') }}
),
summary_base as (
    select
        table_name,
        column_name,
        quality_check,
        sum(is_null) as null_count,
        sum(is_valid) as valid_count,
        sum(is_invalid) as invalid_count,
        count(*) as row_count
    from atomic_quality
    group by table_name, column_name, quality_check
),
summary as (
    select
        *,
        round(null_count / nullif(row_count, 0) * 100, 1) as pct_null,
        round(invalid_count / nullif(row_count - null_count, 0) * 100, 1) as pct_invalid
    from summary_base
)

select *
from summary
where pct_invalid is not null
  and pct_invalid > {{ invalid_threshold }}

