{{ config(
    severity='warn',
    tags=['dq', 'dq_profile', 'bar']
) }}

{% set null_threshold = 20 %}

with atomic_quality as (
select UPPER('{{ ref('silver_bar') }}')::{{ dbt.type_string() }} as table_name
, 'ADDRESS_LINE_1' as column_name
, 'Null check of address line 1' as quality_check
, case
    when ADDRESS_LINE_1 is null then 1
    else 0
end as is_null
, null as is_valid
, null as is_invalid
from {{ ref('silver_bar') }}
),
summary_base as (
    select
        table_name,
        column_name,
        quality_check,
        sum(is_null) as null_count,
        count(*) as row_count
    from atomic_quality
    group by table_name, column_name, quality_check
),
summary as (
    select
        *,
        round(null_count / nullif(row_count, 0) * 100, 1) as pct_null
    from summary_base
)

select *
from summary
where pct_null is not null
  and pct_null > { null_threshold }
