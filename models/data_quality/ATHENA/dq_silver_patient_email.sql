with atomic_quality as (
select UPPER('{{ ref('silver_patient') }}')::{{ dbt.type_string() }} as table_name
, 'EMAIL' as column_name
, 'EMAIL format is valid' as quality_check
, case
    when EMAIL is null then 1
    else 0
end as is_null
, case
    when REGEXP_LIKE(EMAIL, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$') then 1
    else 0
end as is_valid
, case when EMAIL is not null and not REGEXP_LIKE(EMAIL, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$') then 1
    else 0
end as is_invalid
from {{ ref('silver_patient') }}

UNION ALL

select UPPER('{{ ref('silver_patient') }}')::{{ dbt.type_string() }} as table_name
, 'EMAIL' as column_name
, 'EMAIL is placeholder' as quality_check
, case
    when EMAIL is null then 1
    else 0
end as is_null
, case
    when not REGEXP_LIKE(email, '.*(NONGIVEN|NOGIVEN|NONEGIVEN|NONGIVIN|SCHEDULING|NOEMAIL|NOREPLY|DECLINED|NOTAVAILABLE).*', 'i') then 1
    else 0
end as is_valid
, case when EMAIL is not null and REGEXP_LIKE(email, '.*(NONGIVEN|NOGIVEN|NONEGIVEN|NONGIVIN|SCHEDULING|NOEMAIL|NOREPLY|DECLINED|NOTAVAILABLE).*', 'i') then 1
    else 0
end as is_invalid
from {{ ref('silver_patient') }}
)

select table_name
, column_name
, quality_check
, sum(is_null) as null_count
, sum(is_valid) as valid_count
, sum(is_invalid) as invalid_count
, count(*) as row_count
, round(null_count/row_count*100, 1) as pct_null
, round(invalid_count/(row_count - null_count)*100, 1) as pct_invalid
from atomic_quality
group by column_name, quality_check, table_name
