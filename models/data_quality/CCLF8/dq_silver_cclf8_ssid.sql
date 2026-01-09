with atomic_quality as (
select UPPER('{{ ref('silver_cclf8') }}')::{{ dbt.type_string() }} as table_name
, 'SOURCE_SYSTEM_ID' as column_name
, 'Length of MBI length is 11' as quality_check
, case
    when SOURCE_SYSTEM_ID is null then 1
    else 0
end as is_null
, case
    when length(SOURCE_SYSTEM_ID) = 11 then 1
    else 0
end as is_valid
, case when SOURCE_SYSTEM_ID is not null and length(SOURCE_SYSTEM_ID) != 11 then 1
    else 0
end as is_invalid
from {{ ref('silver_cclf8') }}
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
