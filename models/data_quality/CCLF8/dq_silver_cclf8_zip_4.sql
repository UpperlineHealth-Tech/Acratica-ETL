with atomic_quality as (
select UPPER('{{ ref('silver_cclf8') }}')::{{ dbt.type_string() }} as table_name
, 'ZIP_4' as column_name
, 'Length of 4-digit zip code length is 4' as quality_check
, case
    when ZIP_4 is null then 1
    else 0
end as is_null
, case
    when length(ZIP_4) = 4 then 1
    else 0
end as is_valid
, case when ZIP_4 is not null and length(ZIP_4) != 4 then 1
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
