with bar_records as (select *
from {{ref('empi_crosswalk_gold')}}
where source_system = 'BAR')

, missing_bar_to_athena as (select a.empi_id 
, a.source_system
, a.source_system_id
, a.prev_source_system_id_type
, a.prev_source_system_id
, b.source_system
, b.source_system_id
, a.first_name
, b.first_name
, a.last_name
, b.last_name
from bar_records a
left join {{ref('empi_crosswalk_gold')}} b
ON a.empi_id = b.empi_id
AND a.source_system <> b.source_system
where b.source_system is null
order by a.empi_id)

select (select count(*) from missing_bar_to_athena) as missing_bar_to_athena_count
, (select count(*) from bar_records) as total_bar_records
, round((select count(*) from missing_bar_to_athena)/(select count(*) from bar_records)*100,2) as pct_bar_missing_athena