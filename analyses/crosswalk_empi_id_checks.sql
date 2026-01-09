/*
Checking the empi_id from crosswalk to see how many empi_ids comprise two or more distinct enterpriseids from Athena.
Gives a sense of how well the enterpriseid is being assigned in Athena relative to empi_id.
Calculates the percentage of empi_ids with multiple enterpriseids assigned.
*/

with empi_id_checks as (
select empi_id
, count(distinct source_system_id_2) as ct
from {{ref('empi_crosswalk_gold')}} 
where source_system = 'ATHENA'
group by empi_id
having ct > 1
)


, filtered as (
select *
from {{ref('empi_crosswalk_gold')}} 
where source_system = 'ATHENA'
and empi_id in (select empi_id from empi_id_checks)
order by empi_id, source_system_id_2
)

select (select count(distinct empi_id) from filtered) as empi_multi_enterprise
, (select count(distinct empi_id) from {{ref('empi_crosswalk_gold')}}) as empi_distinct
, round(empi_multi_enterprise/empi_distinct*100, 2) as pct_empi_multi_enterprise