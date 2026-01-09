/*
Checking the enterpriseid from athena in the crosswalk table (aliased as source_system_id_2) to see if any enterprise IDs
are being assigned to more than one empi_id for investigation.
Calculates the percentage of enterpriseids with multiple empi_ids assigned.
*/
with enterpriseid_checks as (
select source_system_id_2
, count(distinct empi_id) as ct
from {{ref('empi_crosswalk_gold')}} 
where source_system = 'ATHENA'
group by source_system_id_2
having ct > 1
)
, filtered as (
select *
from {{ref('empi_crosswalk_gold')}} 
where source_system = 'ATHENA'
and source_system_id_2 in (select source_system_id_2 from enterpriseid_checks)
order by source_system_id_2, empi_id
)


select (select count(distinct source_system_id_2) from filtered) as enterprise_multi_empi
, (select count(distinct source_system_id_2) from {{ref('empi_crosswalk_gold')}}) as enterprise_distinct
, round(enterprise_multi_empi/enterprise_distinct*100, 3) as pct_enterprise_multi_empi
