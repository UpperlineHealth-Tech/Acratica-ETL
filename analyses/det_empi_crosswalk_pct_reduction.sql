-- calculating the percentage reduction in source system ids by introducing the deterministic EMPI clustering
select count(distinct source_system_id) as num_ssid
, count(distinct empi_id) as num_empi
, 100 - round(num_empi/num_ssid*100, 2) as pct_reduction
from {{ref('empi_crosswalk_gold')}}