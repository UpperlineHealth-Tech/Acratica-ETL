select deterministic_rule, count(*) as num_pairs
from {{ref('det_pairs')}}
group by deterministic_rule
order by num_pairs desc