-- cluster sizes
select cluster_id
, count(*) as cluster_size
from {{ref('det_clusters')}}
group by cluster_id
order by count(*) desc