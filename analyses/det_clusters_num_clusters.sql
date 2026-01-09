select count(cluster_id) as total_records
, count(distinct cluster_id) as num_clusters
from {{ref('det_clusters')}}