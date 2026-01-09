{{ config(
    materialized='table',
    tags=['full_refresh_only'],
    alias= this.name ~ var('table_suffix', '')
) }}

select l_id
, r_id
from {{ ref('det_pairs_auto_match') }}