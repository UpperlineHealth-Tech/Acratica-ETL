{{ config(
    materialized='table',
    tags=['full_refresh_only'],
    alias= this.name ~ var('table_suffix', '')
) }}

select l_id as id
from {{ ref('det_edges') }}

union

select r_id
from {{ ref('det_edges') }}