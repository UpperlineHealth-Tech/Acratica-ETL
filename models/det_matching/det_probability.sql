{{ config(
    materialized='table',
    tags=['full_refresh_only', 'recal'],
    alias= this.name ~ var('table_suffix', '')
) }}

with m_u_join as (
select 
mv.field as field
, mv.cmp_level as cmp_level
, mv.m_value as m_value
, uv.u_value as u_value
from {{ ref('det_m_values') }} mv
left join {{ ref('det_u_values') }} uv
  on mv.field = uv.field
 and mv.cmp_level = uv.cmp_level
)

select *
, m_value / coalesce(u_value, 1.0::float/1.25E7) as likelihood_ratio
, ln(m_value / coalesce(u_value, 1.0::float/1.25E7)) as log_likelihood_ratio
, current_timestamp()::timestamp_ntz as last_updated_at
from m_u_join