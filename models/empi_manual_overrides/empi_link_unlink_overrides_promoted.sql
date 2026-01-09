{{config(
    materialized='table',
    enabled= (target.name | lower) == 'prod'
)}}

select *
from {{target.database}}.{{var('staging_schema', 'DBT_EMPI_STG')}}.{{var('empi_overrides_table', 'EMPI_LINK_UNLINK_OVERRIDES')}}