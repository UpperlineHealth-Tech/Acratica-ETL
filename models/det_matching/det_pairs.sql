{{ config(
    materialized='table',
    tags=['full_refresh_only'],
    alias= this.name ~ var('table_suffix', '')
) }}

{{ score_pairs() }}