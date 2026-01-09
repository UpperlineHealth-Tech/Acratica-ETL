{% snapshot empi_crosswalk_gold_snapshot %}

{{
    config(
        unique_key = 'SURROGATE_KEY',
        strategy = 'check',
        check_cols = ['empi_id'],
        hard_deletes = 'invalidate',
        alias= this.name ~ var('table_suffix', ''),
        transient= false
    )
}}

select *
from {{ ref('empi_crosswalk_gold') }}

{% endsnapshot %}