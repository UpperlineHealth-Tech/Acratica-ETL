{% snapshot probability_snapshot %}

{{
    config(
        unique_key = ['FIELD', 'CMP_LEVEL'],
        strategy = 'timestamp',
        updated_at = 'last_updated_at',
        hard_deletes = 'invalidate',
        alias= this.name ~ var('table_suffix', ''),
        transient= false
    )
}}

select *
from {{ ref('det_probability') }}

{% endsnapshot %}