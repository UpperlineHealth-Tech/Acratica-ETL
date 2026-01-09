{% macro ensure_empi_overrides(database= target.database, schema= target.schema, table = var('empi_overrides_table', 'EMPI_LINK_UNLINK_OVERRIDES')) %}

{% set sql %} 
    create table if not exists {{database}}.{{schema}}.{{table}} (
        id int identity(1,1),
        surrogate_key string,
        source_system string,
        source_system_id string,
        source_system_id_2 string,
        empi_id string,
        new_empi_id string,
        updated_at timestamp_ntz default current_timestamp(),
        updated_by string default current_user(),
        override_action string,
        reason string,
        approval_status string default 'N',
        reviewed_at timestamp_ntz default null,
        reviewed_by string default null,
        reviewer_comment string default null
    );
   
{% endset %}

{{ log("Ensuring EMPI overrides table exists: " ~ database ~ "." ~ schema ~ "." ~ table, info=True) }}

{% do run_query(sql) %}

{% endmacro %}