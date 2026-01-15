{{ config(
    materialized='table',
    tags=['full_refresh_only'],
    pre_hook=["{{ run_empi_label_prop(ref('det_edges'), ref('det_nodes'), this) }}"],
    alias= this.name ~ var('table_suffix', ''),
    static_analysis='unsafe'
) }}

select * from {{ this }}