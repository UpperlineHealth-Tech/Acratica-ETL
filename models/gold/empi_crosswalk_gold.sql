{{ config(
    materialized='view',
    alias= this.name ~ var('table_suffix', '')
) }}

with latest_overrides as (
    select *
    {% if (target.name | lower) == 'prod' %}
        from {{ ref('empi_link_unlink_overrides_promoted') }}
    {% else %}
        from {{target.database}}.{{target.schema}}.{{var('empi_overrides_table', 'EMPI_LINK_UNLINK_OVERRIDES')}}
    {% endif %}
    qualify row_number() over (partition by SURROGATE_KEY order by UPDATED_AT desc) = 1
)

, apply_overrides as (
    select *
    from latest_overrides
    where APPROVAL_STATUS = 'Y'
    or (APPROVAL_STATUS = 'N' and REVIEWED_AT is not null)
)

select 
    x.SOURCE_SYSTEM,
    x.SOURCE_SYSTEM_ID,
    x.SOURCE_SYSTEM_ID_TYPE,
    x.PREV_SOURCE_SYSTEM_ID,
    x.PREV_SOURCE_SYSTEM_ID_TYPE,
    x.SOURCE_SYSTEM_ID_2,
    x.SOURCE_SYSTEM_ID_2_TYPE,
    x.FIRST_NAME,
    x.LAST_NAME,
    x.DOB,
    x.SEX,
    x.RACE,
    x.ADDRESS_LINE_1,
    x.ADDRESS_LINE_2,
    x.CITY,
    x.STATE,
    x.ZIP_5,
    x.ZIP_4,
    x.PHONE,
    x.EMAIL,
    x.DEATH_DATE,
    x.SSN,
    x.SURROGATE_KEY,
    case 
        when o.reviewed_at is not null then o.reviewed_at
        else x.CLUSTER_UPDATED_AT
    end as CLUSTER_UPDATED_AT,
    x.EMPI_ID_CANONICAL,
    case 
        when o.NEW_EMPI_ID is not null and o.APPROVAL_STATUS = 'Y' then o.NEW_EMPI_ID
        else x.EMPI_ID
    end as EMPI_ID
from {{ ref('det_empi_crosswalk') }} x
left join apply_overrides o
  on x.SURROGATE_KEY = o.SURROGATE_KEY