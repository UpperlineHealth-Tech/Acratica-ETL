-- depends_on: {{ ref('det_probability') }}
{{ config(
    materialized='incremental',
    unique_key='SURROGATE_KEY',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    alias= this.name ~ var('table_suffix', '')
) }}


{% if not is_incremental() %}

-- ===================================================================
-- FULL REBUILD: use det_clusters (label propagation) as source of truth
-- ===================================================================
with empi as (
    select *
    from {{ ref('silver_empi_input') }}
)

, empi_canonical as (
    select
        i.SOURCE_SYSTEM,
        i.SOURCE_SYSTEM_ID,
        i.SOURCE_SYSTEM_ID_TYPE,
        i.PREV_SOURCE_SYSTEM_ID,
        i.PREV_SOURCE_SYSTEM_ID_TYPE,
        i.SOURCE_SYSTEM_ID_2,
        i.SOURCE_SYSTEM_ID_2_TYPE,
        i.FIRST_NAME,
        i.LAST_NAME,
        i.DOB,
        i.SEX,
        i.RACE,
        i.ADDRESS_LINE_1,
        i.ADDRESS_LINE_2,
        i.CITY,
        i.STATE,
        i.ZIP_5,
        i.ZIP_4,
        i.PHONE,
        i.EMAIL,
        i.DEATH_DATE,
        i.SSN,
        i.SURROGATE_KEY,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as CLUSTER_UPDATED_AT,
          -- canonical EMPI for full rebuild: cluster_id if present, else source_system_id
        coalesce(m.CLUSTER_ID, i.SOURCE_SYSTEM_ID) as EMPI_ID_CANONICAL
    from empi i
    left join {{ ref('det_clusters') }} m
      on i.SOURCE_SYSTEM_ID = m.SOURCE_ID
)

select
    *,
    -- hashed EMPI for external use
    substr(SHA2_HEX(EMPI_ID_CANONICAL::string, 256), 1, 32) as EMPI_ID
from empi_canonical

{% else %}

-- ===================================================================
-- INCREMENTAL: merge-only EMPI updates (no splits)
-- ===================================================================

-- 1) current crosswalk (existing state)
with current_crosswalk as (
    select *
    from {{ this }}
)

-- 2) derive watermark from CLUSTER_UPDATED_AT
, last_run as (
    select coalesce(
               max(CLUSTER_UPDATED_AT),
               to_timestamp_ntz('1900-01-01') 
           ) as WATERMARK
    from current_crosswalk
)

-- 3) EMPI input: only rows changed since last EMPI update
, empi as (
    select *
    from {{ ref('silver_empi_input') }}
)

, changed as (
    select e.*
    from empi e
    join last_run lr
      on e.LAST_UPDATED > lr.WATERMARK
)

-- 4) build delta pairs between changed records and full EMPI input

, delta_pairs_raw as (
{{score_pairs('changed', 'empi') }} 
)

, delta_pairs as (
select l_id as id1
, r_id as id2
, deterministic_rule
, total_log_lr
, p_match
from delta_pairs_raw
where (deterministic_rule <> 'name_state' and total_log_lr > {{ var('log_lr_threshold') }} 
or (deterministic_rule = 'name_state' and total_log_lr > {{ var('clerical_name_state_log_lr_threshold') }}))
and deterministic_rule <> 'name_dob_sex_exact_clerical'
)

-- 5) all IDs involved this run (whether they matched or not)
, delta_ids as (
    select ID1 as SOURCE_SYSTEM_ID from delta_pairs
    union
    select ID2 as SOURCE_SYSTEM_ID from delta_pairs
    union
    select SOURCE_SYSTEM_ID from changed
)

-- 6) neighbors: for each ID, which EMPIs do its matched neighbors currently have?
, neighbors as (
    select
        p.ID1 as THIS_ID,
        c.EMPI_ID_CANONICAL
    from delta_pairs p
    join current_crosswalk c
      on c.SOURCE_SYSTEM_ID = p.ID2

    union all

    select
        p.ID2 as THIS_ID,
        c.EMPI_ID_CANONICAL
    from delta_pairs p
    join current_crosswalk c
      on c.SOURCE_SYSTEM_ID = p.ID1
)

, neighbor_agg as (
    select
        THIS_ID as SOURCE_SYSTEM_ID,
        array_agg(distinct EMPI_ID_CANONICAL) as NEIGHBOR_EMPI_IDS
    from neighbors
    group by THIS_ID
)

-- 7) decide canonical EMPI for each delta_id (merge-only)
, delta_empi_assignments as (
    select
        d.SOURCE_SYSTEM_ID,
        case
            when n.NEIGHBOR_EMPI_IDS is null
                 and c.EMPI_ID_CANONICAL is not null then
                -- Existing patient, no new neighbor info → keep current EMPI
                c.EMPI_ID_CANONICAL

            when n.NEIGHBOR_EMPI_IDS is null
                 and c.EMPI_ID_CANONICAL is null then
                -- Brand new patient, no neighbors → new EMPI based on own ID
                d.SOURCE_SYSTEM_ID

            when array_size(n.NEIGHBOR_EMPI_IDS) = 1 then
                case 
                    when c.EMPI_ID_CANONICAL is not null then
                        least(
                            c.EMPI_ID_CANONICAL,
                            n.NEIGHBOR_EMPI_IDS[0]
                        )
                    else
                        n.NEIGHBOR_EMPI_IDS[0]
                end

            else
                -- Multiple neighbor EMPIs → canonical = min
                array_min(n.NEIGHBOR_EMPI_IDS)
        end as EMPI_ID_CANONICAL
    from delta_ids d
    left join neighbor_agg n
      on d.SOURCE_SYSTEM_ID = n.SOURCE_SYSTEM_ID
    left join current_crosswalk c
      on d.SOURCE_SYSTEM_ID = c.SOURCE_SYSTEM_ID
)

-- 8) identify EMPI merges (cases where a single ID connects multiple EMPIs)
, empi_merge_candidates as (
    select
        d.SOURCE_SYSTEM_ID,
        n.NEIGHBOR_EMPI_IDS,
        array_min(n.NEIGHBOR_EMPI_IDS) as CANONICAL_EMPI
    from delta_ids d
    join neighbor_agg n
      on d.SOURCE_SYSTEM_ID = n.SOURCE_SYSTEM_ID
    where array_size(n.NEIGHBOR_EMPI_IDS) > 1
)

-- explode NEIGHBOR_EMPI_IDS → (old_empi_id, canonical_empi)
, empi_merge_map_raw as (
    select
        f.value as OLD_EMPI_ID,
        c.CANONICAL_EMPI
    from empi_merge_candidates c,
         lateral flatten(input => c.NEIGHBOR_EMPI_IDS) f
)

-- if an OLD_EMPI_ID appears with multiple canonicals (unlikely), pick the min
, empi_merge_map as (
    select
        OLD_EMPI_ID,
        min(CANONICAL_EMPI) as CANONICAL_EMPI
    from empi_merge_map_raw
    group by OLD_EMPI_ID
)

-- 9) updated rows for existing members whose EMPI is being merged
, updated_existing as (
    select
        c.SOURCE_SYSTEM,
        c.SOURCE_SYSTEM_ID,
        c.SOURCE_SYSTEM_ID_TYPE,
        c.PREV_SOURCE_SYSTEM_ID,
        c.PREV_SOURCE_SYSTEM_ID_TYPE,
        c.SOURCE_SYSTEM_ID_2,
        c.SOURCE_SYSTEM_ID_2_TYPE,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.DOB,
        c.SEX,
        c.RACE,
        c.ADDRESS_LINE_1,
        c.ADDRESS_LINE_2,
        c.CITY,
        c.STATE,
        c.ZIP_5,
        c.ZIP_4,
        c.PHONE,
        c.EMAIL,
        c.DEATH_DATE,
        c.SSN,
        c.SURROGATE_KEY,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as CLUSTER_UPDATED_AT,
        coalesce(m.CANONICAL_EMPI, c.EMPI_ID_CANONICAL) as EMPI_ID_CANONICAL,
        substr(SHA2_HEX(coalesce(m.CANONICAL_EMPI, c.EMPI_ID_CANONICAL)::string, 256), 1, 32) as EMPI_ID
    from current_crosswalk c
    join empi_merge_map m
      on c.EMPI_ID_CANONICAL = m.OLD_EMPI_ID
    left join delta_ids d
      on d.SOURCE_SYSTEM_ID = c.SOURCE_SYSTEM_ID
    where d.SOURCE_SYSTEM_ID is null   -- exclude delta_ids
)

-- 10) delta rows for IDs in this batch (new or changed IDs)
, delta_rows as (
    select
        i.SOURCE_SYSTEM,
        i.SOURCE_SYSTEM_ID,
        i.SOURCE_SYSTEM_ID_TYPE,
        i.PREV_SOURCE_SYSTEM_ID,
        i.PREV_SOURCE_SYSTEM_ID_TYPE,
        i.SOURCE_SYSTEM_ID_2,
        i.SOURCE_SYSTEM_ID_2_TYPE,
        i.FIRST_NAME,
        i.LAST_NAME,
        i.DOB,
        i.SEX,
        i.RACE,
        i.ADDRESS_LINE_1,
        i.ADDRESS_LINE_2,
        i.CITY,
        i.STATE,
        i.ZIP_5,
        i.ZIP_4,
        i.PHONE,
        i.EMAIL,
        i.DEATH_DATE,
        i.SSN,
        i.SURROGATE_KEY,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz as CLUSTER_UPDATED_AT,
        a.EMPI_ID_CANONICAL,
        substr(SHA2_HEX(a.EMPI_ID_CANONICAL::string, 256), 1, 32) as EMPI_ID
    from delta_empi_assignments a
    join empi i
      on i.SOURCE_SYSTEM_ID = a.SOURCE_SYSTEM_ID
)

-- 11) final output for incremental run:
--     - existing members whose EMPI changed due to cluster merges
--     - plus the delta IDs (new/changed input rows)
select * from updated_existing
union all
select * from delta_rows

{% endif %}