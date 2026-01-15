# QA Analysis Query: crosswalk_enterprise_id_checks

**Source file:** `analyses/crosswalk_enterprise_id_checks.sql`  
**GitHub permalink:** https://github.com/UpperlineHealth-Tech/Acratica-ETL/blob/1a2168e26e17e6bf7338c1a66df79cf384927491/analyses/crosswalk_enterprise_id_checks.sql?plain=1#L1-L25

## What this QA query is for

Checking the enterpriseid from athena in the crosswalk table (aliased as source_system_id_2) to see if any enterprise IDs
are being assigned to more than one empi_id for investigation.
Calculates the percentage of enterpriseids with multiple empi_ids assigned.

## What inputs it depends on

This query reads from:

- `{{ ref('empi_crosswalk_gold') }}`

## How to run it

Because this file lives under `analyses/`, dbt will **compile** it but will not execute it automatically.

Recommended workflow:

1. Compile the analysis SQL:

```bash
dbt compile --select analyses/crosswalk_enterprise_id_checks.sql
```

2. Copy the compiled SQL from `target/compiled/.../analyses/...` into a Snowflake worksheet (or your SQL runner) and execute it.

> Tip: analyses are intended for human review and investigation. They are great for one-off QA checks and spot audits.


## How to interpret the output

This analysis returns summary statistics and/or row samples intended for human review.

A good workflow for a junior engineer:

1. Run the query after a full EMPI build (so the inputs are fresh)
2. Save the output (CSV export) for trending over time
3. If the numbers look off, trace upstream:
   - silver normalization (source mapping)
   - deterministic matching rules/macros
   - clustering logic
   - overrides logic (if applicable)

## How to maintain / extend it

- Update the `WHERE` filters and grouping columns if a source system changes naming or field semantics.
- If you want this to become an automated guardrail, consider converting it into a **dbt data test** (a singular test under `tests/`) so it can run under `dbt test` and fail CI when the invariant breaks.
  - dbt docs: https://docs.getdbt.com/docs/build/data-tests
