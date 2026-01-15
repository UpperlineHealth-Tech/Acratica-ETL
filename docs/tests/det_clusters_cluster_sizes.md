# QA Analysis Query: det_clusters_cluster_sizes

**Source file:** `analyses/det_clusters_cluster_sizes.sql`  
**GitHub permalink:** https://github.com/UpperlineHealth-Tech/Acratica-ETL/blob/1a2168e26e17e6bf7338c1a66df79cf384927491/analyses/det_clusters_cluster_sizes.sql?plain=1#L1-L6

## What this QA query is for

This analysis is a QA/monitoring query used to inspect EMPI behavior and data quality.

## What inputs it depends on

This query reads from:

- `{{ ref('det_clusters') }}`

## How to run it

Because this file lives under `analyses/`, dbt will **compile** it but will not execute it automatically.

Recommended workflow:

1. Compile the analysis SQL:

```bash
dbt compile --select analyses/det_clusters_cluster_sizes.sql
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
