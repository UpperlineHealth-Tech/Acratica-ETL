# QA Analysis Query: det_pairs_rule_sampling

**Source file:** `analyses/det_pairs_rule_sampling.sql`  
**GitHub permalink:** https://github.com/UpperlineHealth-Tech/Acratica-ETL/blob/1a2168e26e17e6bf7338c1a66df79cf384927491/analyses/det_pairs_rule_sampling.sql?plain=1#L1-L136

## What this QA query is for

This analysis is to help draw random sample pairs from each deterministic matching rule
for manual review and labeling.  There are four auto-scorcing rules (1-4) that have been verified.  
Use this to autoscore the sample for definite matches (rule #1) and then quickly check rules 2-4, which should 
all be matches.  Then, manually score #5.  This allows for calculation of precision by rule in conjunction
with prevalence from the num_pairs_by_rule analysis.

## What inputs it depends on

This query reads from:

- `{{ ref('det_pairs') }}`

## How to run it

Because this file lives under `analyses/`, dbt will **compile** it but will not execute it automatically.

Recommended workflow:

1. Compile the analysis SQL:

```bash
dbt compile --select analyses/det_pairs_rule_sampling.sql
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
