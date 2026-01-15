# Data Quality Check: dq_silver_cclf8_dob (CCLF8)

**Source file:** `models/data_quality/CCLF8/dq_silver_cclf8_dob.sql`  
**GitHub permalink:** https://github.com/UpperlineHealth-Tech/Acratica-ETL/blob/1a2168e26e17e6bf7338c1a66df79cf384927491/models/data_quality/CCLF8/dq_silver_cclf8_dob.sql?plain=1#L1-L29

## What this check does

This check produces a **single-row summary** of data quality for the `DOB` field in the silver model:

- `{{ ref('silver_cclf8') }}`

It reports:

- how many rows have `DOB` missing (`null_count`)
- how many rows appear valid (`valid_count`)
- how many rows appear invalid (`invalid_count`)
- percentages like `pct_null` and `pct_invalid`

**Why we have it:** EMPI matching quality depends on clean, standardized demographics. If a key field like `DOB` is frequently null/invalid, match rates and downstream outreach quality can degrade.

## The rule being enforced (human-readable)

The configured check description in the SQL is:

> **DOB format is YYYY-MM-DD**

The core “validity” logic is implemented here: https://github.com/UpperlineHealth-Tech/Acratica-ETL/blob/1a2168e26e17e6bf7338c1a66df79cf384927491/models/data_quality/CCLF8/dq_silver_cclf8_dob.sql?plain=1#L5-L12

```sql
, case
    when REGEXP_LIKE(TO_VARCHAR(DOB), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') then 1
    else 0
end as is_valid
```

## How to run it

This is implemented as a dbt model in `models/data_quality/`, so you can run it like any other model:

```bash
dbt build --select dq_silver_cclf8_dob
```

If you want to run the **entire data quality suite**:

```bash
dbt build --select dq_summary
```

## How to interpret the output

This model returns one row with columns like:

- `row_count`: total rows evaluated
- `null_count`: number of rows where `DOB` is missing
- `invalid_count`: number of rows where `DOB` is present but fails the validity rule
- `pct_null`: percent of all rows missing the value
- `pct_invalid`: percent of *non-null* rows that are invalid

### What a “bad” result looks like

- High `pct_null` means the source system isn’t supplying the field, or the silver model mapping is incomplete.
- High `pct_invalid` means values are present but malformed (for example, wrong format or unexpected characters).

## How to maintain / extend it

Common maintenance tasks:

- **Improve standardization upstream** in the relevant silver model (`silver_cclf8`) if formatting is inconsistent.
- **Tighten or loosen the validity rule** (for example, update a regex) if the source data changes.
- **Add an additional dimension** (like `SOURCE_SYSTEM`) if you want the same check to support multi-source comparisons.

If you extend this check, also ensure it stays referenced by `models/data_quality/dq_summary.sql`, which unions all data-quality checks into a single table for review.
