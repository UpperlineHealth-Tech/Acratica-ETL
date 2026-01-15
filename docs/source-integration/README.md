# Source Integration (adding a non-Athena data source)

This guide documents **how to add a new data source that is _not_ in Athena** to the EMPI project so that:

1. The source is normalized into the canonical EMPI schema (`models/silver/`)
2. The source participates in deterministic matching + clustering (`models/det_matching/`)
3. The source receives an `EMPI_ID` in the final crosswalk (`models/gold/empi_crosswalk_gold.sql`)
4. (Optional) The source can “win” **field-level survivorship** in the golden record (`models/gold/golden_empi_record.sql`) when it is more authoritative than existing sources

> **Important vocabulary**
>
> - **Athena source data**: Any data that makes it into Athena EMR and is then loaded/normalized through the ULH ETL process.
> - **Non-Athena source**: A source that is not loaded into Athena, but is still available to dbt in the warehouse (for this repo, Snowflake SQL is assumed based on functions like `TRY_TO_DATE`, `REGEXP_LIKE`, `TO_VARCHAR`, etc.).
> - **Silver layer**: Normalized source-level tables (`models/silver/*`) with consistent column names + types.
> - **EMPI input**: `models/silver/silver_empi_input.sql` — the unioned table of person-like records that the matching layer consumes.
> - **Crosswalk**: `models/gold/empi_crosswalk_gold.sql` — the mapping of `(SOURCE_SYSTEM, SOURCE_SYSTEM_ID)` → `EMPI_ID`.

---

## Overview: where a new source plugs in

A new non-Athena source becomes part of EMPI in **three concrete steps**:

1. **Expose raw data to dbt** using a dbt `source()` definition in `models/silver/_sources.yml`
2. **Normalize** the source into the EMPI canonical shape in a new silver model (`models/silver/silver_<source>.sql`)
3. **Union** the normalized rows into `models/silver/silver_empi_input.sql`

After that, the existing deterministic matching + clustering will include your new source automatically (as long as you populate the standard fields the rules depend on: name, DOB, sex, address, phone, email, etc.).

---

## Step-by-step: add a new non-Athena source

The example below uses a hypothetical Care Management feed (“CAREMGMT”). The same steps apply to any other non-Athena source.

### Step 1 — Create/identify the raw table in the warehouse

You need a raw table or view that dbt can query, for example:

- `CAREMGMT.MEMBER_DEMOGRAPHICS`

This table should contain identifiers and person demographics such as name, DOB, sex, address, and contact fields.

> If the raw table is delivered as a file, land it into the warehouse (or create an external table/view) **before** doing the dbt work.

### Step 2 — Declare the raw table as a dbt `source()`

Update `models/silver/_sources.yml` and add a source block for your new raw table.

A paste-ready snippet is in:

- [`docs/source-integration/templates/sources-caremgmt-snippet.yml`](templates/sources-caremgmt-snippet.yml)

### Step 3 — Create a new silver normalization model

Create:

- `models/silver/silver_caremgmt.sql`

Start from the template:

- [`docs/source-integration/templates/silver-caremgmt.sql`](templates/silver-caremgmt.sql)

**Your goal:** output the canonical EMPI columns used across the project, including:

- `SOURCE_SYSTEM`
- `SOURCE_SYSTEM_ID` and `SOURCE_SYSTEM_ID_TYPE`
- name fields: `FIRST_NAME`, `LAST_NAME`, soundex/initials (if you can)
- demographics: `DOB`, `BIRTH_YEAR`, `SEX`
- address + contact: `ADDRESS_LINE_1`, `CITY`, `STATE`, `ZIP_5`, `PHONE`, `EMAIL`
- dates: `LOAD_DATE`, `LAST_UPDATED`

> Tip: mirror patterns from `models/silver/silver_patient.sql` (Athena) and `models/silver/silver_bar.sql` (Medicare BAR) for casing, trimming, and date parsing.

### Step 4 — Add the source to the EMPI input union

Open:

- `models/silver/silver_empi_input.sql`

Add a new `UNION ALL` block that selects the canonical columns from `ref('silver_caremgmt')` (or whatever your model is named).

**Why this matters:** deterministic matching only sees what is present in `silver_empi_input`.

### Step 5 — Run the minimal build to validate

Run:

```bash
# build your new silver model and everything downstream that depends on EMPI input
dbt build --select +silver_caremgmt +silver_empi_input +empi_crosswalk_gold
```

If you want to validate the golden record logic too:

```bash
dbt build --select +golden_empi_record
```

---

## Optional: make the new source authoritative for specific fields (survivorship)

The golden record model:

- `models/gold/golden_empi_record.sql`

computes **field-level ranks** like `phone_rank`, `email_rank`, and `address_rank`. Some ranks explicitly prefer an existing source (for example Athena for phone/email).

### When should a new source “win” survivorship?

A practical example:

- **CAREMGMT** records come from a care navigation team that calls patients weekly.
- Athena’s phone/email are often stale (e.g., patients don’t update the EMR record).
- For outreach, the care management phone/email is more reliable.

In this scenario, CAREMGMT should be allowed to win **PHONE** and **EMAIL** survivorship.

### How to implement (concrete)

1) Add a new priority rank in `golden_empi_record.sql` specifically for contact fields:

```sql
CASE
  WHEN SOURCE_SYSTEM = 'CAREMGMT' THEN 1
  WHEN SOURCE_SYSTEM = 'ATHENA'   THEN 2
  WHEN SOURCE_SYSTEM = 'BAR'      THEN 3
  ELSE 99
END AS contact_priority_rank
```

2) Update the phone/email ranking windows to use that rank:

```sql
-- Phone: Completeness -> Recency -> Source priority
ROW_NUMBER() OVER (
  PARTITION BY EMPI_ID
  ORDER BY
    CASE WHEN LENGTH(REGEXP_REPLACE(PHONE, '[^0-9]', '')) = 10 THEN 0 ELSE 1 END,
    LOAD_DATE DESC,
    contact_priority_rank
) AS phone_rank
```

and similarly for `email_rank`.

3) Rebuild the golden record:

```bash
dbt build --select golden_empi_record
```

4) Validate with a targeted query: confirm that when both sources exist, CAREMGMT phone/email are selected.

---

## Deidentified example: new source alongside Athena in the EMPI crosswalk

See:

- [`sample-data/athena-source-empi-input-sample.csv`](sample-data/athena-source-empi-input-sample.csv)
- [`sample-data/caremgmt-source-empi-input-sample.csv`](sample-data/caremgmt-source-empi-input-sample.csv)
- [`sample-data/empi-crosswalk-gold-sample.csv`](sample-data/empi-crosswalk-gold-sample.csv)

The final crosswalk sample demonstrates that ATHENA patient `1001` and CAREMGMT member `CM-9001` receive the same `EMPI_ID` when the matching rules link them.
