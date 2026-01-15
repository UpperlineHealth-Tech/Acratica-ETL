# Tests and QA documentation

This directory documents the **inline tests / QA checks** that exist in this repository.

In this repo, “tests” are implemented in two main ways:

1. **Data quality check models** under `models/data_quality/` — dbt models that output **summary counts** (for example: `null` / `valid` / `invalid`) for key demographic fields.

2. **Analysis QA queries** under `analyses/` — SQL files that are compiled by dbt (so they can use `ref()` / `source()`), but are **run manually** for investigation and spot checks.

> Note: dbt also supports automated tests that run under `dbt test`. In this repo, those tests live under `tests/` and in model/source `*.yml` files that define schema tests (for example: `not_null`, `unique`, `accepted_values`, and relationship checks). A dbt data test passes when its SQL returns **0 failing rows**.

---

## When to use which approach

- If a stakeholder asks **“how do we know the EMPI is behaving?”**, start with the QA analyses.
- If an engineer is troubleshooting **match-rate changes** or **data formatting issues**, start with the data quality checks for the relevant source system.
- If you want automated enforcement of stable expectations (for example: required IDs, uniqueness, referential integrity), implement or extend **dbt tests** and run them with `dbt test`.

---

## Automated dbt tests (`dbt test`)

In addition to the inline QA models and investigation queries, this repository includes a set of **automated dbt data tests** under the repo-root `tests/` directory.

### Data-quality tests (`tests/data_quality/`)

The files in `tests/data_quality/` are **singular tests**: standalone SQL files executed by `dbt test`. Each test is written as an assertion that returns the *rows you do not want to exist*. A test passes when its SQL returns **0 rows**, and it warns/errors when it returns **one or more rows**.

In practice, these tests are “inline checks” expressed in a `dbt test`-friendly form:

- **Null-rate guardrails** (`dq_profile`)  
  Return a summary row when a key demographic field’s null rate crosses a threshold (for example, a spike in missing address/city values that likely indicates an ingestion or mapping issue).

- **Invalid-format conformance checks** (`dq_invalid`)  
  Return a summary row when a field’s invalid rate crosses a threshold (for example, malformed email, phone length not equal to 10, ZIP code length not equal to 5, MBI length not equal to 11, etc.).

When a test *does* return a row, the row includes a compact “why it failed” payload (for example: `table_name`, `column_name`, `quality_check`, counts, and percentages such as `pct_null` or `pct_invalid`). This makes failures immediately actionable in logs and dashboards.

> **Where to look:**  
> - Executable test SQL: `tests/data_quality/*.sql`  
> - Human-readable documentation for each check: the **Master index** below (links into `docs/tests/*.md`)

### How the tests are tagged

All data-quality tests share the tag `dq`, and are further tagged by intent and source system:

- `dq_profile` — missingness/null-rate guardrails
- `dq_invalid` — invalid-format/conformance checks
- Source-system tags such as `athena`, `bar`, `cclf8` (run one source’s checks in isolation)

Most data-quality tests are configured with `severity='warn'` so they surface drift without blocking orchestration. If/when a check becomes a hard contract, you can promote it to `severity='error'`.

### Running tests

Run all automated tests:

```bash
dbt test
```

Run all data-quality tests:

```bash
dbt test --select tag:dq
```

Run only invalid-format conformance tests:

```bash
dbt test --select tag:dq_invalid
```

Run only null-rate / profiling tests:

```bash
dbt test --select tag:dq_profile
```

Run data-quality tests for a single source system (example: ATHENA):

```bash
dbt test --select "tag:dq,tag:athena"
```

Run only the tests in the `tests/data_quality/` directory (path-based selection):

```bash
dbt test --select "path:tests/data_quality"
```

Store failing rows in the warehouse for investigation (optional):

```bash
dbt test --store-failures
```

### Tuning thresholds

Some tests use project variables (for example: `dq_invalid_pct_warn_threshold`). You can override variables for a one-off run:

```bash
dbt test --select tag:dq_invalid --vars '{dq_invalid_pct_warn_threshold: 1}'
```

---

## Master index

### Data quality checks (models/`models/data_quality/`)

#### ATHENA

- [`dq_silver_patient_address_1`](tests/dq_silver_patient_address_1.md) — Checks `silver_patient.ADDRESS_LINE_1`; rule: Null check of address line 1
- [`dq_silver_patient_address_2`](tests/dq_silver_patient_address_2.md) — Checks `silver_patient.ADDRESS_LINE_2`; rule: Null check of address line 2
- [`dq_silver_patient_birth_year`](tests/dq_silver_patient_birth_year.md) — Checks `silver_patient.BIRTH_YEAR`; rule: Length of BIRTH_YEAR is 4
- [`dq_silver_patient_city`](tests/dq_silver_patient_city.md) — Checks `silver_patient.CITY`; rule: Null check of city
- [`dq_silver_patient_death_date`](tests/dq_silver_patient_death_date.md) — Checks `silver_patient.DEATH_DATE`; rule: DEATH_DATE format is YYYY-MM-DD
- [`dq_silver_patient_dob`](tests/dq_silver_patient_dob.md) — Checks `silver_patient.DOB`; rule: DOB format is YYYY-MM-DD
- [`dq_silver_patient_email`](tests/dq_silver_patient_email.md) — Checks `silver_patient.EMAIL`; rule: EMAIL format is valid
- [`dq_silver_patient_first_name`](tests/dq_silver_patient_first_name.md) — Checks `silver_patient.FIRST_NAME`; rule: Null check of first name
- [`dq_silver_patient_last_name`](tests/dq_silver_patient_last_name.md) — Checks `silver_patient.LAST_NAME`; rule: Null check of last name
- [`dq_silver_patient_phone`](tests/dq_silver_patient_phone.md) — Checks `silver_patient.PHONE`; rule: Length of PHONE is 10
- [`dq_silver_patient_ssid`](tests/dq_silver_patient_ssid.md) — Checks `silver_patient.SOURCE_SYSTEM_ID`; rule: Null check of SOURCE_SYSTEM_ID
- [`dq_silver_patient_ssid_2`](tests/dq_silver_patient_ssid_2.md) — Checks `silver_patient.SOURCE_SYSTEM_ID_2`; rule: Null check of SOURCE_SYSTEM_ID_2
- [`dq_silver_patient_ssn`](tests/dq_silver_patient_ssn.md) — Checks `silver_patient.SSN`; rule: Length of SSN is 9
- [`dq_silver_patient_state`](tests/dq_silver_patient_state.md) — Checks `silver_patient.STATE`; rule: Length of STATE is 2
- [`dq_silver_patient_zip_4`](tests/dq_silver_patient_zip_4.md) — Checks `silver_patient.ZIP_4`; rule: Length of 4-digit zip code length is 4
- [`dq_silver_patient_zip_5`](tests/dq_silver_patient_zip_5.md) — Checks `silver_patient.ZIP_5`; rule: Length of 5-digit zip code length is 5

#### BAR

- [`dq_silver_bar_address_1`](tests/dq_silver_bar_address_1.md) — Checks `silver_bar.ADDRESS_LINE_1`; rule: Null check of address line 1
- [`dq_silver_bar_address_2`](tests/dq_silver_bar_address_2.md) — Checks `silver_bar.ADDRESS_LINE_2`; rule: Null check of address line 2
- [`dq_silver_bar_birth_year`](tests/dq_silver_bar_birth_year.md) — Checks `silver_bar.BIRTH_YEAR`; rule: Length of BIRTH_YEAR is 4
- [`dq_silver_bar_city`](tests/dq_silver_bar_city.md) — Checks `silver_bar.CITY`; rule: Null check of city
- [`dq_silver_bar_death_date`](tests/dq_silver_bar_death_date.md) — Checks `silver_bar.DEATH_DATE`; rule: DEATH_DATE format is YYYY-MM-DD
- [`dq_silver_bar_dob`](tests/dq_silver_bar_dob.md) — Checks `silver_bar.DOB`; rule: DOB format is YYYY-MM-DD
- [`dq_silver_bar_first_name`](tests/dq_silver_bar_first_name.md) — Checks `silver_bar.FIRST_NAME`; rule: Null check of first name
- [`dq_silver_bar_last_name`](tests/dq_silver_bar_last_name.md) — Checks `silver_bar.LAST_NAME`; rule: Null check of last name
- [`dq_silver_bar_prev_ssid`](tests/dq_silver_bar_prev_ssid.md) — Checks `silver_bar.PREV_SOURCE_SYSTEM_ID`; rule: Length of previous MBI length is 11
- [`dq_silver_bar_ssid`](tests/dq_silver_bar_ssid.md) — Checks `silver_bar.SOURCE_SYSTEM_ID`; rule: Length of MBI length is 11
- [`dq_silver_bar_state`](tests/dq_silver_bar_state.md) — Checks `silver_bar.STATE`; rule: Length of STATE is 2
- [`dq_silver_bar_zip_4`](tests/dq_silver_bar_zip_4.md) — Checks `silver_bar.ZIP_4`; rule: Length of 4-digit zip code length is 4
- [`dq_silver_bar_zip_5`](tests/dq_silver_bar_zip_5.md) — Checks `silver_bar.ZIP_5`; rule: Length of 5-digit zip code length is 5

#### CCLF8

- [`dq_silver_cclf8_address_1`](tests/dq_silver_cclf8_address_1.md) — Checks `silver_cclf8.ADDRESS_LINE_1`; rule: Null check of address line 1
- [`dq_silver_cclf8_address_2`](tests/dq_silver_cclf8_address_2.md) — Checks `silver_cclf8.ADDRESS_LINE_2`; rule: Null check of address line 2
- [`dq_silver_cclf8_birth_year`](tests/dq_silver_cclf8_birth_year.md) — Checks `silver_cclf8.BIRTH_YEAR`; rule: Length of BIRTH_YEAR is 4
- [`dq_silver_cclf8_city`](tests/dq_silver_cclf8_city.md) — Checks `silver_cclf8.CITY`; rule: Null check of city
- [`dq_silver_cclf8_death_date`](tests/dq_silver_cclf8_death_date.md) — Checks `silver_cclf8.DEATH_DATE`; rule: DEATH_DATE format is YYYY-MM-DD
- [`dq_silver_cclf8_dob`](tests/dq_silver_cclf8_dob.md) — Checks `silver_cclf8.DOB`; rule: DOB format is YYYY-MM-DD
- [`dq_silver_cclf8_first_name`](tests/dq_silver_cclf8_first_name.md) — Checks `silver_cclf8.FIRST_NAME`; rule: Null check of first name
- [`dq_silver_cclf8_last_name`](tests/dq_silver_cclf8_last_name.md) — Checks `silver_cclf8.LAST_NAME`; rule: Null check of last name
- [`dq_silver_cclf8_prev_ssid`](tests/dq_silver_cclf8_prev_ssid.md) — Checks `silver_cclf8.PREV_SOURCE_SYSTEM_ID`; rule: Length of previous MBI length is 11
- [`dq_silver_cclf8_ssid`](tests/dq_silver_cclf8_ssid.md) — Checks `silver_cclf8.SOURCE_SYSTEM_ID`; rule: Length of MBI length is 11
- [`dq_silver_cclf8_state`](tests/dq_silver_cclf8_state.md) — Checks `silver_cclf8.STATE`; rule: Length of STATE is 2
- [`dq_silver_cclf8_zip_4`](tests/dq_silver_cclf8_zip_4.md) — Checks `silver_cclf8.ZIP_4`; rule: Length of 4-digit zip code length is 4
- [`dq_silver_cclf8_zip_5`](tests/dq_silver_cclf8_zip_5.md) — Checks `silver_cclf8.ZIP_5`; rule: Length of 5-digit zip code length is 5

#### DATA_QUALITY

- [`dq_summary`](tests/dq_summary.md)

### QA analyses (analyses/`analyses/`)

- [`athena_duplication_analysis`](tests/athena_duplication_analysis.md) — This script is to check for records created in athena that were created on the same day,
- [`crosswalk_empi_id_checks`](tests/crosswalk_empi_id_checks.md) — Checking the empi_id from crosswalk to see how many empi_ids comprise two or more distinct enterpriseids from Athena.
- [`crosswalk_enterprise_id_checks`](tests/crosswalk_enterprise_id_checks.md) — Checking the enterpriseid from athena in the crosswalk table (tests/aliased as source_system_id_2) to see if any enterprise IDs
- [`det_clusters_cluster_sizes`](tests/det_clusters_cluster_sizes.md)
- [`det_clusters_num_clusters`](tests/det_clusters_num_clusters.md)
- [`det_empi_crosswalk_pct_bar_to_athena_coverage`](tests/det_empi_crosswalk_pct_bar_to_athena_coverage.md)
- [`det_empi_crosswalk_pct_reduction`](tests/det_empi_crosswalk_pct_reduction.md)
- [`det_pairs_num_pairs_by_rule`](tests/det_pairs_num_pairs_by_rule.md)
- [`det_pairs_rule_sampling`](tests/det_pairs_rule_sampling.md) — This analysis is to help draw random sample pairs from each deterministic matching rule
- [`points_relative_frequency`](tests/points_relative_frequency.md)
- [`qa_checks_silver_status_updates`](tests/qa_checks_silver_status_updates.md)
- [`qa_terminated_deceased_after`](tests/qa_terminated_deceased_after.md)
- [`weights_exact_null_else`](tests/weights_exact_null_else.md)

---

## How to use these docs

Each linked page includes:

- a GitHub permalink to the exact code being documented (with line numbers)
- how to run the check
- how to interpret results
- how to safely extend/maintain it
