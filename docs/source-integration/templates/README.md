# Templates

These templates are provided to make it easy to add a new non-Athena data source to EMPI.

- `sources-caremgmt-snippet.yml` — Paste into `models/silver/_sources.yml` (or a new sources file) to declare the raw table as a dbt `source()`.
- `silver-caremgmt.sql` — Starting point for a new `models/silver/silver_<source>.sql` model that normalizes the external source into the canonical EMPI schema.
