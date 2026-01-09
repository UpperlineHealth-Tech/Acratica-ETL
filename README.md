# acratica-ulh-empi

Enterprise Master Patient Index (EMPI) project for creating a consolidated patient identity crosswalk from Medicare source demographic files and Athena EMR.

This repository contains the dbt project, models, macros, analyses and snapshot logic used to assemble a production EMPI (enterprise-wide crosswalk) by ingesting source demographic data, unioning it into a single incremental staging input, and performing deterministic matching, clustering, and downstream longitudinal flagging.

## Project overview

This project implements an end-to-end Enterprise Master Patient Index (EMPI) pipeline using dbt, designed to consolidate patient identities across multiple healthcare data sources.

At a high level:

- Raw demographic data from Medicare-derived sources (CCLF8, CCLF9, BAR) and Athena EMR are first cleaned and standardized in the `models/silver/` layer.
- Each source has its own silver model responsible for normalization and basic quality enforcement.
- After silver models are built, a `data_quality/` folder runs a suite of ephemeral data quality checks across all silver sources.
- Results from these checks are unioned into a persistent data quality summary table (`dq_summary`), providing centralized visibility into completeness, validity, and consistency issues across sources.
- The standardized silver source models are then unioned into a single incremental staging model (`silver_empi_input`), which serves as the canonical input to the matching pipeline.

From this unified input, the project executes a high-precision deterministic matching workflow:

- Strong deterministic blocking and matching rules are applied to generate candidate patient pairs both across sources and within sources, producing the `det_pairs` table.
- The `det_pairs` table captures field-level comparison results and comparison levels used for probabilistic scoring.
- On top of deterministic rule matches, Fellegi–Sunter-style probabilistic scoring is applied:
  - M probabilities are estimated from highly likely matched candidate pairs.
  - U probabilities are estimated from likely unmatched candidate pairs.
  - Field-level M/U ratios are computed and converted into log-likelihood ratios.
- These log-likelihood ratios are aggregated to produce an overall match score for each candidate pair.

Candidate pairs are then thresholded:

- Pairs above an auto-match threshold are promoted into an auto-match table and treated as inferred identity links (edges).
- Pairs slightly below the auto-match threshold are written to a clerical review table for downstream human review.
- Pairs below these thresholds are not promoted further.

These edges are then clustered into enterprise-wide patient identities:

- Matching edges flow through a series of deterministic models that construct nodes, edges, and clusters.
- Final clustering is performed using a label propagation algorithm, which consolidates connected components into stable EMPI clusters.

The output of this process is a system-generated EMPI crosswalk table, but this is not the final, authoritative crosswalk.

To support human-in-the-loop corrections, the project includes an EMPI overrides mechanism managed outside of dbt:

- An EMPI overrides table stores manual link and unlink decisions keyed by surrogate identifiers.
- Two Streamlit applications sit on top of this overrides table:
  - The **Link / Unlink UI** allows users with the appropriate role to manually link or unlink two records. These actions are written to the overrides table in an unapproved state.
  - The **EMPI Approvals UI** allows authorized approvers to review pending overrides and mark them as approved.
- The final “golden” EMPI crosswalk is exposed as a view that reads both the system-generated crosswalk and the overrides table.
- When an approved override exists for a given surrogate key, the overridden EMPI identifier takes precedence over the system-generated assignment.

### Snapshotting and calibration history

The dbt project maintains two snapshots to provide historical traceability over key EMPI artifacts:

- A snapshot over the final golden EMPI crosswalk view, capturing slowly changing history as automated matching logic or approved overrides modify EMPI assignments over time.
- A snapshot over the M and U probability calibration table, allowing visibility into how estimated M/U values and derived log-likelihood ratios evolve across calibration runs.

Together, these snapshots support auditability, model monitoring, and long-term analysis of both identity resolution outcomes and probabilistic scoring behavior.

### Additional gold-level outputs

In addition to the EMPI crosswalk, the project produces two other gold-level tables for downstream consumption:

- A **gold status updates** table that joins EMPI identities to patient-level status indicators, such as participation in external programs, enrollment states, and other longitudinal flags derived from downstream source data.
- A **golden EMPI record** table that applies survivorship logic to determine the agreed-upon patient demographics for each EMPI identity. This logic resolves conflicts across sources using defined source precedence, recency rules, and field-level survivorship policies.

These gold tables allow consumers to work with both identity-resolved linkages and curated, enterprise-approved patient attributes.

The result is a production-grade EMPI system that combines deterministic and probabilistic matching, controlled human overrides, historical traceability, and curated gold outputs suitable for analytics, reporting, and operational workflows.

## Probabilistic matching theory (Fellegi–Sunter)

This project augments high-precision deterministic matching with probabilistic record linkage based on the Fellegi–Sunter framework, as popularized in modern implementations such as Splink. This section provides the minimum theoretical background needed to understand how match scores, thresholds, and posterior probabilities are derived.

### Match vs non-match

For any pair of records, the Fellegi–Sunter model considers two competing hypotheses:

- **M (Match):** the two records refer to the same real-world person
- **U (Unmatch):** the two records refer to different people

All probabilistic scoring is framed as evidence in favor of **M** versus **U**.

---

### Comparison vectors and comparison levels

Each candidate pair is compared across multiple fields (e.g. name, date of birth, SSN-like identifiers, address).  
For each field, the comparison result falls into a **comparison level**, such as:

- Exact match
- Partial / fuzzy match
- Disagreement
- Missing on one or both sides

The full set of comparison outcomes for a pair is called its **comparison vector**.

---

### M and U probabilities

For each field and comparison level, the model estimates two probabilities:

- **M probability**  
  The probability of observing this comparison level _given that the pair is a true match_:

  $$
  m = P(\text{comparison level} \mid M)
  $$

- **U probability**  
  The probability of observing this comparison level _given that the pair is not a match_:

  $$
  u = P(\text{comparison level} \mid U)
  $$

In practice:

- **M probabilities** are estimated from pairs that are highly likely to be true matches
- **U probabilities** are estimated from pairs that are highly likely to be true non-matches

These estimates are recalibrated over time as data distributions evolve.

---

### Log-likelihood ratios (LLRs)

Each field-level comparison contributes evidence in favor of match or non-match via a **log-likelihood ratio**:

$$
\text{LLR}_i = \log \left( \frac{m_i}{u_i} \right)
$$

Where:

- $m_i$ is the M probability for field _i_
- $u_i$ is the U probability for field _i_

Interpretation:

- **Positive LLR:** evidence in favor of a match
- **Negative LLR:** evidence against a match
- **Near zero:** weak or neutral evidence

---

### Total match weight (total LLR)

Assuming conditional independence between fields, the total match weight for a candidate pair is the sum of its field-level log-likelihood ratios:

$$
W = \sum_i \log \left( \frac{m_i}{u_i} \right)
$$

This **total log-likelihood ratio** is the primary probabilistic score used by the matching pipeline.

Higher values of $W$ indicate stronger evidence that the pair is a true match.

---

### Prior match probability

To convert match weights into probabilities, the model incorporates a **prior probability of match**:

$$
P(M)
$$

This reflects the expected base rate of matches among all candidate pairs after blocking.

---

### Posterior match probability

Using Bayes’ rule, the total log-likelihood ratio and the prior are combined to compute a **posterior match probability**:

$$
P(M \mid \text{data}) =
\frac{P(M) \cdot e^{W}}
{P(M) \cdot e^{W} + (1 - P(M))}
$$

This posterior probability answers the question:

> _Given all observed comparisons, how likely is it that these two records refer to the same person?_

---

### Thresholding and decisioning

The project uses thresholds on the total match weight and/or posterior probability to route candidate pairs:

- **Auto-match threshold**  
  Pairs above this threshold are treated as matches and promoted to edges used for clustering.
- **Clerical review band**  
  Pairs just below the auto-match threshold are written to a clerical review table for human evaluation.
- **Below threshold**  
  Pairs with insufficient evidence are discarded.

These thresholds are chosen to balance precision, recall, and operational review capacity.

---

### Relationship to deterministic matching

Deterministic rules are used first to generate high-quality candidate pairs and strong blocking constraints.  
Probabilistic Fellegi–Sunter scoring is then applied **on top of these deterministic candidates** to:

- Quantify match strength
- Rank candidate links
- Support principled thresholding and review workflows

This hybrid approach combines interpretability, precision, and statistical rigor at production scale.

## Production execution patterns and dbt Cloud jobs

In production, this EMPI system is operated via a set of dbt Cloud jobs that cover environment bootstrap, CI (PR + merge validation), CD promotion, scheduled incremental updates, scheduled full recomputes, emergency override promotion, and probabilistic calibration.

These jobs are intentionally separated by responsibility to preserve cluster stability, enforce branch protection, and tightly control when probabilistic calibration is allowed to change.

---

### Environment bootstrap (initial setup)

These jobs are used when standing up a brand-new environment.

#### Overrides bootstrap (non-PROD only)

Before any downstream models depend on overrides, the EMPI overrides table must exist:

```cmd
dbt deps
dbt run-operation ensure_empi_overides
```

#### Initial full build (STG and PROD)

After dependencies are installed, the full project is built:

```cmd
dbt deps
dbt build
```

---

### CI jobs (GitHub branch protection)

These jobs support validation prior to promotion.

#### PR CI (Slim CI, STG)

Triggered on pull requests. Uses deferred state to clone incremental models and builds only modified downstream models.

```cmd
dbt deps
dbt clone --select state:modified+,config.materialized:incremental,state:old
dbt build --select state:modified+
```

#### Merge-to-main CI (STG)

Triggered on merge to main. Fully rebuilds the project while explicitly excluding probabilistic recalibration.

```cmd
dbt deps
dbt build --full-refresh --exclude tag:recal
```

#### Production deploy (CD)

Triggered after successful STG merge CI. Mirrors STG behavior in PROD.

```cmd
dbt deps
dbt build --full-refresh --exclude tag:recal
```

---

### Scheduled operational runs

#### Nightly incremental (STG and PROD)

Routine updates that preserve existing clusters.

```cmd
dbt deps
dbt build --exclude tag:full_refresh_only
```

#### Weekly full recompute (STG and PROD)

Re-evaluates all matching and clustering logic while still excluding recalibration.

```cmd
dbt deps
dbt build --full-refresh --exclude tag:recal
```

---

### Emergency / ad-hoc operational job

#### Override promote (PROD only)

Applies approved manual overrides immediately.

```cmd
dbt deps
dbt run -s empi_link_unlink_overrides_promoted
```

---

### Probabilistic calibration (M/U refresh)

These jobs intentionally allow recalibration of Fellegi–Sunter M and U probabilities.

#### STG probabilistic update (scheduled)

```cmd
dbt deps
dbt build --full-refresh
```

#### PROD probabilistic update (manual)

```cmd
dbt deps
dbt build --full-refresh
```

---

### dbt Cloud job summary

| Job Name            | Environment | Trigger             | Purpose                                                      | Commands                                                                                                                             |
| ------------------- | ----------- | ------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| STG-OVERRIDES-SETUP | STG         | Initial             | Ensure EMPI overrides infrastructure exists                  | `dbt deps`<br>`dbt run-operation ensure_empi_overides`                                                                               |
| STG-INITIAL-SETUP   | STG         | Initial             | Initial full build of staging environment                    | `dbt deps`<br>`dbt build`                                                                                                            |
| PROD-INITIAL-SETUP  | PROD        | Initial             | Initial full build of production environment                 | `dbt deps`<br>`dbt build`                                                                                                            |
| STG-PR-CI           | STG         | Pull Request        | Slim CI using deferred state to validate modified models     | `dbt deps`<br>`dbt clone --select state:modified+,config.materialized:incremental,state:old`<br>`dbt build --select state:modified+` |
| STG-MERGE-CI        | STG         | Merge to main       | Full rebuild without recalibrating probabilities             | `dbt deps`<br>`dbt build --full-refresh --exclude tag:recal`                                                                         |
| PROD-DEPLOY-CD      | PROD        | STG merge success   | Promote validated logic to production                        | `dbt deps`<br>`dbt build --full-refresh --exclude tag:recal`                                                                         |
| STG-NIGHTLY-INC     | STG         | Nightly             | Incremental EMPI maintenance                                 | `dbt deps`<br>`dbt build --exclude tag:full_refresh_only`                                                                            |
| PROD-NIGHTLY-INC    | PROD        | Nightly             | Incremental EMPI maintenance                                 | `dbt deps`<br>`dbt build --exclude tag:full_refresh_only`                                                                            |
| STG-WEEKLY-FULL     | STG         | Weekly              | Full recompute of matching and clustering (no recalibration) | `dbt deps`<br>`dbt build --full-refresh --exclude tag:recal`                                                                         |
| PROD-WEEKLY-FULL    | PROD        | Weekly              | Full recompute of matching and clustering (no recalibration) | `dbt deps`<br>`dbt build --full-refresh --exclude tag:recal`                                                                         |
| OVERRIDE-PROMOTE    | PROD        | Ad-hoc              | Apply approved manual link/unlink overrides                  | `dbt deps`<br>`dbt run -s empi_link_unlink_overrides_promoted`                                                                       |
| STG-PROB-UPDATE     | STG         | Scheduled (Monthly) | Recalibrate Fellegi–Sunter M/U probabilities                 | `dbt deps`<br>`dbt build --full-refresh`                                                                                             |
| PROD-PROB-UPDATE    | PROD        | Manual              | Recalibrate Fellegi–Sunter M/U probabilities in production   | `dbt deps`<br>`dbt build --full-refresh`                                                                                             |

## Development setup (dbt Cloud Studio)

Development work is performed in dbt Cloud Studio using a dedicated development environment that mirrors the project structure and runtime behavior used in staging/production.

### First-time setup (per developer environment)

Before running models, install dependencies and ensure required EMPI support objects exist (including the overrides table):

```cmd
dbt deps
dbt run-operation ensure_empi_overides
```

### Build the project in your dev environment

Once dependencies are installed and overrides infrastructure is present, build the project:

```cmd
dbt build
```

### Working in dbt Cloud Studio

In dbt Cloud Studio, you will have a working copy of the repository directory structure (models, macros, analyses, snapshots, etc.). Use the Studio file tree to navigate the project and iterate on models and macros.

Typical dev workflow:

1. Update models/macros in dbt Cloud Studio.
2. Run `dbt build` (or selectively build targeted models) to validate changes.
3. Open a PR to trigger PR CI (Slim CI) and follow the promotion flow through staging and production.

## Analyses and monitoring

- The `analyses/` folder contains queries that compute EMPI coverage, linkage statistics between sources, pair/cluster diagnostics, and sampling queries for review. Use these for monitoring, QA and reporting.

## Edge cases and operational guidance

- Deletions and splits: deletions in source systems are not handled by merge-only upserts — periodic full refreshes are required.
- False positives/over-linking: check thresholding in deterministic rules and review sampled `det_pairs` / analysis outputs.
- High-churn sources: if a source adds many records each run, increase frequency of full recomputes or improve pre-match filters.

## Testing & validation

- Use the queries in `analyses/` to validate linkage rates and coverage.
- Add or run unit/expectation tests in your dbt tests to assert key invariants (e.g., unique source ids, not null critical fields).

## Contact / authors

See repository maintainers and project owners for onboard and operational questions.

---

This README is intended for operators and data engineers running or maintaining the EMPI dbt project. If you'd like additional runbooks, CI steps, or a one-page architecture diagram added, open an issue or create a PR with a suggested doc.
