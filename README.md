# clinical-trial-dq-pip

Clinical Trial Data Integration & Quality Platform — a medallion
(Bronze → Silver → Gold) pipeline over synthetic clinical-trial data
(studies, sites, subjects, visits) with automated data-quality checks, KPI
summaries, unit tests and CI.

## Repository layout

```
clinical-trial-dq-pip/
├── notebooks/        # thin wrappers: 01_bronze, 02_silver, 03_gold, 04_orchestrate
│                     # Week-4 validation: 05_fresh_end_to_end_run, 07_negative_tests_and_contingency
├── src/              # transforms.py, dq_checks.py (pure PySpark functions)
├── tests/            # test_transforms.py, test_dq_checks.py, test_bronze_validation.py (+ conftest.py)
├── data/             # synthetic CSVs (studies, sites, subjects, visits)
├── RUNBOOK.md        # operational guide: run E2E, rerun safety, DQ gate, troubleshooting
├── OPERATIONS.md     # Week-4 operations notes: run, monitor, recover
├── NEGATIVE_TESTS.md # Week-4 negative-test scenarios + defect log
├── KPI_DICTIONARY.md # KPI formulas, grain, validation methods
├── .github/workflows/tests.yml   # GitHub Actions: pytest on every push
├── requirements.txt
└── README.md
```

## Architecture

* **Bronze** (`notebooks/01_bronze`): incremental, append-only CSV → Delta load
  into `workspace.bronze.*`. No MERGE and no schema changes in the raw layer; a
  run-log watermark plus a null-safe anti-join make reruns idempotent. Bronze
  keeps ALL raw rows, including the planted duplicate subject_ids.
* **Silver** (`notebooks/02_silver`): renames (Phase→phase, Sponsor→sponsor,
  Country→country, City→city, Sex→sex), casts dates to DATE and numerics to
  INT, deduplicates subjects by subject_id, and quarantines rows with blank
  site_id (`silver.subjects_quarantine`) and orphan visits
  (`silver.visits_quarantine`). Ends with the DQ-01..DQ-07 gate.
* **Gold** (`notebooks/03_gold`): `gold.study_kpi_summary` and
  `gold.site_kpi_summary`, validated row-by-row against independent SQL.
* **Orchestration** (`notebooks/04_orchestrate`): single route
  Bronze → Silver → Gold in one run, with per-stage logging to
  `workspace.gold.pipeline_run_log`.

All heavy lifting lives in `src/` as pure PySpark functions; the notebooks
only wire paths, catalog upserts and logging.

## KPIs

| KPI | Definition |
| --- | --- |
| Enrollment Rate | enrolled subjects / target enrollment (study & site level) |
| Visit Completion Rate | completed visits / total visits |
| Visit Data Completeness | visits with actual_date / total visits |

Full formulas, grain, and validation methods: **[KPI_DICTIONARY.md](KPI_DICTIONARY.md)**.

## Data quality checks

DQ-01 no duplicate subject_ids · DQ-02 non-blank site_id · DQ-03 non-null
visit/subject ids · DQ-04 non-null study keys/dates · DQ-05 subjects→studies
referential integrity · DQ-06 non-null site keys/dates · DQ-07 every visit has
a subject.

## Running the tests

```bash
pip install -r requirements.txt
python run_tests.py          # or: pytest tests/ -v
```

Note: locally and in CI these commands create a local Spark session. Inside
the Databricks workspace, run the suite in-process from a notebook
(serverless shells do not expose the Spark Connect channel).

18 tests cover the transforms, quarantine splits, bronze idempotency, KPI
values, the DQ gate and bronze input validation — including negative tests
(missing-column failure, tampered-KPI detection, duplicate/blank-site/orphan
DQ failures, and CSV headers that are missing / reordering / adding columns
vs the mandatory business schema).

## CI

`.github/workflows/tests.yml` runs the whole pytest suite on every push
(and pull request) on ubuntu-latest with Java 17 + Python 3.11.

## Week 4 — integration, negative tests & fixes (04–06 Nov 2026)

* **Clean integrated execution (04-Nov):** `notebooks/05_fresh_end_to_end_run`
  runs the complete route from fresh synthetic inputs staged in the separate
  `workspace.landing.fresh_inputs` volume into isolated `fresh_*` schemas —
  Bronze 1804 → DQ 7/7 → Silver 1796 → Gold 32 KPI rows, byte-identical to the
  production Gold fingerprints, with every stage logged.
* **Negative tests & contingency (05-Nov):** `notebooks/07_negative_tests_and_contingency`
  executes duplicate input, missing mandatory field and interrupted-stage
  scenarios (plus recovery reruns) in the isolated `neg_*` schemas with inputs in
  the separate `workspace.landing.negative_tests` volume. Two defects were found
  and fixed:
  * **DEF-01 (Critical):** a CSV missing a mandatory column was silently ingested
    with positionally misaligned data — fixed with header validation in
    `read_csv_all_strings` (pinned by `tests/test_bronze_validation.py`);
  * **DEF-02 (Medium):** the bronze watermark skipped re-ingestion when bronze
    was empty — fixed by skipping only when the bronze table also has data
    (`01_bronze`, `04_orchestrate`).
* **Fix & document (06-Nov):** regression 18/18 tests passed (local + CI), and
  this documentation set finalized: `OPERATIONS.md`, `NEGATIVE_TESTS.md`,
  `KPI_DICTIONARY.md`.

## Operations

See **[RUNBOOK.md](RUNBOOK.md)** for how to run the pipeline end-to-end,
rerun-safety guarantees, expected row counts, the DQ gate, quarantine
tables, and troubleshooting — and **[OPERATIONS.md](OPERATIONS.md)** for
monitoring queries, failure handling and reviewer rerun guidance.

## Data

`data/` holds the four synthetic CSVs; the planted DQ issues are 8 duplicate
subject_id groups, 5 blank site_id rows and 10 null actual_dates (pending
visits — valid by design).

