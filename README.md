# clinical-trial-dq-pip

Clinical Trial Data Integration & Quality Platform — a medallion
(Bronze → Silver → Gold) pipeline over synthetic clinical-trial data
(studies, sites, subjects, visits) with automated data-quality checks, KPI
summaries, unit tests and CI.

## Repository layout

```
clinical-trial-dq-pip/
├── notebooks/        # thin wrappers: 01_bronze, 02_silver, 03_gold, 04_orchestrate
├── src/              # transforms.py, dq_checks.py (pure PySpark functions)
├── tests/            # test_transforms.py, test_dq_checks.py (+ conftest.py)
├── data/             # synthetic CSVs (studies, sites, subjects, visits)
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

14 tests cover the transforms, quarantine splits, bronze idempotency, KPI
values and the DQ gate — including negative tests (missing-column failure,
tampered-KPI detection, duplicate/blank-site/orphan DQ failures).

## CI

`.github/workflows/tests.yml` runs the whole pytest suite on every push
(and pull request) on ubuntu-latest with Java 17 + Python 3.11.

## Data

`data/` holds the four synthetic CSVs; the planted DQ issues are 8 duplicate
subject_id groups, 5 blank site_id rows and 10 null actual_dates (pending
visits — valid by design).

