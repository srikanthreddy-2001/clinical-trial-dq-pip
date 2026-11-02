# RUNBOOK — clinical-trial-dq-pip

Operational guide: how to run the pipeline end-to-end, rerun it safely, run the
DQ gate and tests, and interpret the quarantine tables.

## 1. Prerequisites

* Databricks workspace with Unity Catalog; tables live in catalog `workspace`
  (`workspace.bronze.*`, `workspace.silver.*`, `workspace.gold.*`).
* Raw CSVs in `data/` (synthetic clinical-trial data with planted DQ issues:
  8 duplicate subject_id groups, 5 blank site_id rows, 10 null actual_dates
  which are valid pending visits).
* Notebooks are thin wrappers around `src/` (pure PySpark), so any cluster with
  Spark + the repo on the path works.

## 2. Run the pipeline end-to-end (single route)

Run **`notebooks/04_orchestrate`** — it executes Bronze → Silver → Gold in one
run and logs each stage to `workspace.gold.pipeline_run_log`.

Equivalent manual order (only if debugging a single stage):

1. `notebooks/01_bronze` — incremental, append-only CSV → Delta into
   `workspace.bronze.*` (watermark + null-safe anti-join make reruns
   idempotent; no MERGE, no schema changes).
2. `notebooks/02_silver` — rename/cast, dedup subjects, quarantine bad rows,
   run DQ gate DQ-01..DQ-07 (must end **7/7 PASS**).
3. `notebooks/03_gold` — rebuild `gold.study_kpi_summary` and
   `gold.site_kpi_summary`, validated against independent SQL.

A daily scheduled job runs this same route; check **Jobs & Pipelines →
"Clinical Trial Medallion Pipeline"** and the run-log table before rerunning
manually.

## 3. Rerun safety & expected counts

Reruns are safe at every stage. After a fresh full run with the baseline
`data/` CSVs the reconciliation must hold:

* subjects: bronze 280 = silver 267 valid + 5 quarantined + 8 duplicate rows
  dropped
* visits: bronze 1492 = silver 1463 valid + 29 orphan visits quarantined
* DQ gate: 7/7 PASS, 0 violations

If bronze counts grow, only *new* rows are processed (watermark); the
reconciliation identity (bronze = valid + quarantine + dups) must still hold.

## 4. Data quality gate & quarantine

* Gate: `src/dq_checks.py::run_dq_checks` — DQ-01 duplicates, DQ-02 blank
  site_id, DQ-03/04/06 mandatory keys, DQ-05 subjects→studies, DQ-07
  visits→subjects. Any FAIL must stop promotion to Gold.
* Quarantine (rows are preserved with context, never deleted):
  * `workspace.silver.subjects_quarantine` — subjects with NULL/blank site_id
  * `workspace.silver.visits_quarantine` — visits whose subject_id is not in
    `silver.subjects` (`quarantine_reason` column states why; currently all
    29 rows cascade from the 5 quarantined subjects)
* Live evidence with traceable queries: notebook
  `clinical-trial-dq-pip/DQ Summaries and Review Package` (run top-to-bottom).

## 5. Tests

```bash
pip install -r requirements.txt
python run_tests.py          # single repeatable command (wraps pytest tests/ -v)
```

* 14 tests: transforms, quarantine splits, bronze idempotency, KPI values,
  DQ gate — including negative tests (missing-column failure,
  tampered-KPI detection).
* Expected result: **14 passed**, exit code 0.
* In the Databricks workspace run the suite in-process from a notebook
  (serverless shells do not expose the Spark Connect channel); the review
  package notebook's test cell does exactly this.
* CI: `.github/workflows/tests.yml` runs the suite on every push/PR.

## 6. Integration scenarios (E2E)

Notebook `Clinical Trial/Clinical Trial Integration Tests` covers 3 end-to-end
scenarios (fresh load, rerun idempotency, DQ-issue handling). All must PASS
before a release/demo. Rerun it after any change to `src/` or the notebooks.

## 7. Troubleshooting

| Symptom | Check |
| --- | --- |
| DQ gate FAIL after a data change | Which check failed, then inspect the matching quarantine table — bad rows are routed there, not dropped |
| Bronze counts unchanged after rerun | Expected: watermark means only new rows load; verify `pipeline_run_log` stage statuses |
| `pytest` errors with `INVALID_CONNECT_URL` | You are outside a Spark session — use `python run_tests.py` locally/CI, or run in-process in a notebook |
| `pytest` fails writing `__pycache__` on /Workspace | Run from a local copy (run_tests.py sets `sys.dont_write_bytecode`; the review notebook copies the repo to /tmp) |
| Gold KPI mismatch | `03_gold` validates against independent SQL; a mismatch usually means Silver changed without rerunning Gold |
