# Operations Notes — Clinical Trial Data Integration & Quality Platform

Week 4 (06-Nov-2026) deliverable: how to run, monitor and recover the pipeline.
See also `RUNBOOK.md` (step-by-step), `KPI_DICTIONARY.md` (metric definitions) and
`NEGATIVE_TESTS.md` (defect log).

## 1. Data locations

| Layer | Location | Notes |
| --- | --- | --- |
| Landing (baseline inputs) | `/Volumes/workspace/landing/raw_files/` | studies / sites / subjects / visits CSVs — the production source |
| Landing (fresh run inputs) | `/Volumes/workspace/landing/fresh_inputs/` | separate volume for Week-4 clean-run validation; baseline untouched |
| Landing (negative tests) | `/Volumes/workspace/landing/negative_tests/` | one folder per scenario; separate from baseline |
| Bronze | `workspace.bronze.*` | append-only raw Delta; no MERGE, no schema changes; keeps duplicates by design |
| Silver | `workspace.silver.*` | typed, deduped, quarantined (`subjects_quarantine`, `visits_quarantine`); DQ-gated |
| Gold | `workspace.gold.study_kpi_summary / site_kpi_summary` | 3 study KPIs + 2 site KPIs (see KPI_DICTIONARY.md) |
| Run log | `workspace.gold.pipeline_run_log` | run_id, stage_name, start/end time, status, records_processed, error_message |
| Isolated validation envs | `workspace.fresh_*`, `workspace.neg_*`, `workspace.freshtest`, `workspace.negtest` | Week-4 validation only; safe to drop and rebuild via notebooks 05 / 07 |

## 2. How to run

* **Production route (single orchestration):** notebook `notebooks/04_orchestrate`
  — Bronze → Silver → Gold in one run, every stage logged to the run log. The
  nightly Lakeflow job executes the same route.
* **Per-stage:** `notebooks/01_bronze`, `02_silver`, `03_gold` (thin wrappers over
  `src/transforms.py` + `src/dq_checks.py`).
* **Clean end-to-end validation:** `notebooks/05_fresh_end_to_end_run` — stages
  fresh inputs into the separate `fresh_inputs` volume, resets the isolated
  `fresh_*` schemas, runs the full route and verifies the KPI tables against the
  production fingerprints + independent recomputation.
* **Negative tests:** `notebooks/07_negative_tests_and_contingency` — duplicate
  input, missing mandatory field, interrupted stage (+ recovery reruns), logging
  to `workspace.negtest.pipeline_run_log`.
* **Unit tests / CI:** `python run_tests.py` locally or in CI (GitHub Actions runs
  `pytest tests/` on every push — 18 tests). In a Databricks notebook REPL, run
  pytest in-process on a /tmp copy of the repo with `--import-mode=importlib`.

## 3. Monitoring

```sql
-- Last 10 stage executions
SELECT run_id, stage_name, status, records_processed, error_message, end_time
FROM workspace.gold.pipeline_run_log ORDER BY end_time DESC LIMIT 10;

-- Any failed stages (should be empty in steady state)
SELECT * FROM workspace.gold.pipeline_run_log WHERE status = 'Failed';

-- Quarantine health (expected steady values for the current dataset:
-- 5 quarantined subjects, 29 quarantined orphan visits)
SELECT (SELECT COUNT(*) FROM workspace.silver.subjects_quarantine) AS subjects_q,
       (SELECT COUNT(*) FROM workspace.silver.visits_quarantine)    AS visits_q;
```

Expected steady-state records per clean run: Bronze **1804** (6/26/280/1492),
Silver DQ **7/7 PASS**, Silver upserts **1796**, Gold **32** KPI rows
(6 study + 26 site). Deviations from these values on unchanged inputs indicate
a defect or out-of-band change — investigate before accepting the run.

## 4. Failure handling & recovery (verified by negative testing, Nov 5)

* **Bad input file (e.g. missing/reordered column):** Bronze fails immediately
  with a clear `ValueError` naming the file and the problem (DEF-01 fix). Fix the
  file, rerun — already-ingested datasets are not re-appended (anti-join idempotency).
* **Interrupted stage:** the run log records the failed stage; gold is never built
  from partial silver. A plain rerun recovers: bronze skips unchanged files whose
  tables still have data, and Silver MERGE re-upserts without doubling
  (verified: subjects 267 after recovery, not 534).
* **Out-of-band bronze reset/truncate:** the DEF-02 hardening forces bronze to
  re-read files even when they are older than the watermark if the table is
  empty, so a rerun rebuilds correctly instead of silently producing empty
  gold tables.
* **DQ gate failure:** Silver raises before Gold runs; inspect the DQ summary in
  the stage output and the quarantine tables, fix the upstream data, rerun.

## 5. Rerun guidance for reviewers

1. `notebooks/05_fresh_end_to_end_run` top-to-bottom — expect "CLEAN RUN
   CONFIRMED", DQ 7/7, both KPI fingerprints `MATCH`.
2. `notebooks/07_negative_tests_and_contingency` top-to-bottom — expect
   S1/S2/S3 verdicts PASS, S2 Bronze failure with `missing=['site_id']`, and
   both recovery reruns green.
3. `python run_tests.py` (or CI) — expect 18 tests passed.
