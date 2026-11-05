# Negative Tests & Contingency Log — Week 4 (04–06 Nov 2026)

Deliverable for the 05-Nov task (*test duplicate input, missing mandatory field
and interrupted stage; prioritize defects*) and the 06-Nov task (*resolve
critical defects, rerun regression*). Executable evidence: notebook
`notebooks/07_negative_tests_and_contingency` (run log:
`workspace.negtest.pipeline_run_log`); unit-level negative tests in
`tests/` (18 tests, all passing).

All negative inputs live in the **separate UC volume**
`/Volumes/workspace/landing/negative_tests/` (one folder per scenario) and run
in isolated `workspace.neg_*` schemas — the baseline data and production
schemas are never touched, so clean-vs-negative results are directly comparable.

## Scenario results (expected vs observed)

| # | Scenario | Expected | Observed (live runs, 05-Nov-2026) | Verdict |
| --- | --- | --- | --- | --- |
| 1 | Duplicate input: `subjects.csv` with 12 exact duplicate rows (292 data rows) | Bronze append-only keeps all 292 raw rows; Silver dedupes to 267 valid + 5 quarantined; DQ 7/7; Gold KPIs unchanged (32 rows) | Bronze subjects = 292 (records 1816), Silver 267 + 5 quarantine, DQ 7/7 PASS, Gold 32 rows, Silver stage records 1796 = clean baseline | PASS — by design, no defect |
| 2 | Missing mandatory field: `subjects.csv` with the `site_id` column removed | Read must fail loudly, naming the missing column, BEFORE ingesting misaligned data | **Pre-fix:** Bronze logged "Success" with 280 silently misaligned rows (Spark's explicit schema maps positionally; birth_year landed in enrollment_date); the pipeline only failed in Silver with `DateTimeException: Text '1982' could not be parsed` and `silver.subjects` stayed empty. **Post-fix:** Bronze FAILS immediately with `ValueError: ... missing=['site_id'] ...` (studies 6 + sites 26 already processed are kept consistent, subjects 0 rows, visits never created) | DEF-01 (Critical) — found and FIXED |
| 2R | Scenario 2 recovery (correct `subjects.csv` restored, rerun) | Full pipeline recovers; no duplicate ingestion of already-loaded datasets | Bronze appended 1772 (subjects 280 + visits 1492; studies/sites correctly SKIPped as unchanged with data), DQ 7/7, Silver 1796, Gold 32 | PASS — idempotent recovery |
| 3 | Interrupted stage: exception injected mid-Silver right after studies+sites upsert | Silver logs Failed; partial Silver (studies + sites only); Gold never runs | Run log shows `Silver_Transformation: Failed` with the injected RuntimeError; silver.studies = 6, silver.sites = 26, silver.subjects missing; Gold never executed | PASS — clean abort as designed |
| 3R | Scenario 3 recovery (plain rerun) | Bronze SKIPs unchanged files (data intact); Silver MERGE re-upserts without doubling; DQ 7/7; Gold rebuilt | Bronze records = 0 (all SKIP, tables have data); Silver studies still 6 (not 12), subjects 267 (not 534), quarantine 5, visits 1463; DQ 7/7 PASS; Gold 32 rows | PASS — MERGE idempotency, no defect |

## Defect log

| ID | Severity | Finding | Root cause | Fix (all committed) | Status |
| --- | --- | --- | --- | --- | --- |
| DEF-01 | **Critical** | CSV missing a mandatory column was silently ingested with positionally misaligned data; failure only surfaced in Silver as a confusing date-parse error | `read_csv_all_strings` used an explicit Spark schema, and Spark's CSV reader maps columns positionally, ignoring header names | Header validation added to `read_csv_all_strings` (`src/transforms.py`): rejects any file whose header ≠ expected business columns (missing / extra / reordered), with an actionable error message. Pinned by 4 new tests in `tests/test_bronze_validation.py` | FIXED + regression-verified |
| DEF-02 | Medium | After an out-of-band reset of the bronze tables, a rerun "succeeded" with 0 records and silently rebuilt empty Silver/Gold tables (observed live: Bronze records=0, silver/gold all 0 rows despite SKIP statuses) | The bronze watermark skipped files older than the last successful run without checking whether the bronze table still contained data | Skip condition now also requires `spark.table(bronze.table).count() > 0`; an empty bronze table always re-reads. Applied to `notebooks/01_bronze`, `notebooks/04_orchestrate` and both validation harnesses | FIXED |
| — | — | Scenario 1 (duplicates) and Scenario 3 (interrupted stage + recovery) behaved exactly as designed | n/a | n/a | No defect |

Prior related fix (Oct 30, for completeness): tz-naive watermark comparison raised
`TypeError: can't compare offset-naive and offset-aware datetimes` — fixed in
`01_bronze` + `04_orchestrate` before Week 4.

## Regression evidence (06-Nov)

* Full pytest suite after the fixes: **18 passed, exit code 0** (14 existing + 4
  new DEF-01 tests), run in-process on a /tmp copy of the repo with
  `--import-mode=importlib`; the same suite runs on GitHub Actions CI.
* Pipeline-level regression: the Week-4 fresh clean run
  (`notebooks/05_fresh_end_to_end_run`) re-executed after the DEF-01 fix and
  reproduced the production Gold fingerprints exactly
  (`study_kpi_summary = ea64730bc6effd53`, `site_kpi_summary = 3ddfe6e7889e09e6`
  on shared columns) with DQ 7/7 — the fix does not false-reject valid files.

**Result: 3 negative scenarios executed (plus 2 recovery reruns), 2 defects
found, both assigned and addressed; zero known critical defects remain.**
