# KPI Dictionary — Clinical Trial Data Integration & Quality Platform

Week 4 (06-Nov-2026) deliverable: definitions, formulas, grain, sources and
validation method for every pipeline KPI. All KPIs are computed in
`src/transforms.py` (`build_study_kpi_summary`, `build_site_kpi_summary`) and
materialized in `workspace.gold`.

## Study-level KPIs — table `workspace.gold.study_kpi_summary` (one row per study)

| KPI | Column | Formula | Inputs (silver) | Notes |
| --- | --- | --- | --- | --- |
| KPI-1 Enrollment rate | `enrollment_rate` | `enrolled_subjects / target_enrollment` (rounded to 4 dp) | `subjects.subject_id` (distinct, valid only) ÷ `studies.target_enrollment` | Quarantined subjects are excluded; counts are post-dedup |
| KPI-2 Visit completion rate | `visit_completion_rate` | `completed_visits / total_visits` (0.0 when no visits) | `visits.visit_status = 'Completed'` ÷ all valid visits | Orphan visits (quarantined) excluded |
| KPI-3 Visit data completeness | `visit_data_completeness` | `visits_with_actual_date / total_visits` (0.0 when no visits) | `visits.actual_date IS NOT NULL` ÷ all valid visits | Measures how much visit data has an actual (not just scheduled) date |

Supporting component columns on the same table: `enrolled_subjects`,
`total_visits`, `completed_visits`, `visits_with_actual_date`.

## Site-level KPIs — table `workspace.gold.site_kpi_summary` (one row per site)

| KPI | Column | Formula | Inputs (silver) | Notes |
| --- | --- | --- | --- | --- |
| Site enrollment rate | `site_enrollment_rate` | `enrolled_subjects / site_target_enrollment` (4 dp) | distinct valid subjects at (site_id, study_id) ÷ `sites.site_target_enrollment` | Grain is site × study |
| Site visit completion rate | `site_visit_completion_rate` | `completed_visits / total_visits` (0.0 when no visits) | completed valid visits ÷ all valid visits at (site_id, study_id) | Same 3-KPI logic at site grain |

## Definitions & conventions

* **Valid subject**: passes the Silver gate — non-blank `site_id`, deduplicated by
  `subject_id` (first occurrence wins); invalid rows go to
  `silver.subjects_quarantine`.
* **Valid visit**: `subject_id` exists among valid Silver subjects; orphan visits go
  to `silver.visits_quarantine` (`quarantine_reason = 'subject_id not in silver.subjects'`).
* **Rounding**: all rates are `ROUND(x, 4)`.
* **Zero-divide**: rates are 0.0 (not NULL) when the denominator is 0.
* **Load timestamps**: `silver_load_timestamp` / `gold_load_timestamp` mark the
  layer's processing time; they are excluded from KPI comparisons (fingerprints).

## Validation (how the KPIs are proven correct)

1. **Independent recomputation** — `independent_kpi_queries(silver_schema)` in
   `src/transforms.py` re-derives each KPI with correlated subqueries (deliberately
   different query style) and is compared against the stored Gold values; 0
   mismatches = PASS (run live in notebook `05_fresh_end_to_end_run`, Cell 5).
2. **Byte-level fingerprint** — Gold tables (minus load timestamps) are sorted and
   hashed; the fresh Week-4 clean run reproduced the production fingerprints
   exactly: `study_kpi_summary = ea64730bc6effd53`,
   `site_kpi_summary = 3ddfe6e7889e09e6` (shared columns).
3. **Unit tests** — `tests/test_transforms.py` pins exact KPI values on a fixture
   dataset (enrollment, completion, completeness, site rates, tamper detection).

## Known-good values (synthetic dataset of 14 Oct 2026)

Bronze 1804 raw rows (studies 6, sites 26, subjects 280, visits 1492) → Silver
267 valid + 5 quarantined subjects, 1463 valid + 29 quarantined visits → Gold
6 study rows + 26 site rows. Study-level examples: STUDY-003 enrollment rate
0.4952, visit completion 0.5051, data completeness 0.9932.
