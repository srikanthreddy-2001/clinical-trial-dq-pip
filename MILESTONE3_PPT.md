# Milestone 3 PPT — 2–3 Slides (03-Nov-2026 deliverable)

Clinical Trial Data Integration & Quality Platform — Week 3 (28 Oct – 3 Nov 2026).
Slide-by-slide content, ready to paste into PowerPoint / Google Slides. All claims
are backed by live evidence in the repo and workspace (see notes under each slide).

---

## Slide 1 — Commitments vs Delivery (Week 3)

**Commitment: deliver the Week-3 tasks on schedule with evidence. Delivery: 4/4 non-PPT tasks DONE, all verified live.**

| Task (Excel Week 3) | Status | Evidence |
| --- | --- | --- |
| 28-Oct: benchmark one transformation + Spark plan review | ✅ DONE | `notebooks/06_clinical_trial_silver_benchmark.ipynb` — Silver subjects transform: median **0.95 s** (0.92–1.16 s), plan fully Photon-supported (PhotonScan → PhotonProject → PhotonGroupingAgg(first)), guardrails held (267 valid / 5 quarantined on every run) |
| 29-Oct: apply one safe optimization | ✅ DONE | Replaced 2 post-MERGE `count()` actions with MERGE Delta-history metrics (4 cell actions → 2); **before/after 11.65 s vs 12.24 s (within noise — speedup not required)**; Gold KPI fingerprints byte-identical before/after (output correctness proven) |
| 30-Oct: Git repo + automated tests, ≥7 tests incl. negative | ✅ DONE (substitution) | GitHub repo `clinical-trial-dq-pip` + GitHub Actions CI (green, runs on every push) — **GitLab not available in this workspace**, GitHub used instead; **18 automated tests** (transforms, DQ gate, bronze idempotency, KPI values, negative tests incl. missing-column failure); single repeatable command `python run_tests.py` |
| 02-Nov: rule-based DQ summaries, ≥3 cases, traceable | ✅ DONE | `DQ Summaries and Review Package` notebook — 3 plain-language cases (duplicate subject_ids; blank site_id quarantine; 29-visit orphan cascade), every metric traceable to a live query, method labeled **100% rule-based (no LLM)** |
| 03-Nov: Milestone 3 PPT | ✅ THIS DECK | — |

---

## Slide 2 — Technical Evidence & Demo

**What was built: a rerun-safe Bronze → Silver → Gold pipeline with a 7-check DQ gate, 5 KPIs, and proof at every layer.**

* **Bronze** — append-only raw Delta (never MERGE, never schema change); incremental ingest via run-log watermark + null-safe anti-join → reruns never duplicate.
* **Silver** — typed + deduplicated; quarantines bad rows instead of dropping: 280 subjects → **267 valid + 5 quarantined**; 1492 visits → **1463 valid + 29 orphan-quarantined**; **DQ gate DQ-01…DQ-07: 7/7 PASS** before Gold ever runs.
* **Gold** — 3 study KPIs + 2 site KPIs (`study_kpi_summary` 6 rows, `site_kpi_summary` 26 rows), each value re-derived by **independent correlated-subquery SQL** (0 mismatches) and pinned by **SHA fingerprints** (`ea64730bc6effd53` / `3ddfe6e7889e09e6`).
* **Orchestration** — single route, every stage logged (Bronze 1804 → Silver Success → Gold 32); nightly job green.
* **Quality proof** — 18/18 pytest tests pass locally **and on a clean CI runner** (GitHub Actions), including negative tests that assert the pipeline *fails correctly*.

**Demo:** `notebooks/05_fresh_end_to_end_run` (clean run from fresh inputs, fingerprints match production) and `notebooks/07_negative_tests_and_contingency` (duplicate input / missing field / interrupted stage + recoveries).

---

## Slide 3 — Blockers & Learnings

**Blockers (resolved or worked around):**

* GitLab not available in the workspace → used **GitHub + GitHub Actions CI** instead (scope preserved: versioned repo, automated tests, green CI).
* Serverless compute limits → no `dbutils.notebook.run` (orchestration runs stages in-process) and pytest must run in-process from a `/tmp` copy (`python run_tests.py` works locally and in CI).

**Learnings:**

* **Negative testing pays off:** Week-4 negative tests found 2 real defects — DEF-01 (Critical): CSVs with a missing column were silently misaligned at ingest → now rejected with a clear header validation error; DEF-02 (Medium): empty-bronze reruns skipped ingest → now always re-read. Both fixed, pinned by tests (suite 14 → 18, all green).
* **Design for idempotency from day one** (append-only bronze, MERGE silver): interrupted stages recover by simply rerunning — no doubling, no manual repair.
* **Prove correctness independently:** independent-SQL recomputation + byte-level fingerprints catch silent logic drift that row counts cannot.

---

*Sources: repo `clinical-trial-dq-pip` (main branch), notebooks 01–07, RUNBOOK.md, OPERATIONS.md, NEGATIVE_TESTS.md, KPI_DICTIONARY.md, DQ Summaries and Review Package notebook, GitHub Actions run history. All numbers verified live 2–5 Nov 2026.*
