"""Unit tests for src.dq_checks (the Silver DQ gate).

Positive test: all 7 checks pass on clean Silver tables.
Negative tests: planted data issues (duplicate subject_ids, blank site_id,
orphan visits) must FAIL the corresponding check, never pass silently.
"""

from pyspark.sql import functions as F

from src.dq_checks import all_passed, run_dq_checks, summarize


def _by_id(checks):
    return {c["check_id"]: c for c in checks}


def _run(silver, **overrides):
    return run_dq_checks(
        subjects=overrides.get("subjects", silver["subjects"]),
        visits=overrides.get("visits", silver["visits"]),
        studies=overrides.get("studies", silver["studies"]),
        sites=overrides.get("sites", silver["sites"]),
    )


def test_all_seven_checks_pass_on_clean_silver(silver_tables):
    checks = _run(silver_tables)
    assert len(checks) == 7
    assert all(c["violations"] == 0 for c in checks)
    assert all_passed(checks)
    assert "7/7 passed" in summarize(checks)


def test_dq_detects_duplicate_subject_ids(silver_tables):
    """NEGATIVE: a duplicated subject row must FAIL DQ-01 with the right count."""
    dirty = silver_tables["subjects"].unionByName(
        silver_tables["subjects"].filter("subject_id = 'SUB-001'")
    )
    checks = _run(silver_tables, subjects=dirty)
    dq01 = _by_id(checks)["DQ-01"]
    assert dq01["status"] == "FAIL"
    assert dq01["violations"] == 1
    assert not all_passed(checks)
    assert "[FAIL] DQ-01" in summarize(checks)


def test_dq_detects_blank_site_id(silver_tables):
    """NEGATIVE: a blank site_id must FAIL DQ-02 (the quarantine contract)."""
    dirty = silver_tables["subjects"].withColumn(
        "site_id",
        F.when(F.col("subject_id") == "SUB-001", F.lit("   ")).otherwise(F.col("site_id")),
    )
    checks = _run(silver_tables, subjects=dirty)
    dq02 = _by_id(checks)["DQ-02"]
    assert dq02["status"] == "FAIL"
    assert dq02["violations"] == 1


def test_dq_detects_referential_violation(silver_tables):
    """NEGATIVE: an orphan visit must FAIL DQ-07."""
    dirty = silver_tables["visits"].unionByName(
        silver_tables["visits_quarantine"].drop("quarantine_reason")
    )
    checks = _run(silver_tables, visits=dirty)
    dq07 = _by_id(checks)["DQ-07"]
    assert dq07["status"] == "FAIL"
    assert dq07["violations"] == 1
