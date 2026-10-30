"""Unit tests for src.transforms (pure PySpark functions).

Includes negative tests: a transform on a DataFrame missing a required
column must fail loudly, and the KPI-mismatch validator must actually
detect a tampered value.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType

from conftest import SILVER_TS
from src.transforms import (
    BUSINESS_COLUMNS,
    build_site_kpi_summary,
    build_study_kpi_summary,
    count_kpi_mismatches,
    new_rows_only,
    transform_sites,
    transform_studies,
    transform_subjects,
    transform_visits,
)


def _types(df):
    return {f.name: f.dataType for f in df.schema.fields}


# ------------------------------------------------------------- renames / casts

def test_transform_studies_renames_and_casts(bronze_studies):
    out = transform_studies(bronze_studies, SILVER_TS)
    types = _types(out)
    assert "phase" in types and "sponsor" in types
    assert "Phase" not in types and "Sponsor" not in types
    assert isinstance(types["start_date"], DateType)
    assert isinstance(types["planned_end_date"], DateType)
    assert isinstance(types["target_enrollment"], IntegerType)
    rows = {r["study_id"]: r for r in out.collect()}
    assert rows["STUDY-001"]["phase"] == "Phase III"
    assert rows["STUDY-001"]["sponsor"] == "Pfizer"
    assert rows["STUDY-001"]["target_enrollment"] == 100


def test_transform_sites_renames_and_casts(bronze_sites):
    out = transform_sites(bronze_sites, SILVER_TS)
    types = _types(out)
    assert "country" in types and "city" in types
    assert "Country" not in types and "City" not in types
    assert isinstance(types["activation_date"], DateType)
    assert isinstance(types["site_target_enrollment"], IntegerType)
    assert out.count() == 3


# ------------------------------------------------------- dedup / quarantine

def test_subjects_dedup_and_quarantine_split(bronze_subjects):
    valid, quarantine = transform_subjects(bronze_subjects, SILVER_TS)
    assert valid.count() == 3  # 5 raw rows - 1 duplicate - 1 quarantined
    assert quarantine.count() == 1
    assert quarantine.collect()[0]["subject_id"] == "SUB-004"
    ids = sorted(r["subject_id"] for r in valid.collect())
    assert ids == ["SUB-001", "SUB-002", "SUB-003"]  # no duplicate survives


def test_visits_orphan_subjects_quarantined(bronze_visits, bronze_subjects):
    valid_subjects, _ = transform_subjects(bronze_subjects, SILVER_TS)
    valid, quarantine = transform_visits(bronze_visits, valid_subjects, SILVER_TS)
    assert valid.count() == 3  # the pending (null actual_date) visit is VALID
    assert quarantine.count() == 1
    q = quarantine.collect()[0]
    assert q["visit_id"] == "VIS-004"
    assert q["quarantine_reason"] == "subject_id not in silver.subjects"


# ------------------------------------------------------- bronze idempotency

def test_new_rows_only_is_idempotent(bronze_subjects):
    cols = BUSINESS_COLUMNS["subjects"]
    # the same data is already in bronze -> nothing new to append
    assert new_rows_only(bronze_subjects, bronze_subjects, cols).count() == 0


def test_new_rows_only_is_null_safe_and_finds_new_rows(spark, bronze_subjects):
    cols = BUSINESS_COLUMNS["subjects"]
    # rows identical including a NULL value on both sides -> NOT new
    existing = spark.createDataFrame(
        [("SUB-003", "STUDY-002", None, "2026-02-15", "2026-02-20",
          "1978", "F", "Treatment", "Enrolled")], bronze_subjects.schema
    )
    assert new_rows_only(existing, existing, cols).count() == 0
    # a genuinely new subject IS detected as new
    extra = spark.createDataFrame(
        [("SUB-777", "STUDY-002", "SITE-003", "2026-03-01", "2026-03-02",
          "1980", "F", "Control", "Enrolled")], bronze_subjects.schema
    )
    assert new_rows_only(extra, bronze_subjects, cols).count() == 1


# ------------------------------------------------------------- gold KPIs

def test_study_kpi_summary_values(silver_tables):
    out = build_study_kpi_summary(
        silver_tables["studies"], silver_tables["subjects"], silver_tables["visits"]
    )
    rows = {r["study_id"]: r for r in out.collect()}
    s1 = rows["STUDY-001"]
    assert s1["enrolled_subjects"] == 2
    assert s1["total_visits"] == 3  # VIS-001, VIS-002, VIS-003 (VIS-004 is quarantined)
    assert s1["completed_visits"] == 2  # VIS-001, VIS-003 (VIS-002 is pending)
    assert s1["visits_with_actual_date"] == 2
    assert s1["enrollment_rate"] == 0.02  # 2 / 100
    assert s1["visit_completion_rate"] == 0.6667  # 2 / 3
    assert s1["visit_data_completeness"] == 0.6667  # 2 / 3
    s2 = rows["STUDY-002"]
    assert s2["enrolled_subjects"] == 1  # SUB-004 was quarantined
    assert s2["total_visits"] == 0
    assert s2["enrollment_rate"] == 0.0125  # 1 / 80
    assert s2["visit_completion_rate"] == 0.0  # zero visits -> 0.0 by design


def test_site_kpi_summary_values(silver_tables):
    out = build_site_kpi_summary(
        silver_tables["sites"], silver_tables["subjects"], silver_tables["visits"]
    )
    rows = {r["site_id"]: r for r in out.collect()}
    assert out.count() == 3
    assert rows["SITE-001"]["site_enrollment_rate"] == 0.025  # 1/40
    assert rows["SITE-001"]["site_visit_completion_rate"] == 0.5  # 1/2
    assert rows["SITE-002"]["site_visit_completion_rate"] == 1.0
    assert rows["SITE-003"]["site_enrollment_rate"] == 0.0333  # 1/30 (SUB-003)
    assert rows["SITE-003"]["site_visit_completion_rate"] == 0.0  # no visits at all


def test_count_kpi_mismatches_detects_tampering(silver_tables):
    gold = build_study_kpi_summary(
        silver_tables["studies"], silver_tables["subjects"], silver_tables["visits"]
    )
    recomputed = build_study_kpi_summary(
        silver_tables["studies"], silver_tables["subjects"], silver_tables["visits"]
    )
    # identical recomputation -> 0 mismatches
    assert count_kpi_mismatches(gold, recomputed, "study_id", "enrollment_rate") == 0
    # NEGATIVE: a tampered KPI value must be detected (validator is not a no-op)
    tampered = gold.withColumn("enrollment_rate", F.lit(0.9999))
    assert count_kpi_mismatches(gold, tampered, "study_id", "enrollment_rate") == 2


# ------------------------------------------------------------- negative test

def test_transform_studies_fails_on_missing_required_column(bronze_studies):
    """NEGATIVE: input missing the Phase column must fail loudly, never
    silently drop the column or return a half-transformed table."""
    broken = bronze_studies.drop("Phase")
    with pytest.raises(Exception):
        transform_studies(broken, SILVER_TS).collect()
