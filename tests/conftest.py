"""Shared pytest fixtures: a Spark session and small synthetic datasets.

The fixtures mirror the real synthetic clinical-trial data -- including the
planted DQ issues (duplicate subject_id, blank site_id, orphan visits) -- but
are tiny so the whole suite runs in seconds. Bronze fixtures are all-STRING,
exactly as the raw landing layer stores them.
"""

import os
import sys

# Make the repo root importable (src.*) no matter how pytest was invoked
# (workspace, `python run_tests.py`, or CI `pytest tests/`).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from pyspark.sql import SparkSession

STUDIES_DDL = ("study_id string, study_name string, therapeutic_area string, "
               "Phase string, Sponsor string, start_date string, "
               "planned_end_date string, target_enrollment string, study_status string")
SITES_DDL = ("site_id string, study_id string, site_name string, Country string, "
             "City string, principal_investigator string, activation_date string, "
             "site_target_enrollment string, site_status string")
SUBJECTS_DDL = ("subject_id string, study_id string, site_id string, "
                "screening_date string, enrollment_date string, birth_year string, "
                "Sex string, treatment_arm string, subject_status string")
VISITS_DDL = ("visit_id string, subject_id string, study_id string, site_id string, "
              "visit_number string, visit_name string, scheduled_date string, "
              "actual_date string, visit_status string")

SILVER_TS = "2026-10-30T00:00:00+00:00"


@pytest.fixture(scope="session")
def spark():
    # getOrCreate() reuses the notebook's session in Databricks and creates a
    # local session in CI; never stop it (that would kill the REPL session).
    return SparkSession.builder.appName("clinical-trial-dq-pip-tests").getOrCreate()


@pytest.fixture
def bronze_studies(spark):
    return spark.createDataFrame([
        ("STUDY-001", "Oncology Trial 1", "Oncology", "Phase III", "Pfizer",
         "2026-01-01", "2027-01-01", "100", "Active"),
        ("STUDY-002", "Cardiology Trial 2", "Cardiology", "Phase II", "Novartis",
         "2026-02-01", "2027-06-01", "80", "Active"),
    ], STUDIES_DDL)


@pytest.fixture
def bronze_sites(spark):
    return spark.createDataFrame([
        ("SITE-001", "STUDY-001", "Boston Site", "USA", "Boston",
         "Dr. Alpha", "2026-01-05", "40", "Active"),
        ("SITE-002", "STUDY-001", "Chicago Site", "USA", "Chicago",
         "Dr. Beta", "2026-01-10", "60", "Active"),
        ("SITE-003", "STUDY-002", "Berlin Site", "Germany", "Berlin",
         "Dr. Gamma", "2026-02-10", "30", "Active"),
    ], SITES_DDL)


@pytest.fixture
def bronze_subjects(spark):
    """5 raw rows: 1 planted duplicate pair, 1 blank site_id (-> quarantine)."""
    return spark.createDataFrame([
        ("SUB-001", "STUDY-001", "SITE-001", "2026-01-10", "2026-01-15",
         "1985", "M", "Treatment", "Enrolled"),
        ("SUB-002", "STUDY-001", "SITE-002", "2026-01-12", "2026-01-18",
         "1990", "F", "Control", "Enrolled"),
        # planted duplicate subject_id (identical values)
        ("SUB-002", "STUDY-001", "SITE-002", "2026-01-12", "2026-01-18",
         "1990", "F", "Control", "Enrolled"),
        ("SUB-003", "STUDY-002", "SITE-003", "2026-02-15", "2026-02-20",
         "1978", "F", "Treatment", "Enrolled"),
        # planted blank site_id -> must be quarantined in Silver
        ("SUB-004", "STUDY-002", "   ", "2026-02-16", None,
         "1982", "M", "Control", "Screening"),
    ], SUBJECTS_DDL)


@pytest.fixture
def bronze_visits(spark):
    """4 raw rows: 1 pending visit (null actual_date is valid), 1 orphan subject."""
    return spark.createDataFrame([
        ("VIS-001", "SUB-001", "STUDY-001", "SITE-001", "1", "Screening",
         "2026-01-20", "2026-01-20", "Completed"),
        ("VIS-002", "SUB-001", "STUDY-001", "SITE-001", "2", "Week 4",
         "2026-02-17", None, "Scheduled"),
        ("VIS-003", "SUB-002", "STUDY-001", "SITE-002", "1", "Screening",
         "2026-01-25", "2026-01-25", "Completed"),
        # orphan: subject_id not among the valid subjects -> quarantined
        ("VIS-004", "SUB-999", "STUDY-001", "SITE-001", "1", "Screening",
         "2026-01-26", "2026-01-26", "Completed"),
    ], VISITS_DDL)


@pytest.fixture
def silver_tables(bronze_studies, bronze_sites, bronze_subjects, bronze_visits):
    """Clean Silver tables built through the REAL src transforms."""
    from src.transforms import (
        transform_studies, transform_sites, transform_subjects, transform_visits,
    )

    studies = transform_studies(bronze_studies, SILVER_TS)
    sites = transform_sites(bronze_sites, SILVER_TS)
    subjects, subjects_quarantine = transform_subjects(bronze_subjects, SILVER_TS)
    visits, visits_quarantine = transform_visits(bronze_visits, subjects, SILVER_TS)
    return {
        "studies": studies,
        "sites": sites,
        "subjects": subjects,
        "visits": visits,
        "subjects_quarantine": subjects_quarantine,
        "visits_quarantine": visits_quarantine,
    }
