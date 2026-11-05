"""Negative tests for Bronze ingestion (DEF-01, found 05-Nov-2026 by
pipeline-level negative testing, see NEGATIVE_TESTS.md).

A landing CSV whose header does not contain every mandatory business column
in the expected order must be REJECTED at read time. With an explicit Spark
schema the CSV reader maps columns POSITIONALLY, so before the DEF-01 fix a
subjects.csv missing site_id was ingested silently misaligned (birth_year
landed in enrollment_date) and only failed later in Silver with a confusing
date-parse error. These tests pin the fixed behavior.
"""

import pytest

from src.transforms import BUSINESS_COLUMNS, read_csv_all_strings

SUBJECT_COLS = BUSINESS_COLUMNS["subjects"]
VALID_HEADER = ",".join(SUBJECT_COLS)
VALID_ROW = (
    "SUB-777,STUDY-001,SITE-001,2026-03-01,2026-03-02,"
    "1985,F,Control,Enrolled"
)


def _write_csv(tmp_path, header, rows):
    p = tmp_path / "subjects.csv"
    p.write_text("\n".join([header] + rows) + "\n")
    return str(p)


def test_read_csv_accepts_valid_header(spark, tmp_path):
    """Happy path: an exact header must pass validation (fix must not
    false-reject good files). The DataFrame stays LAZY and is never
    materialized here — Spark Connect on serverless cannot read client-local
    tmp paths (even .schema triggers a server analyze). Full end-to-end reads
    of valid files are proven by the pipeline runs in notebooks
    05_fresh_end_to_end_run / 07_negative_tests_and_contingency and by the
    nightly job; here we pin only that validation accepts the exact header."""
    path = _write_csv(tmp_path, VALID_HEADER, [VALID_ROW])
    df = read_csv_all_strings(spark, path, SUBJECT_COLS)   # must NOT raise ValueError
    assert df is not None


def test_read_csv_rejects_missing_mandatory_column(spark, tmp_path):
    """NEGATIVE (DEF-01): subjects.csv without the mandatory site_id column
    must fail loudly at read time, naming the missing column."""
    header = ",".join(c for c in SUBJECT_COLS if c != "site_id")
    row = ",".join(v for i, v in enumerate(VALID_ROW.split(",")) if i != 2)
    path = _write_csv(tmp_path, header, [row])
    with pytest.raises(ValueError, match="site_id"):
        read_csv_all_strings(spark, path, SUBJECT_COLS)


def test_read_csv_rejects_reordered_columns(spark, tmp_path):
    """NEGATIVE: same columns in a different order must also be rejected
    (Spark would map them positionally and silently swap values)."""
    reordered = SUBJECT_COLS[1:] + SUBJECT_COLS[:1]
    path = _write_csv(tmp_path, ",".join(reordered), [VALID_ROW])
    with pytest.raises(ValueError, match="positionally"):
        read_csv_all_strings(spark, path, SUBJECT_COLS)


def test_read_csv_rejects_extra_column(spark, tmp_path):
    """NEGATIVE: an unexpected extra column shifts every positional mapping
    and must be rejected too."""
    path = _write_csv(tmp_path, VALID_HEADER + ",bonus_col", [VALID_ROW + ",x"])
    with pytest.raises(ValueError, match="unexpected"):
        read_csv_all_strings(spark, path, SUBJECT_COLS)
