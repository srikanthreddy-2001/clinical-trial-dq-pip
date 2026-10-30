"""Data-quality checks as pure PySpark functions over DataFrames.

Each helper returns the number of violations (0 = pass). run_dq_checks()
executes the Silver DQ gate (DQ-01..DQ-07) used by the pipeline; checks are
pure so they can be unit-tested on small in-memory DataFrames.
"""

from functools import reduce
from operator import or_

from pyspark.sql import functions as F


def count_duplicate_keys(df, key):
    """Number of key values appearing more than once."""
    return df.groupBy(key).count().filter(F.col("count") > 1).count()


def count_blank_or_null(df, col_name):
    """Rows where col_name is null or trims to an empty string."""
    c = F.col(col_name)
    return df.filter(c.isNull() | (F.trim(c) == "")).count()


def count_null_in(df, col_names):
    """Rows where ANY of the listed columns is null."""
    cond = reduce(or_, [F.col(c).isNull() for c in col_names])
    return df.filter(cond).count()


def count_referential_violations(child, parent, key):
    """child rows whose key value is missing from parent."""
    return child.select(key).join(parent.select(key), key, "left_anti").count()


def run_dq_checks(subjects, visits, studies, sites):
    """Run the Silver DQ gate DQ-01..DQ-07; returns a list of result dicts."""
    checks = []

    def _add(check_id, description, violations):
        checks.append({
            "check_id": check_id,
            "description": description,
            "violations": violations,
            "status": "PASS" if violations == 0 else "FAIL",
        })

    _add("DQ-01", "No duplicate subject_ids in silver.subjects",
         count_duplicate_keys(subjects, "subject_id"))
    _add("DQ-02", "All silver.subjects have non-blank site_id",
         count_blank_or_null(subjects, "site_id"))
    _add("DQ-03", "All silver.visits have non-null visit_id and subject_id",
         count_null_in(visits, ["visit_id", "subject_id"]))
    _add("DQ-04", "All silver.studies have non-null study_id and start_date",
         count_null_in(studies, ["study_id", "start_date"]))
    _add("DQ-05", "Referential integrity: subjects.study_id exists in studies",
         count_referential_violations(subjects, studies, "study_id"))
    _add("DQ-06", "All silver.sites have non-null site_id and activation_date",
         count_null_in(sites, ["site_id", "activation_date"]))
    _add("DQ-07", "Every silver.visits row has a subject in silver.subjects",
         count_referential_violations(visits, subjects, "subject_id"))
    return checks


def all_passed(checks):
    """True when every check passed."""
    return all(c["status"] == "PASS" for c in checks)


def summarize(checks):
    """Human-readable DQ report (mirrors the original notebook output)."""
    lines = ["=" * 60, "=== DATA QUALITY CHECKS ===", "=" * 60]
    for c in checks:
        lines.append(f"[{c['status']}] {c['check_id']}: {c['description']} "
                     f"(violations={c['violations']})")
    failed = [c for c in checks if c["status"] == "FAIL"]
    lines.append(f"Summary: {len(checks) - len(failed)}/{len(checks)} passed, "
                 f"{len(failed)} failed")
    return "\n".join(lines)
