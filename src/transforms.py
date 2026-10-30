"""Pure PySpark transformation functions for the clinical-trial DQ pipeline.

All Bronze -> Silver -> Gold data logic lives here as plain functions over
DataFrames so that:
  * the notebooks stay thin and only call these functions, and
  * pytest can exercise every rule on small in-memory DataFrames -- locally,
    on GitHub Actions CI, and inside the Databricks workspace -- with no
    Databricks-specific dependency.

Bronze design (user-confirmed):
  * append-only: no MERGE and no schema changes ever in the bronze layer;
  * deduplication happens ONLY in Silver (bronze keeps all raw rows).
"""

from functools import reduce
from operator import and_

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

DATE_FORMAT = "yyyy-MM-dd"

# Business columns per dataset (bronze reads exactly these from the CSV).
BUSINESS_COLUMNS = {
    "studies": [
        "study_id", "study_name", "therapeutic_area", "Phase", "Sponsor",
        "start_date", "planned_end_date", "target_enrollment", "study_status",
    ],
    "sites": [
        "site_id", "study_id", "site_name", "Country", "City",
        "principal_investigator", "activation_date", "site_target_enrollment",
        "site_status",
    ],
    "subjects": [
        "subject_id", "study_id", "site_id", "screening_date",
        "enrollment_date", "birth_year", "Sex", "treatment_arm",
        "subject_status",
    ],
    "visits": [
        "visit_id", "subject_id", "study_id", "site_id", "visit_number",
        "visit_name", "scheduled_date", "actual_date", "visit_status",
    ],
}


# ---------------------------------------------------------------------------
# Bronze helpers (raw layer: keep everything, read only business columns)
# ---------------------------------------------------------------------------

def read_csv_all_strings(spark, path, business_cols):
    """Read a landing CSV as-is: every business column typed STRING (raw bronze)."""
    schema = StructType([StructField(c, StringType(), True) for c in business_cols])
    return (
        spark.read.schema(schema)
        .option("header", "true")
        .csv(path)
        .select(*business_cols)
    )


def new_rows_only(source_df, existing_df, business_cols):
    """Return source rows NOT already present in existing_df.

    Null-safe LEFT ANTI JOIN on ALL business columns, so:
      * re-reading an unchanged file appends zero rows (idempotent rerun);
      * NULL == NULL is treated as equal;
      * a row differing in any single column counts as new.
    """
    src = source_df.select(*business_cols)
    tgt = existing_df.select(*business_cols)
    cond = reduce(and_, [src[c].eqNullSafe(tgt[c]) for c in business_cols])
    return src.join(tgt, cond, "left_anti")


# ---------------------------------------------------------------------------
# Silver transforms (rename, cast, dedup, quarantine)
# ---------------------------------------------------------------------------

def transform_studies(df, silver_ts):
    """Bronze studies -> Silver studies.

    Renames Phase -> phase and Sponsor -> sponsor, casts start_date /
    planned_end_date to DATE and target_enrollment to INT, and stamps
    silver_load_timestamp.
    """
    return df.select(
        F.col("study_id"),
        F.col("study_name"),
        F.col("therapeutic_area"),
        F.col("Phase").alias("phase"),
        F.col("Sponsor").alias("sponsor"),
        F.to_date("start_date", DATE_FORMAT).alias("start_date"),
        F.to_date("planned_end_date", DATE_FORMAT).alias("planned_end_date"),
        F.col("target_enrollment").cast("int").alias("target_enrollment"),
        F.col("study_status"),
        F.lit(silver_ts).alias("silver_load_timestamp"),
    )


def transform_sites(df, silver_ts):
    """Bronze sites -> Silver sites (Country -> country, City -> city, typed)."""
    return df.select(
        F.col("site_id"),
        F.col("study_id"),
        F.col("site_name"),
        F.col("Country").alias("country"),
        F.col("City").alias("city"),
        F.col("principal_investigator"),
        F.to_date("activation_date", DATE_FORMAT).alias("activation_date"),
        F.col("site_target_enrollment").cast("int").alias("site_target_enrollment"),
        F.col("site_status"),
        F.lit(silver_ts).alias("silver_load_timestamp"),
    )


def transform_subjects(df, silver_ts):
    """Bronze subjects -> (valid deduped, quarantined).

    * trims site_id; rows with blank/null site_id are QUARANTINED;
    * valid rows are deduplicated by subject_id (first occurrence wins).

    Returns a (valid_df, quarantine_df) tuple.
    """
    raw = df.select(
        F.col("subject_id"),
        F.col("study_id"),
        F.trim(F.col("site_id")).alias("site_id"),
        F.to_date("screening_date", DATE_FORMAT).alias("screening_date"),
        F.to_date("enrollment_date", DATE_FORMAT).alias("enrollment_date"),
        F.col("birth_year").cast("int").alias("birth_year"),
        F.col("Sex").alias("sex"),
        F.col("treatment_arm"),
        F.col("subject_status"),
        F.lit(silver_ts).alias("silver_load_timestamp"),
    )
    has_site = F.col("site_id").isNotNull() & (F.trim(F.col("site_id")) != "")
    valid = raw.filter(has_site).dropDuplicates(["subject_id"])
    quarantine = raw.filter(~has_site)
    return valid, quarantine


def transform_visits(df, valid_subjects, silver_ts):
    """Bronze visits -> (valid, quarantined).

    A null actual_date is valid for pending visits and is retained.
    Visits whose subject_id is not among valid_subjects are quarantined.
    """
    raw = df.select(
        F.col("visit_id"),
        F.col("subject_id"),
        F.col("study_id"),
        F.col("site_id"),
        F.col("visit_number").cast("int").alias("visit_number"),
        F.col("visit_name"),
        F.to_date("scheduled_date", DATE_FORMAT).alias("scheduled_date"),
        F.to_date("actual_date", DATE_FORMAT).alias("actual_date"),
        F.col("visit_status"),
        F.lit(silver_ts).alias("silver_load_timestamp"),
    )
    subject_ids = valid_subjects.select("subject_id")
    valid = raw.join(subject_ids, "subject_id", "left_semi")
    quarantine = raw.join(subject_ids, "subject_id", "left_anti").withColumn(
        "quarantine_reason", F.lit("subject_id not in silver.subjects")
    )
    return valid, quarantine


# ---------------------------------------------------------------------------
# Gold KPI summaries
# ---------------------------------------------------------------------------

def build_study_kpi_summary(studies, subjects, visits, gold_ts=None):
    """One row per study with the 3 pipeline KPIs.

    KPI-1 enrollment_rate         = enrolled_subjects / target_enrollment
    KPI-2 visit_completion_rate   = completed_visits / total_visits
    KPI-3 visit_data_completeness = visits_with_actual_date / total_visits
    """
    dims = [
        "study_id", "study_name", "therapeutic_area", "phase", "sponsor",
        "study_status", "target_enrollment",
    ]
    enrollment = (
        studies.join(subjects, "study_id", "left")
        .groupBy(*dims)
        .agg(F.countDistinct("subject_id").alias("enrolled_subjects"))
    )
    visit_stats = visits.groupBy("study_id").agg(
        F.count(F.lit(1)).alias("total_visits"),
        F.sum(F.when(F.col("visit_status") == "Completed", 1).otherwise(0)).alias("completed_visits"),
        F.sum(F.when(F.col("actual_date").isNotNull(), 1).otherwise(0)).alias("visits_with_actual_date"),
    )
    out = (
        enrollment.join(visit_stats, "study_id", "left")
        .select(
            *dims,
            F.col("enrolled_subjects"),
            F.coalesce("total_visits", F.lit(0)).alias("total_visits"),
            F.coalesce("completed_visits", F.lit(0)).alias("completed_visits"),
            F.coalesce("visits_with_actual_date", F.lit(0)).alias("visits_with_actual_date"),
        )
        .select(
            F.col("*"),
            F.round(
                F.coalesce("enrolled_subjects", F.lit(0)).cast("double")
                / F.col("target_enrollment").cast("double"), 4
            ).alias("enrollment_rate"),
            F.when(
                F.col("total_visits") > 0,
                F.round(F.col("completed_visits").cast("double") / F.col("total_visits"), 4),
            ).otherwise(F.lit(0.0)).alias("visit_completion_rate"),
            F.when(
                F.col("total_visits") > 0,
                F.round(F.col("visits_with_actual_date").cast("double") / F.col("total_visits"), 4),
            ).otherwise(F.lit(0.0)).alias("visit_data_completeness"),
        )
    )
    if gold_ts is not None:
        out = out.withColumn("gold_load_timestamp", F.lit(gold_ts))
    return out


def build_site_kpi_summary(sites, subjects, visits, gold_ts=None):
    """One row per site with the site-level KPIs.

    site_enrollment_rate       = enrolled_subjects / site_target_enrollment
    site_visit_completion_rate = completed_visits / total_visits
    """
    dims = [
        "site_id", "study_id", "site_name", "country", "city", "site_status",
        "site_target_enrollment", "activation_date",
    ]
    enrollment = (
        sites.join(subjects, ["site_id", "study_id"], "left")
        .groupBy(*dims)
        .agg(F.countDistinct("subject_id").alias("enrolled_subjects"))
    )
    visit_stats = visits.groupBy("site_id", "study_id").agg(
        F.count(F.lit(1)).alias("total_visits"),
        F.sum(F.when(F.col("visit_status") == "Completed", 1).otherwise(0)).alias("completed_visits"),
    )
    out = (
        enrollment.join(visit_stats, ["site_id", "study_id"], "left")
        .select(
            *dims,
            F.col("enrolled_subjects"),
            F.coalesce("total_visits", F.lit(0)).alias("total_visits"),
            F.coalesce("completed_visits", F.lit(0)).alias("completed_visits"),
        )
        .select(
            F.col("*"),
            F.round(
                F.coalesce("enrolled_subjects", F.lit(0)).cast("double")
                / F.col("site_target_enrollment").cast("double"), 4
            ).alias("site_enrollment_rate"),
            F.when(
                F.col("total_visits") > 0,
                F.round(F.col("completed_visits").cast("double") / F.col("total_visits"), 4),
            ).otherwise(F.lit(0.0)).alias("site_visit_completion_rate"),
        )
    )
    if gold_ts is not None:
        out = out.withColumn("gold_load_timestamp", F.lit(gold_ts))
    return out


# ---------------------------------------------------------------------------
# KPI validation helpers
# ---------------------------------------------------------------------------

def count_kpi_mismatches(actual, expected, key_col, value_col):
    """Compare a stored KPI column against an independently recomputed one.

    Both DataFrames must expose key_col and value_col. Counts value
    differences plus keys missing on either side; 0 = PASS.
    """
    a = actual.select(F.col(key_col), F.col(value_col).alias("actual_val"))
    e = expected.select(F.col(key_col), F.col(value_col).alias("expected_val"))
    joined = a.join(e, key_col)
    diff = joined.filter(~F.col("actual_val").eqNullSafe(F.col("expected_val"))).count()
    missing = (
        a.join(e, key_col, "left_anti").count()
        + e.join(a, key_col, "left_anti").count()
    )
    return diff + missing


def independent_kpi_queries(silver_schema):
    """Independent KPI recomputations (correlated subqueries) for validation.

    Returns {check_name: (sql, key_col, gold_table, gold_col)}; each SQL
    recomputes a KPI in a deliberately different query style so the Gold
    tables are checked against an independent implementation.
    """
    s = silver_schema
    return {
        "KPI-1 Study enrollment rate": (
            f"""
            SELECT st.study_id AS id,
                   ROUND((SELECT COUNT(DISTINCT su.subject_id) FROM {s}.subjects su
                          WHERE su.study_id = st.study_id)
                         / CAST(st.target_enrollment AS DOUBLE), 4) AS val
            FROM {s}.studies st""",
            "study_id", "study_kpi_summary", "enrollment_rate",
        ),
        "KPI-2 Study visit completion rate": (
            f"""
            SELECT st.study_id AS id,
                   ROUND((SELECT COUNT(*) FROM {s}.visits v
                          WHERE v.study_id = st.study_id AND v.visit_status = 'Completed')
                         / CAST((SELECT COUNT(*) FROM {s}.visits v2
                                 WHERE v2.study_id = st.study_id) AS DOUBLE), 4) AS val
            FROM {s}.studies st""",
            "study_id", "study_kpi_summary", "visit_completion_rate",
        ),
        "KPI-3 Study visit data completeness": (
            f"""
            SELECT st.study_id AS id,
                   ROUND((SELECT COUNT(*) FROM {s}.visits v
                          WHERE v.study_id = st.study_id AND v.actual_date IS NOT NULL)
                         / CAST((SELECT COUNT(*) FROM {s}.visits v2
                                 WHERE v2.study_id = st.study_id) AS DOUBLE), 4) AS val
            FROM {s}.studies st""",
            "study_id", "study_kpi_summary", "visit_data_completeness",
        ),
        "KPI-4 Site enrollment rate": (
            f"""
            SELECT si.site_id AS id,
                   ROUND((SELECT COUNT(DISTINCT su.subject_id) FROM {s}.subjects su
                          WHERE su.site_id = si.site_id AND su.study_id = si.study_id)
                         / CAST(si.site_target_enrollment AS DOUBLE), 4) AS val
            FROM {s}.sites si""",
            "site_id", "site_kpi_summary", "site_enrollment_rate",
        ),
        "KPI-5 Site visit completion rate": (
            f"""
            SELECT si.site_id AS id,
                   ROUND((SELECT COUNT(*) FROM {s}.visits v
                          WHERE v.site_id = si.site_id AND v.study_id = si.study_id
                            AND v.visit_status = 'Completed')
                         / CAST((SELECT COUNT(*) FROM {s}.visits v2
                                 WHERE v2.site_id = si.site_id
                                   AND v2.study_id = si.study_id) AS DOUBLE), 4) AS val
            FROM {s}.sites si""",
            "site_id", "site_kpi_summary", "site_visit_completion_rate",
        ),
    }
