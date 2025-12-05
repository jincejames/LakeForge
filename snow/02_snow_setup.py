# Databricks notebook source
# MAGIC %run "/Shared/ServiceNow/01_snow_config"

# COMMAND ----------

# 02_snow_setup

# MAGIC %run "./01_snow_config"

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)
from pyspark.sql import functions as F
from datetime import datetime, timezone

# ------------------------------------------------------------------
# Time helpers
# ------------------------------------------------------------------
def now_utc():
    return datetime.now(timezone.utc)

def format_utc_ts(dt: datetime):
    if dt is None:
        return None
    # EXACT format you requested
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------
# Paths & table names for audit
# ------------------------------------------------------------------
def get_checkpoint_path():
    return f"{BASE_PATH}/audit/snow_checkpoint"

def get_checkpoint_table():
    return tbl(AUDIT_SCHEMA, "snow_checkpoint")

def get_history_path():
    return f"{BASE_PATH}/audit/snow_history"

def get_history_table():
    return tbl(AUDIT_SCHEMA, "snow_history")


# ------------------------------------------------------------------
# Schemas
# ------------------------------------------------------------------
CHECKPOINT_SCHEMA = StructType([
    StructField("table_name",      StringType(), True),
    StructField("last_page",       LongType(),   True),
    StructField("last_row_count",  LongType(),   True),
    StructField("last_run_id",     StringType(), True),
    StructField("status",          StringType(), True),
    StructField("last_utc_ts",     StringType(), True),   # watermark (UTC string)
    StructField("updated_at",      TimestampType(), True),
    StructField("updated_by",      StringType(), True),
])

HISTORY_SCHEMA = StructType([
    StructField("table_name",            StringType(), True),
    StructField("run_id",                StringType(), True),
    StructField("run_type",              StringType(), True),  # FULL / INCR
    StructField("status",                StringType(), True),  # SUCCESS / FAILED
    StructField("started_at",            TimestampType(), True),
    StructField("finished_at",           TimestampType(), True),
    StructField("total_rows",            LongType(),   True),
    StructField("start_page",            LongType(),   True),
    StructField("end_page",              LongType(),   True),
    StructField("api_final_page_utc_ts", StringType(), True),
    StructField("error_message",         StringType(), True),
])

UPDATED_BY = "snow_ingest"   # simple constant


# ------------------------------------------------------------------
# Ensure audit tables exist
# ------------------------------------------------------------------
def _ensure_delta_at_path(path: str, schema: StructType):
    try:
        spark.read.format("delta").load(path).limit(1)
    except Exception:
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.mode("overwrite").format("delta").save(path)

def ensure_audit_tables():
    cp_path = get_checkpoint_path()
    hist_path = get_history_path()

    _ensure_delta_at_path(cp_path, CHECKPOINT_SCHEMA)
    _ensure_delta_at_path(hist_path, HISTORY_SCHEMA)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_checkpoint_table()}
        USING DELTA
        LOCATION '{cp_path}'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {get_history_table()}
        USING DELTA
        LOCATION '{hist_path}'
    """)

# Ensure on import
ensure_audit_tables()


# ------------------------------------------------------------------
# Checkpoint helpers
# ------------------------------------------------------------------
def write_checkpoint(table_name: str,
                     last_page: int,
                     last_row_count: int,
                     run_id: str,
                     status: str,
                     last_utc_ts_str: str):
    """
    Append a new checkpoint row.
    """
    ensure_audit_tables()
    now = now_utc()

    data = [(
        table_name,
        int(last_page) if last_page is not None else -1,
        int(last_row_count) if last_row_count is not None else 0,
        run_id,
        status,
        last_utc_ts_str,
        now,
        UPDATED_BY
    )]

    df = spark.createDataFrame(data, CHECKPOINT_SCHEMA)
    df.write.mode("append").format("delta").save(get_checkpoint_path())


def get_last_state_checkpoint(table_name: str):
    """
    Last row for this table (any status) – for resume of FULL loads.
    """
    ensure_audit_tables()
    df = spark.read.format("delta").load(get_checkpoint_path())
    df = df.filter(F.col("table_name") == table_name) \
           .orderBy(F.col("updated_at").desc())
    rows = df.limit(1).collect()
    if not rows:
        return -1, 0, None, None, None
    r = rows[0]
    return r.last_page, r.last_row_count, r.last_run_id, r.status, r.last_utc_ts


def get_last_success_checkpoint(table_name: str):
    """
    Last SUCCESS row for this table – for incremental watermark.
    """
    ensure_audit_tables()
    df = spark.read.format("delta").load(get_checkpoint_path())
    df = df.filter(
        (F.col("table_name") == table_name) &
        (F.col("status") == "SUCCESS")
    ).orderBy(F.col("updated_at").desc())
    rows = df.limit(1).collect()
    if not rows:
        return -1, 0, None, None, None
    r = rows[0]
    return r.last_page, r.last_row_count, r.last_run_id, r.status, r.last_utc_ts


# ------------------------------------------------------------------
# History helper
# ------------------------------------------------------------------
def write_history(table_name: str,
                  run_id: str,
                  run_type: str,      # FULL / INCR
                  status: str,        # SUCCESS / FAILED
                  started_at: datetime,
                  finished_at: datetime,
                  total_rows: int,
                  start_page: int,
                  end_page: int,
                  api_final_page_utc_ts_str: str,
                  error_message: str = None):
    ensure_audit_tables()

    data = [(
        table_name,
        run_id,
        run_type,
        status,
        started_at,
        finished_at,
        int(total_rows) if total_rows is not None else 0,
        int(start_page) if start_page is not None else -1,
        int(end_page) if end_page is not None else -1,
        api_final_page_utc_ts_str,
        error_message
    )]

    df = spark.createDataFrame(data, HISTORY_SCHEMA)
    df.write.mode("append").format("delta").save(get_history_path())
