# Databricks notebook source
# MAGIC %run "/Shared/ServiceNow/02_snow_setup"

# COMMAND ----------

# 04_snow_incremental_raw
# Incremental load using last_update watermark (api_final_page_utc_ts)

from pyspark.sql import Row
from pyspark.sql import functions as F
from datetime import datetime, timezone
import time
import uuid
import requests
from requests.auth import HTTPBasicAuth


# ============================================================
# Time helpers
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def fmt_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# JSON Flatten (same as full)
# ============================================================

def flatten_json(nested_json, parent_key: str = "", sep: str = "_") -> dict:
    items = []
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key, sep).items())

        elif isinstance(value, list):
            if all(isinstance(v, dict) for v in value):
                for i, v in enumerate(value):
                    items.extend(flatten_json(v, f"{new_key}{sep}{i}", sep).items())
            else:
                if value is not None:
                    items.append((new_key, str(value)))
        else:
            if value is not None:
                items.append((new_key, str(value)))
    return dict(items)


# ============================================================
# Checkpoint / History helpers (re-use config paths)
# ============================================================

def get_last_success_watermark(table_name: str) -> str:
    """
    Get last successful api_final_page_utc_ts for a table,
    from checkpoint table. Used as last_update watermark.
    """
    cp_tbl = get_checkpoint_table()
    try:
        rows = spark.sql(f"""
            SELECT api_final_page_utc_ts
            FROM {cp_tbl}
            WHERE table_name = '{table_name}'
              AND status = 'SUCCESS'
              AND api_final_page_utc_ts IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT 1
        """).collect()
        if rows:
            return rows[0].api_final_page_utc_ts
    except Exception as e:
        print(f"⚠️ Could not fetch watermark from checkpoint for {table_name}: {e}")
    return None


def get_last_checkpoint(table_name: str):
    cp_tbl = get_checkpoint_table()
    try:
        rows = spark.sql(f"""
            SELECT last_page, last_row_count, last_run_id, status, api_final_page_utc_ts
            FROM {cp_tbl}
            WHERE table_name = '{table_name}'
            ORDER BY updated_at DESC
            LIMIT 1
        """).collect()
        if rows:
            r = rows[0]
            return r.last_page, r.last_row_count, r.last_run_id, r.status, r.api_final_page_utc_ts
    except Exception as e:
        print(f"⚠️ Could not read checkpoint for {table_name}: {e}")
    return -1, 0, None, None, None


def write_checkpoint(
    table_name: str,
    last_page: int,
    last_row_count: int,
    run_id: str,
    status: str,
    api_final_page_utc_ts: str
):
    cp_path = get_checkpoint_path()
    now = now_utc()
    updated_by = "ServiceNowIngest"

    data = [(table_name, last_page, last_row_count, run_id, status,
             api_final_page_utc_ts, now, updated_by)]

    df = spark.createDataFrame(
        data,
        schema="""
            table_name STRING,
            last_page BIGINT,
            last_row_count BIGINT,
            last_run_id STRING,
            status STRING,
            api_final_page_utc_ts STRING,
            updated_at TIMESTAMP,
            updated_by STRING
        """
    )

    df.write.mode("append").format("delta").save(cp_path)


def write_history(
    table_name: str,
    run_id: str,
    run_type: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    row_count: int,
    api_final_page_utc_ts: str,
    error_message: str = None
):
    hist_path = get_history_path()

    data = [(table_name,
             run_id,
             run_type,
             status,
             started_at,
             finished_at,
             int(row_count),
             api_final_page_utc_ts,
             error_message)]

    df = spark.createDataFrame(
        data,
        schema="""
            table_name STRING,
            run_id STRING,
            run_type STRING,
            status STRING,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            row_count BIGINT,
            api_final_page_utc_ts STRING,
            error_message STRING
        """
    )
    df.write.mode("append").format("delta").save(hist_path)


# ============================================================
# INCREMENTAL LOAD ENGINE
# ============================================================

def incremental_load_table_raw(table_name: str, resume_from_checkpoint: bool = True):

    print(f"\n\n🚀 INCREMENTAL LOAD START: {table_name}")

    paths = get_paths(table_name)

    # watermarked last_update from last successful run
    last_update = get_last_success_watermark(table_name)
    if not last_update:
        print(f"⚠️ No previous successful run for {table_name}. "
              f"Consider running full load first. Skipping incremental.")
        return

    print(f"🕒 Using last_update watermark for {table_name}: {last_update}")

    # checkpoint logic
    last_page, last_count, last_run_id, last_status, last_api_ts = get_last_checkpoint(table_name)

    if resume_from_checkpoint and last_status == "IN_PROGRESS" and last_page >= 0:
        start_page = last_page + 1
        print(f"🔁 Resuming incremental for {table_name} from page {start_page}")
    else:
        start_page = 0
        print(f"🧨 Starting new incremental run for {table_name}")
        write_checkpoint(
            table_name=table_name,
            last_page=-1,
            last_row_count=0,
            run_id=None,
            status="IN_PROGRESS",
            api_final_page_utc_ts=None
        )

    run_id = str(uuid.uuid4())
    started_at = now_utc()
    print(f"run_id = {run_id}")

    current_page = start_page
    total_rows_staged = 0
    seen_non_empty = False
    run_max_page_ts = None

    # For incremental we also use staging, but do NOT delete existing staging
    # if resuming an IN_PROGRESS run. Only clear when start_page == 0.
    if start_page == 0:
        print(f"🧹 Clearing staging for incremental {table_name}: {paths['staging']}")
        dbutils.fs.rm(paths["staging"], recurse=True)

    # inner function
    def fetch_page(page_id: int):
        """
        Return: (page_id, row_count, rows, page_response_ts_str_or_None, error_message_or_None)
        """
        error_msg = None
        for attempt in range(MAX_PAGE_RETRIES):
            try:
                params = {
                    "page": page_id,
                    "last_update": last_update
                }
                resp = requests.get(
                    f"{BASE_URL}/data/{table_name}",
                    auth=HTTPBasicAuth(username, token),
                    params=params,
                    timeout=TIMEOUT_SEC
                )
                resp_ts_str = fmt_utc(now_utc())

                if resp.status_code == 200:
                    body = resp.json().get("result", {}).get("body", {})
                    result_rows = body.get("result", [])
                    if not result_rows:
                        return page_id, 0, [], resp_ts_str, None

                    local_rows = []
                    for raw_row in result_rows:
                        flat = flatten_json(raw_row)
                        flat["_page_id"] = page_id
                        flat["_page_response_utc_ts"] = resp_ts_str
                        local_rows.append(flat)

                    return page_id, len(local_rows), local_rows, resp_ts_str, None

                elif resp.status_code == 404:
                    return page_id, 0, [], resp_ts_str, None
                else:
                    error_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"

            except Exception as e:
                error_msg = str(e)

            time.sleep(PAGE_SLEEP_SEC * (attempt + 1))

        return page_id, 0, [], None, error_msg

    # main loop
    try:
        while current_page < PAGE_LIMIT:
            pages_this_round = list(range(current_page, current_page + PARALLEL_PAGES))
            print(f"➡️ [INCR] Fetching pages {pages_this_round[0]}–{pages_this_round[-1]} for {table_name}")

            rdd = sc.parallelize(pages_this_round, len(pages_this_round))
            results = rdd.map(fetch_page).collect()

            failures = [r for r in results if r[4] is not None]
            if failures:
                print("❌ Page failures detected (incremental):")
                for (pid, _, _, _, err) in failures:
                    print(f"   - Page {pid}: {err}")

                finished_at = now_utc()
                final_ts_str = fmt_utc(run_max_page_ts) if run_max_page_ts else None

                write_checkpoint(
                    table_name=table_name,
                    last_page=current_page,
                    last_row_count=total_rows_staged,
                    run_id=run_id,
                    status="FAILED",
                    api_final_page_utc_ts=final_ts_str
                )
                write_history(
                    table_name=table_name,
                    run_id=run_id,
                    run_type="INCREMENTAL",
                    status="FAILED",
                    started_at=started_at,
                    finished_at=finished_at,
                    row_count=total_rows_staged,
                    api_final_page_utc_ts=final_ts_str,
                    error_message="Page failures during incremental load"
                )
                raise Exception(f"Stopping incremental due to failures for table: {table_name}")

            non_empty = [r for r in results if r[1] > 0]
            any_data_in_window = len(non_empty) > 0

            if any_data_in_window:
                seen_non_empty = True
            else:
                if not seen_non_empty:
                    print(f"🛑 No incremental data found for {table_name} (window at page {current_page})")
                    break
                else:
                    print(f"🛑 Reached first all-empty incremental window after data for {table_name}. Stopping.")
                    break

            flat_data = []
            for (pid, row_count, rows, page_ts_str, _) in non_empty:
                flat_data.extend(rows)
                if page_ts_str:
                    page_dt = datetime.strptime(page_ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    if (run_max_page_ts is None) or (page_dt > run_max_page_ts):
                        run_max_page_ts = page_dt

                total_rows_staged += row_count

            if flat_data:
                batch_df = spark.createDataFrame([Row(**r) for r in flat_data])

                if "_page_id" in batch_df.columns:
                    batch_df = batch_df.withColumnRenamed("_page_id", "page_id")
                if "_page_response_utc_ts" in batch_df.columns:
                    batch_df = batch_df.withColumnRenamed("_page_response_utc_ts", "page_response_utc_ts")

                batch_df = (
                    batch_df
                    .withColumn("load_batch_id", F.lit(run_id))
                    .withColumn("run_type", F.lit("INCREMENTAL"))
                    .withColumn("ingest_utc_ts", F.current_timestamp())
                )

                batch_df.write.mode("append").format("delta").save(paths["staging"])

            processed_pages = [pid for (pid, _, _, _, err) in results if err is None]
            last_processed_page = max(processed_pages) if processed_pages else current_page

            final_ts_str = fmt_utc(run_max_page_ts) if run_max_page_ts else None

            write_checkpoint(
                table_name=table_name,
                last_page=last_processed_page,
                last_row_count=total_rows_staged,
                run_id=run_id,
                status="IN_PROGRESS",
                api_final_page_utc_ts=final_ts_str
            )

            print(f"📦 [INCR] {table_name}: Pages {pages_this_round[0]}–{pages_this_round[-1]} => "
                  f"window_rows={sum([r[1] for r in non_empty])} | STAGED_TOTAL={total_rows_staged}")

            current_page = last_processed_page + 1

    except Exception as e:
        print(f"💥 ERROR in incremental load for {table_name}: {e}")
        raise

    finished_at = now_utc()
    final_ts_str = fmt_utc(run_max_page_ts) if run_max_page_ts else None

    try:
        print(f"\n🔄 [INCR] Dedup + promote to RAW for {table_name}")

        staged_df = spark.read.format("delta").load(paths["staging"])
        deduped = staged_df.dropDuplicates()

        if final_ts_str:
            deduped = deduped.withColumn("api_final_page_utc_ts", F.lit(final_ts_str))
        else:
            deduped = deduped.withColumn("api_final_page_utc_ts", F.lit(None).cast("string"))

        deduped.write.mode("append").format("delta") \
              .partitionBy("load_batch_id") \
              .save(paths["raw"])

        write_checkpoint(
            table_name=table_name,
            last_page=current_page - 1,
            last_row_count=total_rows_staged,
            run_id=run_id,
            status="SUCCESS",
            api_final_page_utc_ts=final_ts_str
        )
        write_history(
            table_name=table_name,
            run_id=run_id,
            run_type="INCREMENTAL",
            status="SUCCESS",
            started_at=started_at,
            finished_at=finished_at,
            row_count=total_rows_staged,
            api_final_page_utc_ts=final_ts_str,
            error_message=None
        )

        print(f"🎯 INCREMENTAL RAW LOAD SUCCESS for {table_name} | INCREMENTAL_ROWS={total_rows_staged}")
        print(f"   api_final_page_utc_ts = {final_ts_str}")

    except Exception as e:
        print(f"💥 ERROR while promoting incremental to RAW for {table_name}: {e}")
        write_checkpoint(
            table_name=table_name,
            last_page=current_page - 1,
            last_row_count=total_rows_staged,
            run_id=run_id,
            status="FAILED",
            api_final_page_utc_ts=final_ts_str
        )
        write_history(
            table_name=table_name,
            run_id=run_id,
            run_type="INCREMENTAL",
            status="FAILED",
            started_at=started_at,
            finished_at=finished_at,
            row_count=total_rows_staged,
            api_final_page_utc_ts=final_ts_str,
            error_message=str(e)[:4000]
        )
        raise


# ======================================================================
# EXECUTION BLOCK (tables_to_run can be tuned)
# ======================================================================

tables_to_run = [
    "incident",
    "incident_task",
    "task",
    "task_sla",
    "contract_sla",
    "sc_request",
    "sc_req_item",
    "sys_user",
    "sys_user_group",
    "cmdb_ci",
    "cmdb_ci_service",
    "cmdb_ci_service_auto",
]

for t in tables_to_run:
    print(f"\n\n============================")
    print(f"INCREMENTAL TABLE START: {t}")
    print(f"============================")
    incremental_load_table_raw(t, resume_from_checkpoint=True)
