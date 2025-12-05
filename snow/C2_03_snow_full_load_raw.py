# Databricks notebook source
# MAGIC %run "/Shared/ServiceNow/02_snow_setup"

# COMMAND ----------

# 03_snow_full_load

# MAGIC %run "./01_snow_config"
# MAGIC %run "./02_snow_setup"

from pyspark.sql import Row
from pyspark.sql import functions as F
import uuid
import time
import requests
from requests.auth import HTTPBasicAuth
from pyspark.sql.types import StructType, StructField, StringType

# ------------------------------------------------------------------
# JSON flatten
# ------------------------------------------------------------------
def flatten_json(nested_json, parent_key="", sep="_"):
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
                items.append((new_key, str(value)))
        else:
            items.append((new_key, str(value) if value is not None else None))
    return dict(items)


# ------------------------------------------------------------------
# Single-page fetch for FULL LOAD (no last_update)
# ------------------------------------------------------------------
def make_full_page_fetcher(table_name: str):
    """
    Returns a function(page_id) → (page_id, row_count, rows, page_ts_str, error, soft_no_data)
    soft_no_data=True for 404 / "no more data" situations (not treated as error).
    """
    def fetch_page(page_id: int):
        for attempt in range(MAX_PAGE_RETRIES):
            try:
                start_call = now_utc()

                resp = requests.get(
                    f"{BASE_URL}/data/{table_name}",
                    auth=HTTPBasicAuth(username, token),
                    params={"page": page_id},
                    timeout=TIMEOUT_SEC
                )

                page_ts_str = format_utc_ts(now_utc())

                # Normal success
                if resp.status_code == 200:
                    body = resp.json().get("result", {}).get("body", {})
                    result = body.get("result", [])
                    if not result:
                        # Empty page – no rows, but not error
                        return page_id, 0, [], page_ts_str, None, False

                    rows = []
                    for r in result:
                        flat = flatten_json(r)
                        # keep per-page timestamp
                        flat["_page_response_utc_ts"] = page_ts_str
                        rows.append(flat)

                    return page_id, len(rows), rows, page_ts_str, None, False

                # Soft "no more data" – treat 404 as end-of-data, not failure
                if resp.status_code == 404:
                    return page_id, 0, [], page_ts_str, None, True

                # Other HTTP errors → retry
            except Exception:
                pass

            time.sleep(PAGE_SLEEP_SEC)

        # Hard failure after retries
        return page_id, 0, [], None, f"Failed page {page_id} after {MAX_PAGE_RETRIES} attempts", False

    return fetch_page


# ------------------------------------------------------------------
# FULL LOAD driver
# ------------------------------------------------------------------
def full_load_table_raw(table_name: str, resume_from_checkpoint: bool = True):
    print(f"\n\n🚀 FULL LOAD START: {table_name}")

    paths = get_paths(table_name)   # from 01_snow_config
    started_at = now_utc()

    # Get last state (any status) for resume
    last_page, last_count, last_run_id, last_status, last_utc_ts = get_last_state_checkpoint(table_name)

    if resume_from_checkpoint and last_status == "IN_PROGRESS":
        run_id = last_run_id or str(uuid.uuid4())
        start_page = last_page + 1
        print(f"🔁 Resuming {table_name} from checkpoint page {start_page} (run_id={run_id})")
    else:
        run_id = str(uuid.uuid4())
        start_page = 0
        print(f"🧨 Starting CLEAN full load for {table_name} (run_id={run_id})")
        # clear staging only at fresh full start
        print(f"🧹 Clearing staging for {table_name}: {paths['staging']}")
        dbutils.fs.rm(paths["staging"], recurse=True)
        write_checkpoint(table_name, -1, 0, run_id, "IN_PROGRESS", None)

    total_rows_staged = 0
    page_watermarks = []   # list of page-level UTC strings
    current_page = start_page

    fetch_page = make_full_page_fetcher(table_name)

    try:
        while current_page < PAGE_LIMIT:
            pages_this_round = list(range(current_page, current_page + PARALLEL_PAGES))
            print(f"➡️ Fetching pages {pages_this_round[0]}–{pages_this_round[-1]} for {table_name}")

            rdd = sc.parallelize(pages_this_round, len(pages_this_round))
            results = rdd.map(fetch_page).collect()

            # Hard failures
            failures = [r for r in results if r[4] is not None]
            if failures:
                print("❌ Page failures:", failures)
                current_watermark = max(page_watermarks) if page_watermarks else None
                finished_at = now_utc()
                write_history(
                    table_name, run_id, "FULL", "FAILED",
                    started_at, finished_at,
                    total_rows_staged,
                    start_page, current_page,
                    current_watermark,
                    error_message="Page failures during full load"
                )
                write_checkpoint(table_name, current_page, total_rows_staged, run_id, "FAILED", current_watermark)
                raise Exception(f"Stopping due to failures for table: {table_name}")

            # Non-empty pages
            non_empty = [r for r in results if r[1] > 0]

            # If ALL pages empty → we reached end of data → stop cleanly
            if not non_empty:
                print(f"🛑 No more data after page {current_page} for {table_name} – stopping full load.")
                break

            # Flatten data and collect watermarks
            flat_data = []
            for (pid, row_count, rows, page_ts_str, err, soft_no_data) in non_empty:
                flat_data.extend(rows)
                total_rows_staged += row_count
                if page_ts_str:
                    page_watermarks.append(page_ts_str)

            # Write into STAGING (with stable schema)
            if flat_data:
                all_keys = sorted({k for r in flat_data for k in r.keys()})
                row_objs = [Row(**{k: r.get(k) for k in all_keys}) for r in flat_data]
                # Build schema: every column as StringType
                schema = StructType([StructField(k, StringType(), True) for k in all_keys])

                row_dicts = [{k: str(r.get(k)) if r.get(k) is not None else None for k in all_keys}
                            for r in flat_data]

                batch_df = spark.createDataFrame(row_dicts, schema=schema)

                if "_page_response_utc_ts" in batch_df.columns:
                    batch_df = batch_df.withColumnRenamed("_page_response_utc_ts", "page_response_utc_ts")

                batch_df = (
                    batch_df
                    .withColumn("load_batch_id", F.lit(run_id))
                    .withColumn("run_type", F.lit("FULL"))
                    .withColumn("ingest_utc_ts", F.current_timestamp())
                )

                batch_df.write.mode("append").format("delta").save(paths["staging"])

            # Determine last processed page (highest non-empty page id)
            processed_pages = [pid for (pid, _, _, _, _, _) in non_empty]
            last_processed_page = max(processed_pages)
            current_page = last_processed_page + 1

            current_watermark = max(page_watermarks) if page_watermarks else None
            print(f"📦 {table_name}: pages {processed_pages[0]}–{processed_pages[-1]} "
                  f"→ {sum([r[1] for r in non_empty])} rows | STAGED_TOTAL={total_rows_staged} | "
                  f"WM={current_watermark}")

            write_checkpoint(table_name, last_processed_page, total_rows_staged, run_id, "IN_PROGRESS", current_watermark)

        # ---------------- Commit to RAW ----------------
        print(f"\n🔄 Dedup + Promote to RAW: {table_name}")

        if total_rows_staged > 0:
            staged_df = spark.read.format("delta").load(paths["staging"])
            deduped = staged_df.dropDuplicates()

            (
                deduped
                .write
                .mode("overwrite")
                .format("delta")
                .partitionBy("load_batch_id")
                .save(paths["raw"])
            )
        else:
            print(f"ℹ️ No rows staged for {table_name}; skipping RAW write.")

        finished_at = now_utc()
        final_watermark = max(page_watermarks) if page_watermarks else None

        write_history(
            table_name, run_id, "FULL", "SUCCESS",
            started_at, finished_at,
            total_rows_staged,
            start_page, current_page - 1,
            final_watermark,
            error_message=None
        )
        write_checkpoint(table_name, current_page - 1, total_rows_staged, run_id, "SUCCESS", final_watermark)

        print(f"🎯 RAW COMMIT SUCCESS {table_name} | TOTAL_ROWS={total_rows_staged} | WM={final_watermark}")

    except Exception as e:
        # in case of unexpected exception not handled above
        finished_at = now_utc()
        current_watermark = max(page_watermarks) if page_watermarks else None
        write_history(
            table_name, run_id, "FULL", "FAILED",
            started_at, finished_at,
            total_rows_staged,
            start_page, current_page,
            current_watermark,
            error_message=str(e)
        )
        write_checkpoint(table_name, current_page, total_rows_staged, run_id, "FAILED", current_watermark)
        print(f"💥 FULL LOAD FAILED for {table_name}: {e}")
        raise


# ------------------------------------------------------------------
# Example runner – you can adjust this list per cluster
# ------------------------------------------------------------------
tables_to_run = [
    "life_cycle_stage",
    "life_cycle_stage_status",
    "metric_definition",
]

for t in tables_to_run:
    print(f"\n============================")
    print(f"TABLE START: {t}")
    print(f"============================")
    full_load_table_raw(t, resume_from_checkpoint=True)
