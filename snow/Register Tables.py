# Databricks notebook source
from pyspark.sql import functions as F
import os

raw_base_path = "/mnt/snow-cmi/raw"

# List all subfolders under the raw layer
folders = [f.name for f in dbutils.fs.ls(raw_base_path) if f.isDir()]

for folder in folders:
    table_name = folder.lower().replace('/', '_')  # optional: format table name
    table_path = f"{raw_base_path}/{folder}"

    print(f"📌 Registering Table: {table_name} → {table_path}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS raw.{table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)

print("🎯 All folders registered as Delta tables successfully!")

# COMMAND ----------

raw_base_path = "/mnt/snow-cmi/raw"

# List all folders under raw layer with cleanup for trailing slash
folders = [f.name.rstrip('/').strip() for f in dbutils.fs.ls(raw_base_path) if f.isDir()]

# Drop existing tables
print("🧹 Dropping existing tables in raw schema...\n")
for folder in folders:
    table_name = folder.replace("-", "_").replace(" ", "_").lower()
    spark.sql(f"DROP TABLE IF EXISTS raw.{table_name}")
    print(f"❌ Dropped: raw.{table_name}")

print("\n♻️ Registering each folder as fresh Delta table...\n")

# Recreate tables
for folder in folders:
    table_name = folder.replace("-", "_").replace(" ", "_").lower()
    table_path = f"{raw_base_path}/{folder}"

    # Skip empty folders if any
    if len(dbutils.fs.ls(table_path)) == 0:
        print(f"⚠️ Skipped empty: {table_name}")
        continue
    
    spark.sql(f"""
        CREATE TABLE raw.{table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)
    
    print(f"✅ Registered: raw.{table_name}")

print("\n🎯 All tables dropped & re-registered successfully!")


# COMMAND ----------

# Fetch all tables from raw schema
tables = spark.sql("SHOW TABLES IN raw").collect()

tables_to_drop = [
    row.tableName for row in tables
    if row.tableName.endswith("_")
]

print("Tables to drop:", tables_to_drop)

for tbl in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS raw.{tbl}")
    print(f"❌ Dropped: raw.{tbl}")

print("\n✨ Cleanup complete! All trailing-underscore tables removed.")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.audit.snow_checkpoint where table_name = 'cmdb_ci' order by updated_at desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.raw.incident

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.raw.incident_task
# MAGIC
# MAGIC  --b on a.sys_id_value=b.sys_id_value

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, sha2, lit, udf
from pyspark.sql.types import StringType
import re

# Load raw incident table
raw_table = "raw.incident"
df = spark.table(raw_table)

print(f"🔍 Loaded Raw Table: {raw_table}")

patterns = {
    "email": r"([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)",
    "phone": r"(\+?\d{2})\d{6}(\d{2})",
    "password": r"(?i)(password\s*[:=]\s*)(\S+)",
    "sysid": r"\b[a-f0-9]{32}\b",
    "card": r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b"
}

# Track detections
audit_log = []

def selective_mask(text):
    if text is None:
        return None
    
    original = text
    modified = text
    
    for label, pattern in patterns.items():
        if re.search(pattern, modified):
            modified = re.sub(pattern, 
                lambda m: 
                    # Email
                    "***@" + m.group(2) if label == "email" else
                    # Phone
                    m.group(1) + "XXXXXX" + m.group(2) if label == "phone" else
                    # Password
                    m.group(1) + "HASHED" if label == "password" else
                    # sys_id / card # Full Hash
                    sha2(lit(m.group(0)), 256) if label in ["sysid", "card"] else 
                    modified, 
                modified)
    
    return modified if modified != original else original


mask_udf = udf(selective_mask, StringType())

# Identify string columns
string_cols = [c for c, t in df.dtypes if t == "string"]

# Apply masking and track detections
df_masked = df
for col_name in string_cols:
    detected = (
        df.filter(col(col_name).rlike("|".join(patterns.values())))
          .select(col_name).limit(5)  # sample
    )
    
    if detected.count() > 0:
        audit_log.append((col_name, detected.count()))
        print(f"🛡️ Masking Sensitive Data in Column: {col_name}")
        display(detected)
    
    df_masked = df_masked.withColumn(col_name, mask_udf(col(col_name)))

# Display audit log
print("\n📋 Pseudonymization Summary:")
for col_name, count in audit_log:
    print(f"✔ {col_name} → {count} sensitive entries masked")

display(df_masked.limit(20))
