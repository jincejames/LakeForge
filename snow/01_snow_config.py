# Databricks notebook source
# 01_snow_config
# Global configuration for ServiceNow ingestion (UC-ready)

# ======================================================================
# CATALOG + SCHEMAS
# ======================================================================

CATALOG = "hive_metastore"   # Later: switch to your UC catalog (e.g. "snow")

RAW_SCHEMA     = "raw"
STAGING_SCHEMA = "staging"
SILVER_SCHEMA  = "silver"   # for later
AUDIT_SCHEMA   = "audit"

BASE_PATH = "/mnt/snow-cmi"

# ======================================================================
# Helper builders
# ======================================================================

def db(schema: str) -> str:
    return f"{CATALOG}.{schema}"

def tbl(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"

def path(layer: str, table: str) -> str:
    return f"{BASE_PATH}/{layer}/{table}"

def get_paths(table: str) -> dict:
    return {
        "raw":     f"{BASE_PATH}/raw/{table}",
        "staging": f"{BASE_PATH}/staging/{table}",
        # silver reserved for later
    }

def get_tables(table: str) -> dict:
    return {
        "raw":     tbl(RAW_SCHEMA,     table),
        "staging": tbl(STAGING_SCHEMA, table),
        "silver":  tbl(SILVER_SCHEMA,  table),
    }

# ======================================================================
# Checkpoint + History helpers
# ======================================================================

def get_checkpoint_table() -> str:
    return tbl(AUDIT_SCHEMA, "snow_checkpoint")

def get_checkpoint_path() -> str:
    return f"{BASE_PATH}/audit/snow_checkpoint"

def get_history_table() -> str:
    return tbl(AUDIT_SCHEMA, "snow_run_history")

def get_history_path() -> str:
    return f"{BASE_PATH}/audit/snow_run_history"

# ======================================================================
# MASTER TABLE CONFIGURATION (priority for incident-first analysis)
# ======================================================================

# You can change enabled / priority as needed
TABLE_CONFIG = [
    # Incident core
    {"name": "incident",                "enabled": True, "priority": 1},
    {"name": "incident_task",           "enabled": True, "priority": 1},
    {"name": "task",                    "enabled": True, "priority": 1},
    {"name": "task_sla",                "enabled": True, "priority": 1},
    {"name": "contract_sla",            "enabled": True, "priority": 1},

    # Problem Management
    {"name": "problem",                 "enabled": True, "priority": 2},
    {"name": "problem_task",            "enabled": True, "priority": 2},

    # Request Management
    {"name": "sc_request",              "enabled": True, "priority": 3},
    {"name": "sc_req_item",             "enabled": True, "priority": 3},

    # CI / CMDB
    {"name": "cmdb_ci",                 "enabled": True, "priority": 4},
    {"name": "cmdb_ci_service",         "enabled": True, "priority": 4},
    {"name": "cmdb_ci_service_auto",    "enabled": True, "priority": 4},

    # User / Org
    {"name": "sys_user",                "enabled": True, "priority": 5},
    {"name": "sys_user_group",          "enabled": True, "priority": 5},
    {"name": "cmn_department",          "enabled": True, "priority": 5},
    {"name": "cmn_location",            "enabled": True, "priority": 5},

    # SLA / Lifecycle / Classification
    {"name": "u_classification_value",  "enabled": True, "priority": 6},
    {"name": "life_cycle_stage",        "enabled": True, "priority": 6},
    {"name": "life_cycle_stage_status", "enabled": True, "priority": 6},

    # Metrics
    {"name": "metric_definition",       "enabled": True, "priority": 7},
    {"name": "metric_instance",         "enabled": True, "priority": 7},

    # Vulnerability
    {"name": "sn_vul_third_party_entry","enabled": True, "priority": 8},
    {"name": "sn_vul_vulnerable_item",  "enabled": True, "priority": 8},

    # Local taxonomy
    {"name": "u_dai_company",           "enabled": True, "priority": 9},
    {"name": "u_dai_plant",             "enabled": True, "priority": 9},

    # Change
    {"name": "change_task",             "enabled": True, "priority": 10},
]

def get_enabled_tables_sorted() -> list:
    return [
        t["name"]
        for t in sorted(
            [t for t in TABLE_CONFIG if t.get("enabled", True)],
            key=lambda x: x.get("priority", 999),
        )
    ]

# ======================================================================
# Operational Settings
# ======================================================================

# How many pages to fetch in one Spark window (parallel)
PARALLEL_PAGES = 50   # You can change this later

# Safety / API behavior
PAGE_LIMIT       = 10000   # Hard max pages per table (safety guard)
MAX_PAGE_RETRIES = 10
PAGE_SLEEP_SEC   = 0.3     # Base sleep between retry attempts
TIMEOUT_SEC      = 120     # HTTP timeout per request (seconds)

# Spark optimizations
spark.conf.set("spark.sql.shuffle.partitions", "500")
spark.conf.set("spark.default.parallelism",    "500")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Authentication from Key Vault (Databricks secrets)
SCOPE        = "snow-keyvault"
USERNAME_KEY = "SNOW-USERNAME"
TOKEN_KEY    = "SNOW-API-Password"

username = dbutils.secrets.get(scope=SCOPE, key=USERNAME_KEY)
token    = dbutils.secrets.get(scope=SCOPE, key=TOKEN_KEY)

# Base API URL (from your documentation)
BASE_URL = "https://servicenow.i.mercedes-benz.com/api/x_4dai_vi/v1"
