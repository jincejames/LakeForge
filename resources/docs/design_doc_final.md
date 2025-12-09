
This document provides the complete technical design for implementing a Databricks Lakehouse solution for ServiceNow Incident data. The solution follows the medallion architecture (Bronze → Silver → Gold) with comprehensive data quality governance at each layer.

**Project Name**: servicenow_incident_lakehouse  
**Source System**: ServiceNow ITSM - Incident Module  
**Target Platform**: Databricks Unity Catalog  
**Data Pattern**: SCD Type 2 (Full History Preservation)

### Key Deliverables
- 1 Bronze table (raw ingestion with CDC)
- 2 Silver tables (transformed and current state)
- 5 Gold tables with consumer views (aggregated metrics)
- Comprehensive data quality framework

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SERVICENOW INSTANCE                               │
│                         (Incident Table)                                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ Lakeflow Connect (CDC)
┌─────────────────────────────────────────────────────────────────────────┐
│                          BRONZE LAYER                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ bronze.incident                                                   │   │
│  │ - Raw CDC data with __START_AT/__END_AT                          │   │
│  │ - Partitioned by ingestion_date                                   │   │
│  │ - SCD2 pattern for full history                                   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ Transformation + Quality Checks
┌─────────────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER                                    │
│  ┌──────────────────────────────┐  ┌──────────────────────────────┐    │
│  │ silver.incident_flattened    │  │ silver.incident_current      │    │
│  │ - Full SCD2 history          │  │ - Current records only       │    │
│  │ - Derived metrics            │  │ - Simplified analytics       │    │
│  │ - Extracted reference IDs    │  │ - Partitioned by year/month  │    │
│  └──────────────────────────────┘  └──────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ Aggregation + Governance
┌─────────────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER                                     │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────┐ │
│  │ incident_daily_     │  │ incident_category_  │  │ assignment_     │ │
│  │ metrics             │  │ metrics             │  │ group_monthly_  │ │
│  │ + view              │  │ + view              │  │ metrics + view  │ │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────┘ │
│  ┌─────────────────────┐  ┌─────────────────────┐                      │
│  │ incident_backlog_   │  │ incident_weekly_    │                      │
│  │ snapshot + views    │  │ metrics + views     │                      │
│  └─────────────────────┘  └─────────────────────┘                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Bronze Layer Design

### 2.1 Table: bronze.incident

**Classification**: Historized  
**Finality**: Correctable - Type 2 CDC  
**Ingestion Strategy**: RepeatPull - Incremental CDC

#### Storage Configuration
```
Location: s3://datalake-bronze/servicenow/incident/ (AWS)
         OR abfss://bronze@<storage_account>.dfs.core.windows.net/servicenow/incident/ (Azure)
Format: Delta
Partitioning: ingestion_date (YYYY-MM-DD)
```

#### Ingestion Strategy Details
1. **Initial Load**: Full extraction of all incident records to establish baseline
2. **Incremental Loads**: Pull records where `sys_updated_on >= last_ingestion_timestamp`
3. **CDC Handling**: Lakeflow Connect captures inserts, updates, and deletes with `__START_AT/__END_AT` validity windows
4. **Merge Pattern**: MERGE INTO with SCD2 logic - expire existing records and insert new versions
5. **Frequency**: Every 15-30 minutes (recommended for operational incident data)
6. **Watermark Management**: Store high watermark in pipeline metadata table

#### DDL (Coder Implementation Step 1)
```sql
-- Bronze table will be auto-created by Lakeflow Connect
-- Ensure external location and catalog permissions are configured
-- Key columns from ServiceNow incident table will include:
-- sys_id, number, short_description, description, state, priority, urgency, impact,
-- category, subcategory, opened_at, resolved_at, closed_at, sys_created_on, sys_updated_on,
-- opened_by, caller_id, assigned_to, assignment_group, etc.
-- Plus CDC columns: __START_AT, __END_AT, _databricks_deleted
```

---

## 3. Silver Layer Design

### 3.1 Table: silver.incident_flattened

**Purpose**: Full SCD2 history with flattened references and derived metrics  
**Partitioning**: opened_date

#### DDL (Coder Implementation Step 2)
```sql
CREATE TABLE IF NOT EXISTS silver.incident_flattened (
    -- Primary Key
    sys_id STRING NOT NULL COMMENT 'Unique incident identifier',
    
    -- Core Incident Fields (passthrough)
    number STRING COMMENT 'Incident number (e.g., INC0010001)',
    short_description STRING COMMENT 'Brief description of the incident',
    description STRING COMMENT 'Detailed description of the incident',
    
    -- State and Status Fields
    state INT COMMENT 'State code',
    state_name STRING COMMENT 'Derived: Human-readable state name',
    active BOOLEAN COMMENT 'Whether incident is active',
    
    -- Priority Fields
    priority INT COMMENT 'Priority code (1-5)',
    priority_name STRING COMMENT 'Derived: Human-readable priority name',
    urgency INT COMMENT 'Urgency code (1-3)',
    urgency_name STRING COMMENT 'Derived: Human-readable urgency name',
    impact INT COMMENT 'Impact code (1-3)',
    impact_name STRING COMMENT 'Derived: Human-readable impact name',
    
    -- Categorization (normalized)
    category STRING COMMENT 'Normalized: Incident category (uppercase)',
    subcategory STRING COMMENT 'Normalized: Incident subcategory (uppercase)',
    contact_type STRING COMMENT 'Normalized: Contact type (uppercase)',
    close_code STRING COMMENT 'Normalized: Close code (uppercase)',
    
    -- Timestamps (passthrough)
    opened_at TIMESTAMP COMMENT 'When incident was opened',
    resolved_at TIMESTAMP COMMENT 'When incident was resolved',
    closed_at TIMESTAMP COMMENT 'When incident was closed',
    sys_created_on TIMESTAMP COMMENT 'Record creation timestamp',
    sys_updated_on TIMESTAMP COMMENT 'Record last update timestamp',
    work_start TIMESTAMP COMMENT 'When work started on incident',
    work_end TIMESTAMP COMMENT 'When work ended on incident',
    reopened_time TIMESTAMP COMMENT 'When incident was reopened',
    
    -- Derived Dates
    opened_date DATE COMMENT 'Derived: Date portion of opened_at',
    resolved_date DATE COMMENT 'Derived: Date portion of resolved_at',
    closed_date DATE COMMENT 'Derived: Date portion of closed_at',
    
    -- Derived Flags
    is_resolved BOOLEAN COMMENT 'Derived: True if state = 6 (Resolved)',
    is_closed BOOLEAN COMMENT 'Derived: True if state = 7 (Closed)',
    
    -- Derived Duration Metrics
    time_to_resolve_hours DOUBLE COMMENT 'Derived: Hours from open to resolve',
    time_to_close_hours DOUBLE COMMENT 'Derived: Hours from open to close',
    first_response_hours DOUBLE COMMENT 'Derived: Hours from open to first work',
    calendar_duration_seconds BIGINT COMMENT 'Calendar duration in seconds',
    business_duration_seconds BIGINT COMMENT 'Business duration in seconds',
    
    -- Extracted Reference IDs
    opened_by_id STRING COMMENT 'Extracted: User ID who opened the incident',
    caller_id_id STRING COMMENT 'Extracted: Caller user ID',
    assigned_to_id STRING COMMENT 'Extracted: Assigned user ID',
    assignment_group_id STRING COMMENT 'Extracted: Assignment group ID',
    resolved_by_id STRING COMMENT 'Extracted: User ID who resolved',
    closed_by_id STRING COMMENT 'Extracted: User ID who closed',
    parent_id STRING COMMENT 'Extracted: Parent task ID',
    parent_incident_id STRING COMMENT 'Extracted: Parent incident ID',
    caused_by_id STRING COMMENT 'Extracted: Change that caused incident',
    problem_id_id STRING COMMENT 'Extracted: Related problem ID',
    rfc_id STRING COMMENT 'Extracted: Related RFC ID',
    cmdb_ci_id STRING COMMENT 'Extracted: Configuration item ID',
    business_service_id STRING COMMENT 'Extracted: Business service ID',
    service_offering_id STRING COMMENT 'Extracted: Service offering ID',
    company_id STRING COMMENT 'Extracted: Company ID',
    location_id STRING COMMENT 'Extracted: Location ID',
    reopened_by_id STRING COMMENT 'Extracted: User ID who reopened',
    contract_id STRING COMMENT 'Extracted: Contract ID',
    
    -- Additional Passthrough Fields
    close_notes STRING COMMENT 'Notes on closure',
    comments_and_work_notes STRING COMMENT 'Combined comments and work notes',
    reopen_count INT COMMENT 'Number of times reopened',
    reassignment_count INT COMMENT 'Number of reassignments',
    escalation INT COMMENT 'Escalation level',
    severity INT COMMENT 'Severity level',
    notify INT COMMENT 'Notification setting',
    knowledge BOOLEAN COMMENT 'Knowledge article created',
    made_sla BOOLEAN COMMENT 'Whether SLA was met',
    upon_reject STRING COMMENT 'Action upon rejection',
    upon_approval STRING COMMENT 'Action upon approval',
    correlation_id STRING COMMENT 'Correlation identifier',
    correlation_display STRING COMMENT 'Correlation display value',
    
    -- SCD2 Tracking Columns
    __START_AT TIMESTAMP NOT NULL COMMENT 'SCD2: Record effective start timestamp',
    __END_AT TIMESTAMP COMMENT 'SCD2: Record effective end timestamp (NULL for current)',
    _databricks_deleted BOOLEAN COMMENT 'Soft delete flag from source',
    
    -- Silver Layer Metadata
    _silver_processed_at TIMESTAMP COMMENT 'Silver layer processing timestamp',
    _bronze_file_path STRING COMMENT 'Source bronze file path for lineage'
)
USING DELTA
COMMENT 'Silver layer incident data with flattened references and derived metrics'
PARTITIONED BY (opened_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality.layer' = 'silver',
    'quality.scd_type' = '2'
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_incident_number ON silver.incident_flattened (number);
CREATE INDEX IF NOT EXISTS idx_incident_state ON silver.incident_flattened (state);
CREATE INDEX IF NOT EXISTS idx_incident_priority ON silver.incident_flattened (priority);
```

#### CDC Implementation (Coder Implementation Step 3)
```sql
-- CDC Implementation: SCD Type 2 with __END_AT column handling
-- This notebook implements full SCD2 merge logic for incident_flattened

-- Step 1: Get incremental changes from Bronze
CREATE OR REPLACE TEMPORARY VIEW bronze_changes AS
SELECT
    -- Primary Key
    sys_id,
    
    -- All transformed columns
    number,
    short_description,
    description,
    state,
    CASE state 
        WHEN 1 THEN 'New' 
        WHEN 2 THEN 'In Progress' 
        WHEN 3 THEN 'On Hold' 
        WHEN 6 THEN 'Resolved' 
        WHEN 7 THEN 'Closed' 
        WHEN 8 THEN 'Canceled' 
        ELSE 'Unknown' 
    END AS state_name,
    active,
    priority,
    CASE priority 
        WHEN 1 THEN 'Critical' 
        WHEN 2 THEN 'High' 
        WHEN 3 THEN 'Moderate' 
        WHEN 4 THEN 'Low' 
        WHEN 5 THEN 'Planning' 
        ELSE 'Unknown' 
    END AS priority_name,
    urgency,
    CASE urgency 
        WHEN 1 THEN 'High' 
        WHEN 2 THEN 'Medium' 
        WHEN 3 THEN 'Low' 
        ELSE 'Unknown' 
    END AS urgency_name,
    impact,
    CASE impact 
        WHEN 1 THEN 'High' 
        WHEN 2 THEN 'Medium' 
        WHEN 3 THEN 'Low' 
        ELSE 'Unknown' 
    END AS impact_name,
    UPPER(TRIM(category)) AS category,
    UPPER(TRIM(subcategory)) AS subcategory,
    UPPER(TRIM(contact_type)) AS contact_type,
    UPPER(TRIM(close_code)) AS close_code,
    opened_at,
    resolved_at,
    closed_at,
    sys_created_on,
    sys_updated_on,
    work_start,
    work_end,
    reopened_time,
    TO_DATE(opened_at) AS opened_date,
    TO_DATE(resolved_at) AS resolved_date,
    TO_DATE(closed_at) AS closed_date,
    (state = 6) AS is_resolved,
    (state = 7) AS is_closed,
    ROUND((UNIX_TIMESTAMP(resolved_at) - UNIX_TIMESTAMP(opened_at)) / 3600.0, 2) AS time_to_resolve_hours,
    ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600.0, 2) AS time_to_close_hours,
    ROUND((UNIX_TIMESTAMP(work_start) - UNIX_TIMESTAMP(opened_at)) / 3600.0, 2) AS first_response_hours,
    calendar_stc AS calendar_duration_seconds,
    business_stc AS business_duration_seconds,
    opened_by.value AS opened_by_id,
    caller_id.value AS caller_id_id,
    assigned_to.value AS assigned_to_id,
    assignment_group.value AS assignment_group_id,
    resolved_by.value AS resolved_by_id,
    closed_by.value AS closed_by_id,
    parent.value AS parent_id,
    parent_incident.value AS parent_incident_id,
    caused_by.value AS caused_by_id,
    problem_id.value AS problem_id_id,
    rfc.value AS rfc_id,
    cmdb_ci.value AS cmdb_ci_id,
    business_service.value AS business_service_id,
    service_offering.value AS service_offering_id,
    company.value AS company_id,
    location.value AS location_id,
    reopened_by.value AS reopened_by_id,
    contract.value AS contract_id,
    close_notes,
    comments_and_work_notes,
    reopen_count,
    reassignment_count,
    escalation,
    severity,
    notify,
    knowledge,
    made_sla,
    upon_reject,
    upon_approval,
    correlation_id,
    correlation_display,
    __START_AT,
    __END_AT,
    _databricks_deleted,
    CURRENT_TIMESTAMP() AS _silver_processed_at,
    _metadata.file_path AS _bronze_file_path
FROM bronze.incident
WHERE __START_AT > '${last_processed_timestamp}';

-- Step 2: SCD Type 2 MERGE Operation
MERGE INTO silver.incident_flattened AS target
USING (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY sys_id ORDER BY __START_AT DESC) as rn
    FROM bronze_changes
) AS source
ON target.sys_id = source.sys_id 
   AND target.__END_AT IS NULL

WHEN MATCHED 
    AND source.__START_AT > target.__START_AT 
    AND source.rn = 1
THEN UPDATE SET
    target.__END_AT = source.__START_AT,
    target._silver_processed_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED AND source.rn = 1
THEN INSERT (
    sys_id, number, short_description, description,
    state, state_name, active,
    priority, priority_name, urgency, urgency_name, impact, impact_name,
    category, subcategory, contact_type, close_code,
    opened_at, resolved_at, closed_at, sys_created_on, sys_updated_on,
    work_start, work_end, reopened_time,
    opened_date, resolved_date, closed_date,
    is_resolved, is_closed,
    time_to_resolve_hours, time_to_close_hours, first_response_hours,
    calendar_duration_seconds, business_duration_seconds,
    opened_by_id, caller_id_id, assigned_to_id, assignment_group_id,
    resolved_by_id, closed_by_id, parent_id, parent_incident_id,
    caused_by_id, problem_id_id, rfc_id, cmdb_ci_id,
    business_service_id, service_offering_id, company_id, location_id,
    reopened_by_id, contract_id,
    close_notes, comments_and_work_notes, reopen_count, reassignment_count,
    escalation, severity, notify, knowledge, made_sla,
    upon_reject, upon_approval, correlation_id, correlation_display,
    __START_AT, __END_AT, _databricks_deleted,
    _silver_processed_at, _bronze_file_path
)
VALUES (
    source.sys_id, source.number, source.short_description, source.description,
    source.state, source.state_name, source.active,
    source.priority, source.priority_name, source.urgency, source.urgency_name, 
    source.impact, source.impact_name,
    source.category, source.subcategory, source.contact_type, source.close_code,
    source.opened_at, source.resolved_at, source.closed_at, source.sys_created_on, 
    source.sys_updated_on, source.work_start, source.work_end, source.reopened_time,
    source.opened_date, source.resolved_date, source.closed_date,
    source.is_resolved, source.is_closed,
    source.time_to_resolve_hours, source.time_to_close_hours, source.first_response_hours,
    source.calendar_duration_seconds, source.business_duration_seconds,
    source.opened_by_id, source.caller_id_id, source.assigned_to_id, source.assignment_group_id,
    source.resolved_by_id, source.closed_by_id, source.parent_id, source.parent_incident_id,
    source.caused_by_id, source.problem_id_id, source.rfc_id, source.cmdb_ci_id,
    source.business_service_id, source.service_offering_id, source.company_id, source.location_id,
    source.reopened_by_id, source.contract_id,
    source.close_notes, source.comments_and_work_notes, source.reopen_count, 
    source.reassignment_count, source.escalation, source.severity, source.notify, 
    source.knowledge, source.made_sla, source.upon_reject, source.upon_approval, 
    source.correlation_id, source.correlation_display,
    source.__START_AT, source.__END_AT, source._databricks_deleted,
    source._silver_processed_at, source._bronze_file_path
);

-- Step 3: Handle soft deletes
UPDATE silver.incident_flattened
SET __END_AT = CURRENT_TIMESTAMP(),
    _databricks_deleted = TRUE,
    _silver_processed_at = CURRENT_TIMESTAMP()
WHERE sys_id IN (
    SELECT sys_id FROM bronze_changes WHERE _databricks_deleted = TRUE
)
AND __END_AT IS NULL;

-- Step 4: Optimize table after merge
OPTIMIZE silver.incident_flattened
WHERE opened_date >= DATE_SUB(CURRENT_DATE(), 30)
ZORDER BY (sys_id, __START_AT);
```

#### Quality Checks (Coder Implementation Step 4)
```yaml
quality_checks:
  table: silver.incident_flattened
  layer: silver
  
  primary_key_checks:
    - name: pk_sys_id_not_null
      expectation: "sys_id IS NOT NULL"
      severity: critical
      
    - name: pk_sys_id_unique_current
      expectation: |
        SELECT sys_id, COUNT(*) as cnt 
        FROM silver.incident_flattened 
        WHERE __END_AT IS NULL 
        GROUP BY sys_id 
        HAVING COUNT(*) > 1
      expected_result: "0 rows returned"
      severity: critical

  not_null_checks:
    - name: nn_number
      expectation: "number IS NOT NULL"
      severity: high
      
    - name: nn_state
      expectation: "state IS NOT NULL"
      severity: high
      
    - name: nn_opened_at
      expectation: "opened_at IS NOT NULL"
      severity: high
      
    - name: nn_start_at_scd2
      expectation: "__START_AT IS NOT NULL"
      severity: critical

  range_checks:
    - name: range_state_valid
      expectation: "state BETWEEN 1 AND 8"
      severity: high
      
    - name: range_priority_valid
      expectation: "priority IS NULL OR priority BETWEEN 1 AND 5"
      severity: high
      
    - name: range_urgency_valid
      expectation: "urgency IS NULL OR urgency BETWEEN 1 AND 3"
      severity: medium
      
    - name: range_impact_valid
      expectation: "impact IS NULL OR impact BETWEEN 1 AND 3"
      severity: medium

  derived_field_checks:
    - name: derived_is_resolved_consistency
      expectation: "is_resolved = (state = 6)"
      severity: high
      
    - name: derived_is_closed_consistency
      expectation: "is_closed = (state = 7)"
      severity: high
      
    - name: derived_time_to_resolve_non_negative
      expectation: "time_to_resolve_hours IS NULL OR time_to_resolve_hours >= 0"
      severity: high

  temporal_checks:
    - name: temporal_resolved_after_opened
      expectation: "resolved_at IS NULL OR resolved_at >= opened_at"
      severity: high
      
    - name: temporal_closed_after_opened
      expectation: "closed_at IS NULL OR closed_at >= opened_at"
      severity: high
      
    - name: temporal_scd2_end_after_start
      expectation: "__END_AT IS NULL OR __END_AT > __START_AT"
      severity: critical
```

### 3.2 Table: silver.incident_current

**Purpose**: Current state view for simplified analytics  
**Partitioning**: opened_year, opened_month

#### DDL (Coder Implementation Step 5)
```sql
CREATE TABLE IF NOT EXISTS silver.incident_current (
    incident_id STRING NOT NULL COMMENT 'Unique incident identifier (sys_id)',
    number STRING COMMENT 'Human-readable incident number',
    short_description STRING COMMENT 'Brief description of the incident',
    description STRING COMMENT 'Detailed description of the incident',
    category STRING COMMENT 'Incident category',
    subcategory STRING COMMENT 'Incident subcategory',
    priority INT COMMENT 'Incident priority (1-5)',
    urgency INT COMMENT 'Incident urgency level',
    impact INT COMMENT 'Incident impact level',
    severity INT COMMENT 'Incident severity level',
    state STRING COMMENT 'Current incident state',
    state_code INT COMMENT 'Numeric state code',
    active BOOLEAN COMMENT 'Whether incident is active',
    assigned_to STRING COMMENT 'User assigned to incident',
    assignment_group STRING COMMENT 'Group assigned to incident',
    caller_id STRING COMMENT 'User who reported the incident',
    opened_at TIMESTAMP COMMENT 'When incident was opened',
    closed_at TIMESTAMP COMMENT 'When incident was closed',
    resolved_at TIMESTAMP COMMENT 'When incident was resolved',
    sys_created_on TIMESTAMP COMMENT 'System creation timestamp',
    sys_updated_on TIMESTAMP COMMENT 'System last update timestamp',
    time_to_resolution_hours DOUBLE COMMENT 'Hours from opened to resolved',
    time_to_close_hours DOUBLE COMMENT 'Hours from opened to closed',
    is_resolved BOOLEAN COMMENT 'Derived: Whether incident is resolved',
    is_closed BOOLEAN COMMENT 'Derived: Whether incident is closed',
    opened_date DATE COMMENT 'Derived: Date portion of opened_at for partitioning',
    opened_year INT COMMENT 'Derived: Year of opened_at',
    opened_month INT COMMENT 'Derived: Month of opened_at',
    _bronze_loaded_at TIMESTAMP COMMENT 'When record was loaded to Bronze',
    _silver_loaded_at TIMESTAMP COMMENT 'When record was loaded to Silver',
    _source_system STRING COMMENT 'Source system identifier'
)
USING DELTA
PARTITIONED BY (opened_year, opened_month)
COMMENT 'Silver layer incident table containing current non-deleted records with derived analytics columns'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'silver'
);
```

#### CDC Implementation (Coder Implementation Step 6)
```sql
-- CDC Processing for incident_current (current records only)
MERGE INTO silver.incident_current AS target
USING (
    SELECT 
        sys_id AS incident_id,
        number,
        short_description,
        description,
        category,
        subcategory,
        CAST(priority AS INT) AS priority,
        CAST(urgency AS INT) AS urgency,
        CAST(impact AS INT) AS impact,
        CAST(severity AS INT) AS severity,
        state_name AS state,
        CAST(state AS INT) AS state_code,
        active,
        assigned_to_id AS assigned_to,
        assignment_group_id AS assignment_group,
        caller_id_id AS caller_id,
        opened_at,
        closed_at,
        resolved_at,
        sys_created_on,
        sys_updated_on,
        time_to_resolve_hours AS time_to_resolution_hours,
        time_to_close_hours,
        is_resolved,
        is_closed,
        opened_date,
        YEAR(opened_at) AS opened_year,
        MONTH(opened_at) AS opened_month,
        __START_AT AS _bronze_loaded_at,
        CURRENT_TIMESTAMP() AS _silver_loaded_at,
        'ServiceNow' AS _source_system
    FROM silver.incident_flattened
    WHERE __END_AT IS NULL 
      AND (_databricks_deleted IS NULL OR _databricks_deleted = FALSE)
) AS source
ON target.incident_id = source.incident_id

WHEN MATCHED THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *;

-- Remove deleted records
DELETE FROM silver.incident_current
WHERE incident_id NOT IN (
    SELECT sys_id 
    FROM silver.incident_flattened 
    WHERE __END_AT IS NULL 
      AND (_databricks_deleted IS NULL OR _databricks_deleted = FALSE)
);
```

---

## 4. Gold Layer Design

### 4.1 Table: gold.incident_daily_metrics

**Grain**: One row per calendar date  
**Purpose**: Daily incident metrics for operational reporting

#### DDL (Coder Implementation Step 7)
```sql
CREATE TABLE IF NOT EXISTS gold.incident_daily_metrics (
    metric_date DATE NOT NULL,
    total_incidents_opened INTEGER,
    total_incidents_resolved INTEGER,
    total_incidents_closed INTEGER,
    critical_incidents_opened INTEGER,
    high_incidents_opened INTEGER,
    avg_time_to_resolve_hours DOUBLE,
    p50_time_to_resolve_hours DOUBLE,
    p95_time_to_resolve_hours DOUBLE,
    sla_met_count INTEGER,
    sla_missed_count INTEGER,
    sla_compliance_rate DECIMAL(5,2),
    avg_reassignments DOUBLE,
    avg_reopens DOUBLE,
    _gold_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _source_table STRING DEFAULT 'silver.incident_flattened',
    _record_hash STRING,
    PRIMARY KEY (metric_date)
)
USING DELTA
COMMENT 'Daily aggregated incident metrics for operational reporting. Grain: one row per calendar date.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```