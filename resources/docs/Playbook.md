# Data Lakehouse Implementation Playbook

This playbook provides structured guidance for implementing a Bronze → Silver → Gold data lakehouse architecture on Databricks.

---

## Phase 1: Land & Solidify Bronze

### 1.1 Overview

**Bronze Layer Definition:** A physical layer containing a well-structured, properly formatted copy of source data optimized for the primary data processing engine (Databricks). This copy perfectly mirrors the source with zero changes, with occasional exception of additional metadata columns added on arrival.

> **Important:** Making this copy to bronze is imperative for demonstrating best practices. The source from which data is copied may become the bronze source in production, but this varies case-by-case.

> **Exception:** If the customer wants to see how Databricks combines sources from multiple locations, identify a small lookup table in a common RDBMS and capture it at runtime through the connector instead of copying to bronze. Document the pros and cons of this architecture.

---

### 1.2 Qualifying Sources

Before working with customer data, qualify their data to minimize issues in future stages.

#### 1.2.1 Data Classification by Class

| Class | Description |
|-------|-------------|
| **Historized** | Strategically flattened data through time providing resolution through history. Can get very large. Often stored in unfriendly formats—warrant deeper analysis before adding to scope. |
| **Fact** | Core business events (e.g., `CartFact` identifies cart ownership and items by distinct cart). Joined with dimensional tables for details. |
| **Dimensional** | Slow-changing dimension tables with "As Of" columns or ability to identify values at a given point in time. |
| **Lookups** | Simple tables with ad-hoc data and common keys for joining. |

#### 1.2.2 Data Type (Managed Finality)

Determine if data is:
- **Final:** Never changed after capture
- **Correctable:** Can be modified later

If correctable, identify the correction pattern. Delta Lake typically pursues **Type 2** for most change data.

#### 1.2.3 Schema Design Patterns

| Pattern | Description | Migration Complexity |
|---------|-------------|---------------------|
| **Star Schema** | Fact surrounded by denormalized dimensional tables | Usually efficient to migrate |
| **Snowflake Schema** | Fact surrounded by normalized dimensional tables with deep relationships | Often complicated—requires detailed ERDs to map back to denormalized sets |
| **Data Lake Schema** | Flattened structure maintaining dimensions only for high-cardinality data | Target state |

**Design Guidance:**
- **Flatten low-cardinality dimensions** (e.g., include state/address in customer fact table—these rarely change)
- **Keep high-cardinality dimensions separate** (e.g., never combine item fact with item dimension since customer × item × attributes × time = extremely high cardinality)

#### 1.2.4 Data Sensitivity

Understand the customer's data classification strategy:
- Internal Only
- Restricted Confidential
- Legal Hold
- Regulatory Confidential

> **Action:** Ask explicitly about data classification at the beginning. Take appropriate measures to keep customer data safe.

---

### 1.3 Internal Review (Pre-Implementation)

Schedule a **1.5-hour review** with RSA, SC, and offshore consultant before implementation.

#### Review Checklist

| Item | Owner | Actions |
|------|-------|---------|
| **Source Qualifications Assessment** | RSA/SC | Identify issues/risks with source datasets, especially regarding incremental data capture |
| **Mappings Document** | RSA/SC | Verify completeness and clarity for Bronze → Silver transformations |

#### Risk Escalation Triggers

> **⚠️ RISK: Non-Type-2 Medium/Large Sources**
> - Must have explicit ingestion plan
> - Get documented customer signoff on approach
> - RSA identifies long-term options if current approach isn't production-viable
> - Adjust scope with project manager if necessary

> **⚠️ RISK: Unclear Mappings**
> - If mappings are not clearly understood, escalate immediately
> - Notify project manager and customer
> - **Project WILL NOT be successful without clear mappings**
> - Put project on hold until satisfactory documentation is provided

---

### 1.4 Implementation Steps

#### Step 1: Design Simplified Star Schema for Bronze

- Convert IDs to values where cardinality remains low
- Remove lookup/dimension tables that can be flattened

#### Step 2: Create Bronze Database

```sql
CREATE DATABASE dw_bronze LOCATION 's3://...'
-- or
CREATE DATABASE dw_bronze LOCATION 'dbfs:/mnt/...'
-- where mount links to production-grade storage
```

> **⚠️ Warning:** Do NOT use DBFS blob storage. It has limited throughput and security features, negatively impacting enterprise customers. Data lakes should NOT be stored in blob storage or root object store.

#### Step 3: Determine Ingestion Strategy

**Option A: Single Pull (Snapshot)**

Challenges:
- Time-consuming to simulate new data and pipelines
- Critical to demonstrate incremental capabilities (key Databricks/Delta value)

**Recommended Approach:**
1. Ingest ~60% of data into bronze directly
2. Land remaining data into staging database to simulate future pulls
3. Note Delta version of bronze tables
4. Build script to reset to those versions using Time Travel for re-simulation
5. Size simulated batches similar to production batch sizes

**Option B: Repeat Pull from Sources**

> **⚠️ Caution:** Easy to overwhelm source systems with Spark during business hours. Know when to pull and how much.

**Recommended Approach:**
1. Break ingestion into two sub-epics:
   - **Historical data pull** (relative to snapshot date)
   - **New data pull** (priority focus)
2. Start with very small sample of historical data
3. Test landing processes with small snapshots
4. Identify business rules to filter "future" snapshots for subsequent load simulation

**Ingestion Rules by Source Type:**

| Source Type | Strategy |
|-------------|----------|
| Small, non-Type-2 | Pull entire dataset each run, append timestamp. Historize to archive later. |
| Type-2 | Pull records where update timestamp ≥ existing bronze timestamp (by key) |
| Large, non-Type-2 | Work with customer to identify appropriate strategy. CDC upsert handled in Bronze → Silver. |

**Handling Deletes:**
- Identify customer's desired delete method for data lake
- Source deletes ≠ data lake deletes automatically
- Soft deletes typically more valuable but may require regulatory compliance processes

#### Step 4: Ingest to Bronze as Delta

```sql
-- Target location should be database table in metastore routed to proper storage
-- Append ingestion timestamp and schema version (start at 1.0)
```

**Connector Best Practices:**
- Use proper connector for each source (e.g., don't use default JDBC for CosmosDB)
- Review documentation for unfamiliar large-scale sources
- Request peer consultation when needed
- Identify custom connector properties early (shards, partitions, indexes, RSUs)
- Notify SMEs ASAP about required changes

#### Step 5: Validate Each Landing

| Validation | Purpose |
|------------|---------|
| Count of distinct keys | Sanity check throughout process |
| Time range | Understand working time ranges |
| Number of files per table/partition | Monitor file proliferation |
| Average file size | Avoid ingesting many small files (slow to write/compact) |

#### Step 6: Optimize (Compaction)

**Key Points:**
- Delta compaction is a single-line operation (major selling point)
- Develop optimized schedule based on ingest periodicity and downstream requirements

**Best Practices:**
- Optimize with smaller clusters in off-hours (cheaper compute, less throttling)
- Use separate small cluster for compaction (not the ingestion cluster)
- Parallelize compaction across all tables
- **Do NOT** apply auto-optimize/auto-compact to tables that will be z-ordered (optimization during write is lost during subsequent z-order)

#### Step 7: Compute Statistics

```sql
ANALYZE TABLE <table_name> COMPUTE STATISTICS FOR COLUMNS joinKey1, joinKey2
```

Calculate statistics for:
- Entire table
- Key columns specifically

---

## Phase 2: Land & Solidify Silver

### 2.1 Overview

**Silver Layer Definition:** A physical layer structured similar to Gold (the final, high-performance structure). Silver provides:
- Persisted location for validations
- Security measure before impacting customer-facing tables
- Type 2 history storage for Gold tables that don't need this detail

> **Key Insight:** Omitting unnecessary versions improves performance and lowers production Gold costs. All transformations (mappings) are completed between Bronze and Silver.

---

### 2.2 Offshore Resource Utilization

By this phase, offshore resource should:
- Be familiar with implementation process and Bronze layer approach
- Have dedicated time understanding the customer's mapping document
- Highlight mapping issues or foreseen challenges

> **Success Factor:** Efficient offshore resource utilization is key to successful implementation within budget.

---

### 2.3 Mappings Approach

**Default:** Complete all mappings in notebooks.

| Package | IDE Recommendation |
|---------|-------------------|
| **Foundational** | IDE out of scope—too time-consuming to set up |
| **Extended/Optimized** | More time available; helper functions provide value |

**IDE Considerations:**
- Must set up IDE for offshore resource too
- Customer must confirm if IDE is acceptable
- Customer cannot require IDE in Foundational/Extended
- For Optimized with IDE request: offshore works in notebook, SC merges to IDE

**Important:** Know if customer is moving to Optimized with scaling requirements. Scale requirements can profoundly impact mapping design and table definitions.

---

### 2.4 Implementation Steps

#### Step 1: Apply Mappings

> **⚠️ ALERT: Primary Risk Area**
> - Mappings must be clearly defined by customer
> - This is where engagements are most likely to fall behind
> - Warning signs should flash if engagement begins without near-perfectly defined mappings

#### Step 2: Complete Validations

- Keep validations simple and strategic
- Much validation should have been done in foundational delivery

#### Step 3: Implement Change Data Capture (CDC)

- Ensure only current data is active in Silver
- Mark historical records with `end_date` or `expired` column (timestamp type)

#### Step 4: Design Partitioning Strategy

- Base on use-case requirements
- Validate delivery of well-formed files

#### Step 5: Design Z-Ordering and Optimization

- Enable Silver layer to efficiently accept upserts
- Optimize delivery to Gold

---

## Phase 3: Develop Gold Layer

### 3.1 Overview

**Gold Layer Definition:** The physical layer from which the broad user group consumes data. Contains truth as it exists at current moment (given delayed periodicity).

**Key Characteristics:**
- May become source of truth for enterprise datasets
- Must be efficient, easy to understand, and accurate
- Often split into current and history tables

**Example Pattern:**
- `campaign` table: Only currently accurate information (optimized for broad usage)
- `campaign_hist` table: Type 2 structure with full history

---

### 3.2 View Abstraction Layer

**Best Practice:** Customers access views with 1-to-1 mappings to Gold tables, not tables directly.

**Benefits:**
- No performance impact
- Obfuscation layer simplifies future changes
- Enables complex security requirements
- Schema changes don't require data rebuilds

**Schema Change Example:**

```sql
-- 1. Add new column to table
ALTER TABLE campaign_t ADD COLUMN columnName_v2 STRING;

-- 2. Backfill if necessary (pipeline continues running)

-- 3. Update view mapping
CREATE OR REPLACE VIEW campaign AS
SELECT
    columnName_v2 AS columnName,  -- Remapped
    ...
FROM gold.campaign_t;
```

> **Result:** Downstream consumers never know a change was made—data is "magically" improved.

---

### 3.3 Governance Framework

Gold data must be high-quality. Implement validations before data moves to Gold.

#### Governance Rule Examples

| Rule Type | Example |
|-----------|---------|
| Range validation | Count should be between x and y |
| Cross-field validation | Sum of price given rule A must be ≤ value x given rule Z |
| Statistical validation | Standard deviation of column x must be ≤ n |
| Trend validation | Distinct values today must be ≥ distinct values yesterday |
| Boundary validation | Max value of column x must be ≤ n |
| Temporal validation | Max timestamp of column x must be ≤ today |

> **Customer Requirement:** Customer must provide ≤ 5 business rules to apply to the data.

---

### 3.4 Implementation Steps

#### Step 1: Create Gold Database

```sql
CREATE DATABASE gold LOCATION 's3://dedicated-gold-bucket/...'
```

**Architecture Recommendations:**
- Place Gold in dedicated bucket/storage account
- Separating Gold from pre-Gold alleviates storage system pressure
- Heavy ETL in pre-Gold won't contend with customer Gold access
- Provides additional security capabilities

**Database Structure:**

| Database | Purpose |
|----------|---------|
| `gold_etl` | ETL tables (persisted data) |
| `gold` | Consumer access (views only) |
| `gold_hist` | History tables (optional, for cleaner consumption area) |

**ACL Configuration:**

```sql
-- Create database with ACLs cluster
-- Validate proper ownership before continuing

-- Pipeline databases (gold_t, silver, bronze):
REVOKE ALL PRIVILEGES ON DATABASE gold_t FROM consumer_group;

-- Consumer database (gold):
GRANT SELECT, READ_METADATA ON DATABASE gold TO consumer_group;
```

> **Result:** Consumers only see the gold database with views; pipeline databases are invisible.

#### Step 2: Build Table Inventory

For each table, document:
- Source
- Type (Type 2, Slow-changing Dimension, Type 1 Lookup, etc.)
- Need for current vs. history versions
- Applicable governance rules

#### Step 3: Implement Rules Engine

Create scaffolding to simplify rule addition:
- Put package, class, and object in a notebook to run inline
- Organize instantiated Rules by business hierarchy/logic
- Complex column definitions should derive from UDFs
- Each UDF method takes 0+ columns (Column type) and outputs single column

**Python Users:** Use PandasUDFs if `spark.sql.functions` doesn't provide needed functions. Consult RSA if neither works.

#### Step 4: Handle Rule Failures

- Review requirements for handling failed data per rule
- Expand rules engine to generate failure reports
- Route failed records to centralized location for analyst review
- Consider "smart reprocessing" for common break patterns

#### Step 5: Implement Unit Testing

```python
# Simple validation example
assert result.count() > 0, "Result should not be empty"
require(column_value <= max_threshold, "Value exceeds threshold")
```

> **Note:** Full unit/integration testing is NOT in scope, but simple assertions facilitate upselling.

#### Step 6: Schedule Rule Execution

| Timing | Approach |
|--------|----------|
| After Silver landing | Use landing cluster for validations (preferred—more time for fix/repair) |
| Before Gold move | Validate at Gold transition point |

#### Step 7: Enable Metadata Capture

Capture stateful metadata at column and table levels:
- Identify what must be captured
- Assess capture cost vs. value
- Determine storage location

#### Step 8: Design Table Types

Based on Gold definition, identify:
- Customer-facing tables as Type 2 with historical data
- Required history retention periods
- Tables needing `*_hist` variants

#### Step 9: Validate Rules Against Data

**Process:**
1. Review all failures with customer
2. Ask customer SMEs to validate source data
3. Determine: pipeline error vs. source data error
4. If pipeline error: validate logic (provided incorrectly vs. implemented incorrectly)
5. Ask customer to fix issues

> **Critical:** Delegate data quality investigation to customer SMEs. You don't have time to chase issues. This also increases SME ability to take over the project.

**Escalation Path:**
1. Customer SME engagement (primary)
2. Offshore resource (if SME won't engage or isn't progressing)

#### Step 10: Design Partitioning and Z-Ordering

- Review customer requirements and usage patterns
- Design strategies for live tables and `_hist` tables
- **Do NOT use bucketing** (consult RSA if you feel it's necessary)

#### Step 11: Land and Validate Gold Data

Validations:
- Proper number of properly sized files per partition
- Run complete profile on partitions/folders
- Review for skew and computation issues

#### Step 12: Apply Naming Conventions

| Convention | Example | Purpose |
|------------|---------|---------|
| Partition columns | `p_yyyymm`, `p_campaign_id` | Easy identification for downstream users |
| Table prefix | `t_tablename`, `tablename_t` | Distinguish tables from views in access layer |
| History suffix | `t_tablename_hist` | Identify history tables |

#### Step 13: Create Views

**Critical:** Follow these instructions precisely.

```sql
CREATE OR REPLACE VIEW campaign_hist AS
SELECT
    g_ct_hist.a AS a,
    g_ct_hist.b AS b,
    g_ct_hist.p_campaign_id AS p_campaign_id
FROM gold.campaign_t_hist g_ct_hist
WHERE p_campaign_id = g_ct_hist.p_campaign_id;
```

**Requirements:**
- **Name every column explicitly** (DO NOT use `SELECT *`)
- **Map partition columns in predicate** for partition pruning push down
- **Alias every column**

> **Schema Change Benefit:** Remapping is trivial: `g_ct_hist.b_v2 AS b`

#### Step 14: Demonstrate Security Features

Demonstrate table ACLs (demonstration only, not production implementation):

1. Create/use test user
2. Create groups: `data_engineer`, `gold_consumer`
3. Assign users to groups
4. Configure access:
   - `data_engineer`: Access to all tables
   - `gold_consumer`: Access only to Gold views (not underlying tables)
5. Test and validate

> **Note:** Table ACLs require high concurrency clusters with ACLs enabled. For Scala users, revoke permissions using storage account permissions and user passthrough.

---

## Phase 4: Documentation and Close

### 4.1 Timing

| Package | Documentation Timing |
|---------|---------------------|
| Foundation only | End of Foundation (minimum 1 full day) |
| Continuing to Extended/Optimized | May reschedule, but 1 day per package level required |

### 4.2 Requirements

> **⚠️ Never skip documentation.** It's critical to leave the customer with clear information about completed work. Keep an eye on this deliverable as you approach engagement end.

**Best Practice:** Document along the way to speed up the final documentation process.

---

## Quick Reference

### Database Naming Convention

| Layer | Database Name | Contents |
|-------|---------------|----------|
| Bronze | `dw_bronze` | Raw data copies |
| Silver | `dw_silver` | Transformed, validated data |
| Gold (tables) | `gold_etl` or `gold_t` | Persisted Gold tables |
| Gold (views) | `gold` | Consumer-facing views |
| Gold (history) | `gold_hist` | Historical data (optional) |

### Key SQL Commands

```sql
-- Create database with proper storage
CREATE DATABASE dw_bronze LOCATION 's3://bucket/bronze/';

-- Compute statistics
ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS col1, col2;

-- ACL: Revoke consumer access to pipeline databases
REVOKE ALL PRIVILEGES ON DATABASE silver FROM consumer_group;

-- ACL: Grant consumer access to Gold views
GRANT SELECT, READ_METADATA ON DATABASE gold TO consumer_group;
```

### Risk Escalation Summary

| Risk | Trigger | Action |
|------|---------|--------|
| Non-Type-2 large sources | No incremental capture strategy | Document plan, get signoff, escalate to PM |
| Unclear mappings | RSA/SC not confident in clarity | Escalate immediately, halt if needed |
| Data quality issues | Failures during validation | Delegate to customer SME, escalate to offshore if blocked |
