"""
DSPy Signatures for Lakehouse Architect Agent.

These signatures codify the design methodology from the Playbook,
enabling structured reasoning through the medallion architecture phases.
"""

import dspy


# =============================================================================
# Phase 1: Source Qualification & Bronze Layer Signatures
# =============================================================================


class ClassifyDataSource(dspy.Signature):
    """Classify a data source table according to the Lakehouse data classification framework.

    Classification Categories:
    - Historized: Strategically flattened data through time, can get very large
    - Fact: Core business events (e.g., OrderFact, CartFact), joined with dimensions
    - Dimensional: Slow-changing dimension tables with temporal tracking (SCD Type 2)
    - Lookup: Simple reference tables with common keys, candidates for flattening
    """

    table_name: str = dspy.InputField(desc="Name of the source table")
    table_schema: str = dspy.InputField(desc="DDL or column definitions of the table")
    sample_data: str = dspy.InputField(
        desc="Sample rows from the table (optional)", default=""
    )
    business_context: str = dspy.InputField(
        desc="Business context about how this table is used", default=""
    )

    classification: str = dspy.OutputField(
        desc="One of: Historized, Fact, Dimensional, Lookup"
    )
    reasoning: str = dspy.OutputField(desc="Explanation for the classification")
    design_considerations: str = dspy.OutputField(
        desc="Key considerations for lakehouse design based on classification"
    )


class AssessDataMutability(dspy.Signature):
    """Determine if source data is final or correctable, and recommend ingestion strategy.

    Mutability Types:
    - Final: Never changed after capture → Simple append strategy
    - Correctable: Can be modified later → Requires CDC/merge strategy

    For correctable data, identify the correction pattern (Type 2 preferred for Delta Lake).
    """

    table_name: str = dspy.InputField(desc="Name of the source table")
    table_schema: str = dspy.InputField(desc="DDL or column definitions")
    has_update_timestamp: bool = dspy.InputField(
        desc="Whether table has an update timestamp column"
    )
    has_primary_key: bool = dspy.InputField(
        desc="Whether table has a clear primary key"
    )
    business_context: str = dspy.InputField(
        desc="How data changes over time in this table", default=""
    )

    mutability_type: str = dspy.OutputField(desc="One of: Final, Correctable")
    change_pattern: str = dspy.OutputField(
        desc="For correctable: Type 1 (overwrite), Type 2 (historize), or Custom"
    )
    ingestion_strategy: str = dspy.OutputField(
        desc="Recommended ingestion approach: full_pull, incremental_timestamp, cdc_merge"
    )
    reasoning: str = dspy.OutputField(desc="Explanation for the assessment")


class AnalyzeSchemaPattern(dspy.Signature):
    """Identify source schema pattern and plan transformation to Data Lake Schema.

    Schema Patterns:
    - Star Schema: Fact surrounded by denormalized dimensions → Usually efficient to migrate
    - Snowflake Schema: Fact with normalized dimension chains → Requires detailed ERD mapping
    - Normalized OLTP: Highly normalized transactional schema → Significant transformation needed
    - Data Lake Schema: Already flattened (target state)
    """

    source_tables: str = dspy.InputField(
        desc="List of related tables with their schemas"
    )
    relationships: str = dspy.InputField(
        desc="Foreign key relationships between tables"
    )
    primary_fact_table: str = dspy.InputField(
        desc="The main fact table if identifiable", default=""
    )

    schema_pattern: str = dspy.OutputField(
        desc="One of: Star, Snowflake, Normalized_OLTP, Data_Lake"
    )
    transformation_complexity: str = dspy.OutputField(
        desc="One of: Low, Medium, High"
    )
    denormalization_plan: str = dspy.OutputField(
        desc="Which dimensions to flatten vs keep separate"
    )
    reasoning: str = dspy.OutputField(desc="Detailed analysis of the schema pattern")


class DesignBronzeTable(dspy.Signature):
    """Design Bronze layer table specification following Lakehouse best practices.

    Bronze Layer Principles:
    - Zero transformation from source (mirror perfectly)
    - Add metadata columns: _ingested_at, _source_file, _schema_version
    - Route to proper storage (NOT DBFS blob storage)
    - Define ingestion strategy based on source type
    """

    table_name: str = dspy.InputField(desc="Source table name")
    table_schema: str = dspy.InputField(desc="Source table DDL/columns")
    data_classification: str = dspy.InputField(
        desc="Historized, Fact, Dimensional, or Lookup"
    )
    mutability_type: str = dspy.InputField(desc="Final or Correctable")
    estimated_row_count: str = dspy.InputField(
        desc="Approximate row count", default=""
    )
    update_frequency: str = dspy.InputField(
        desc="How often source data changes", default=""
    )

    bronze_table_ddl: str = dspy.OutputField(
        desc="CREATE TABLE statement for Bronze table with metadata columns"
    )
    storage_location: str = dspy.OutputField(
        desc="Recommended storage path pattern"
    )
    ingestion_strategy: str = dspy.OutputField(
        desc="full_pull_with_timestamp, incremental_by_timestamp, or cdc_with_customer_approval"
    )
    validation_checks: str = dspy.OutputField(
        desc="List of validation checks: distinct_key_count, time_range, file_count, avg_file_size"
    )


# =============================================================================
# Phase 2: Silver Layer Signatures
# =============================================================================


class DesignTransformationMapping(dspy.Signature):
    """Design Bronze → Silver transformation mapping following Lakehouse principles.

    Transformation Principles:
    - Flatten low-cardinality dimensions into fact tables (e.g., state/address into customer)
    - Keep high-cardinality dimensions separate (e.g., item dimension separate from transactions)
    - Apply all business mappings at this layer
    - Implement CDC: mark historical records with _valid_to timestamp
    """

    bronze_table: str = dspy.InputField(desc="Bronze table name and schema")
    target_silver_table: str = dspy.InputField(desc="Target Silver table name")
    related_bronze_tables: str = dspy.InputField(
        desc="Other Bronze tables to join/flatten", default=""
    )
    business_mappings: str = dspy.InputField(
        desc="Business transformation rules from customer", default=""
    )
    cardinality_info: str = dspy.InputField(
        desc="Cardinality information for dimensions", default=""
    )

    transformation_sql: str = dspy.OutputField(
        desc="SQL or PySpark transformation logic"
    )
    flattened_dimensions: str = dspy.OutputField(
        desc="List of dimensions flattened into the table"
    )
    kept_separate_dimensions: str = dspy.OutputField(
        desc="List of high-cardinality dimensions kept as separate tables"
    )
    cdc_columns: str = dspy.OutputField(
        desc="CDC tracking columns: _valid_from, _valid_to, _is_current"
    )
    data_type_conversions: str = dspy.OutputField(
        desc="Any data type conversions applied"
    )


class DesignSilverTable(dspy.Signature):
    """Design Silver layer table specification with partitioning and optimization.

    Silver Layer Purpose:
    - Persisted location for validations
    - Security checkpoint before Gold
    - Type 2 history storage for tables that don't need this detail in Gold
    - All mappings/transformations completed here
    """

    table_name: str = dspy.InputField(desc="Silver table name")
    transformation_mapping: str = dspy.InputField(
        desc="The transformation logic from Bronze"
    )
    primary_key_columns: str = dspy.InputField(desc="Primary key column(s)")
    common_query_patterns: str = dspy.InputField(
        desc="How downstream queries filter this data", default=""
    )
    join_keys: str = dspy.InputField(
        desc="Columns commonly used for joins", default=""
    )
    estimated_daily_records: str = dspy.InputField(
        desc="Expected daily record volume", default=""
    )

    silver_table_ddl: str = dspy.OutputField(
        desc="CREATE TABLE statement with partitioning"
    )
    partition_strategy: str = dspy.OutputField(
        desc="Partition columns and rationale (use p_ prefix)"
    )
    zorder_columns: str = dspy.OutputField(
        desc="Columns to Z-order for optimal join performance"
    )
    validation_rules: str = dspy.OutputField(
        desc="Data quality rules to apply at Silver layer"
    )
    optimization_schedule: str = dspy.OutputField(
        desc="When to run OPTIMIZE and compaction"
    )


# =============================================================================
# Phase 3: Gold Layer Signatures
# =============================================================================


class DesignGoldTable(dspy.Signature):
    """Design Gold layer table following enterprise consumption patterns.

    Gold Layer Definition:
    - Physical layer for broad user consumption
    - Contains truth as it exists at current moment
    - May become source of truth for enterprise datasets
    - Must be efficient, easy to understand, and accurate

    Table Types:
    - Current table (e.g., campaign): Only currently accurate information
    - History table (e.g., campaign_hist): Type 2 structure with full history
    """

    silver_source_table: str = dspy.InputField(desc="Source Silver table(s)")
    table_purpose: str = dspy.InputField(
        desc="Business purpose and consumer use cases"
    )
    requires_history: bool = dspy.InputField(
        desc="Whether full history is needed for consumers"
    )
    history_retention_period: str = dspy.InputField(
        desc="How long to retain historical records", default=""
    )
    consumer_query_patterns: str = dspy.InputField(
        desc="How consumers will query this data", default=""
    )

    current_table_ddl: str = dspy.OutputField(
        desc="CREATE TABLE for current-state table (table_name_t)"
    )
    history_table_ddl: str = dspy.OutputField(
        desc="CREATE TABLE for history table (table_name_t_hist) if needed"
    )
    table_type: str = dspy.OutputField(
        desc="Type 2 SCD, Slow-changing Dimension, or Type 1 Lookup"
    )
    partition_strategy: str = dspy.OutputField(
        desc="Partitioning for Gold tables (use p_ prefix)"
    )
    zorder_strategy: str = dspy.OutputField(desc="Z-ordering for Gold tables")


class DesignGovernanceRules(dspy.Signature):
    """Design data governance rules for Gold layer validation.

    Rule Categories (select up to 5 per table):
    - Range validation: count BETWEEN x AND y
    - Cross-field validation: SUM(price) WHERE rule_a <= value_x
    - Statistical validation: STDDEV(column_x) <= n
    - Trend validation: COUNT(DISTINCT today) >= COUNT(DISTINCT yesterday)
    - Boundary validation: MAX(column_x) <= n
    - Temporal validation: MAX(timestamp_x) <= CURRENT_DATE
    """

    table_name: str = dspy.InputField(desc="Gold table name")
    table_schema: str = dspy.InputField(desc="Gold table schema")
    business_rules: str = dspy.InputField(
        desc="Business rules provided by customer (up to 5)"
    )
    critical_columns: str = dspy.InputField(
        desc="Columns critical for data quality", default=""
    )

    governance_rules: str = dspy.OutputField(
        desc="List of up to 5 validation rules with SQL expressions"
    )
    failure_handling: str = dspy.OutputField(
        desc="How to handle records that fail validation"
    )
    rule_execution_timing: str = dspy.OutputField(
        desc="When to execute rules: after_silver_landing or before_gold_move"
    )


class DesignGoldView(dspy.Signature):
    """Design consumer-facing view for Gold table following strict conventions.

    View Requirements:
    - Consumers access views, never tables directly
    - Name every column explicitly (NO SELECT *)
    - Map partition columns in predicate for pruning
    - Alias every column for future remapping flexibility

    Benefits:
    - No performance impact
    - Schema changes don't require data rebuilds
    - Enables complex security requirements
    """

    gold_table_name: str = dspy.InputField(
        desc="Physical Gold table name (with _t suffix)"
    )
    table_schema: str = dspy.InputField(desc="Gold table columns")
    partition_columns: str = dspy.InputField(desc="Partition columns (with p_ prefix)")
    consumer_facing_name: str = dspy.InputField(
        desc="View name consumers will use (no suffix)"
    )

    view_ddl: str = dspy.OutputField(
        desc="CREATE OR REPLACE VIEW statement with explicit columns and partition predicate"
    )
    column_mappings: str = dspy.OutputField(
        desc="Current column mappings for documentation"
    )
    schema_evolution_notes: str = dspy.OutputField(
        desc="How to handle future schema changes via view remapping"
    )


# =============================================================================
# Phase 4: Security & Architecture Signatures
# =============================================================================


class DesignDatabaseArchitecture(dspy.Signature):
    """Design database and storage architecture for the Lakehouse.

    Database Structure:
    - dw_bronze: Raw data copies
    - dw_silver: Transformed, validated data
    - gold_etl: Persisted Gold tables (ETL access)
    - gold: Consumer-facing views only
    - gold_hist: Historical data tables (optional)

    Critical Rules:
    - Gold must be in dedicated bucket/storage account
    - Never use DBFS blob storage
    - Separate Gold from pre-Gold to avoid contention
    """

    project_name: str = dspy.InputField(desc="Project or domain name")
    cloud_provider: str = dspy.InputField(desc="AWS, Azure, or GCP")
    bronze_tables: str = dspy.InputField(desc="List of Bronze tables")
    silver_tables: str = dspy.InputField(desc="List of Silver tables")
    gold_tables: str = dspy.InputField(desc="List of Gold tables")
    consumer_groups: str = dspy.InputField(
        desc="User groups that will consume data", default=""
    )

    database_definitions: str = dspy.OutputField(
        desc="CREATE DATABASE statements with storage locations"
    )
    storage_architecture: str = dspy.OutputField(
        desc="Storage account/bucket structure"
    )
    acl_configuration: str = dspy.OutputField(
        desc="GRANT/REVOKE statements for access control"
    )
    naming_conventions: str = dspy.OutputField(
        desc="Applied naming conventions summary"
    )


class AssessRisks(dspy.Signature):
    """Assess implementation risks and recommend mitigations.

    Risk Escalation Triggers:
    - Non-Type-2 Large Sources: No incremental capture strategy
      → Document explicit plan, get customer signoff
    - Unclear Mappings: Transformation logic not clearly defined
      → HALT - escalate immediately, project at risk
    - Data Quality Issues: Validation failures in testing
      → Delegate to customer SME for investigation
    """

    source_inventory: str = dspy.InputField(desc="All source tables and their types")
    mapping_clarity: str = dspy.InputField(
        desc="Assessment of mapping document clarity"
    )
    large_non_type2_tables: str = dspy.InputField(
        desc="List of large tables without Type 2 support", default=""
    )
    data_sensitivity: str = dspy.InputField(
        desc="Data classification levels present", default=""
    )

    identified_risks: str = dspy.OutputField(
        desc="List of identified risks with severity"
    )
    mitigation_strategies: str = dspy.OutputField(
        desc="Recommended mitigation for each risk"
    )
    escalation_required: bool = dspy.OutputField(
        desc="Whether immediate escalation is needed"
    )
    escalation_reason: str = dspy.OutputField(
        desc="Reason for escalation if required"
    )


# =============================================================================
# Orchestration Signature
# =============================================================================


class GenerateDesignDocument(dspy.Signature):
    """Generate the complete DESIGN_DOCUMENT.md from all design components.

    Document Structure:
    1. Executive Summary
    2. Source Analysis
    3. Bronze Layer Design
    4. Silver Layer Design
    5. Gold Layer Design
    6. Security & Access Control
    7. Operational Considerations
    8. Risk Register
    """

    source_analysis: str = dspy.InputField(
        desc="Compiled source classification and assessment"
    )
    bronze_design: str = dspy.InputField(desc="Bronze layer specifications")
    silver_design: str = dspy.InputField(desc="Silver layer specifications")
    gold_design: str = dspy.InputField(desc="Gold layer specifications")
    security_design: str = dspy.InputField(desc="Security and ACL configuration")
    risk_assessment: str = dspy.InputField(desc="Risk register and mitigations")
    project_context: str = dspy.InputField(
        desc="Project name and business context", default=""
    )

    design_document: str = dspy.OutputField(
        desc="Complete DESIGN_DOCUMENT.md in markdown format"
    )

