"""
DSPY Signatures for Lakehouse Design - Codifying Playbook Rules.

These signatures encode the rules from the Data Lakehouse Implementation Playbook
for Bronze → Silver → Gold architecture design.
"""

import dspy


# =============================================================================
# Phase 1: Bronze Layer Signatures (Playbook Section 1)
# =============================================================================


class QualifySource(dspy.Signature):
    """Qualify source data before Bronze ingestion.
    
    Rules from Playbook 1.2:
    - Classify by Class: Historized, Fact, Dimensional, or Lookup
    - Determine Data Finality: Final (never changed) or Correctable (can be modified)
    - Identify Schema Pattern: Star, Snowflake, or Data Lake
    - Assess Data Sensitivity: Internal, Restricted, Confidential, Legal Hold
    """
    
    table_name: str = dspy.InputField(desc="Name of the source table")
    table_schema: str = dspy.InputField(desc="DDL or column definitions")
    sample_data: str = dspy.InputField(desc="Sample data rows if available", default="")
    business_context: str = dspy.InputField(desc="Business context and usage", default="")
    
    data_class: str = dspy.OutputField(
        desc="Classification: Historized|Fact|Dimensional|Lookup"
    )
    data_finality: str = dspy.OutputField(
        desc="Finality: Final|Correctable. If Correctable, identify CDC pattern (Type 1/2)"
    )
    schema_pattern: str = dspy.OutputField(
        desc="Pattern: Star|Snowflake|DataLake - with migration complexity assessment"
    )
    flattening_recommendations: str = dspy.OutputField(
        desc="Which dimensions to flatten (low-cardinality) vs keep separate (high-cardinality)"
    )


class DesignBronzeIngestion(dspy.Signature):
    """Design Bronze layer ingestion strategy.
    
    Rules from Playbook 1.3-1.4:
    - Use proper storage (NOT DBFS blob)
    - Determine ingestion strategy: Single Pull or Repeat Pull
    - For Type-2: Pull where update_timestamp >= existing bronze timestamp
    - Handle deletes appropriately (soft vs hard)
    - Use proper connectors for each source type
    """
    
    table_name: str = dspy.InputField(desc="Table name")
    source_type: str = dspy.InputField(desc="Source type: JDBC, API, File, etc.")
    data_class: str = dspy.InputField(desc="Data class from qualification")
    data_finality: str = dspy.InputField(desc="Finality from qualification")
    has_update_timestamp: bool = dspy.InputField(desc="Has update timestamp column")
    estimated_row_count: str = dspy.InputField(desc="Estimated row count", default="")
    update_frequency: str = dspy.InputField(desc="How often data updates", default="")
    
    ingestion_strategy: str = dspy.OutputField(
        desc="Strategy: SinglePull|RepeatPull with detailed approach"
    )
    storage_location: str = dspy.OutputField(
        desc="Storage location pattern (s3:// or abfss://). NEVER use DBFS blob."
    )
    connector_config: str = dspy.OutputField(
        desc="Connector configuration and best practices for this source"
    )
    delete_handling: str = dspy.OutputField(
        desc="Delete strategy: soft delete (preferred) or hard delete with reasoning"
    )
    incremental_key: str = dspy.OutputField(
        desc="Column(s) for incremental ingestion if applicable"
    )


class DesignBronzeValidation(dspy.Signature):
    """Design Bronze layer validation checks.
    
    Rules from Playbook 1.4 Step 5:
    - Count of distinct keys (sanity check)
    - Time range understanding
    - Number of files per table/partition
    - Average file size (avoid small files)
    """
    
    table_name: str = dspy.InputField(desc="Table name")
    primary_key: str = dspy.InputField(desc="Primary key column(s)")
    
    validation_checks: str = dspy.OutputField(
        desc="SQL validation checks for post-landing validation"
    )
    optimization_strategy: str = dspy.OutputField(
        desc="Compaction and Z-ordering strategy per Playbook guidelines"
    )


# =============================================================================
# Phase 2: Silver Layer Signatures (Playbook Section 2)
# =============================================================================


class DesignSilverTransformation(dspy.Signature):
    """Design Bronze to Silver transformation.
    
    Rules from Playbook 2.3-2.4:
    - Apply all mappings in notebooks (default)
    - Mappings must be clearly defined
    - Complete CDC implementation: mark historical with end_date or expired
    - Design partitioning based on use-case
    """
    
    bronze_table: str = dspy.InputField(desc="Source Bronze table")
    bronze_schema: str = dspy.InputField(desc="Bronze table schema")
    target_silver_table: str = dspy.InputField(desc="Target Silver table name")
    transformation_rules: str = dspy.InputField(desc="Transformation rules from specs")
    
    silver_ddl: str = dspy.OutputField(
        desc="Silver table DDL with all derived columns and proper types"
    )
    transformation_sql: str = dspy.OutputField(
        desc="SQL transformation from Bronze to Silver"
    )
    cdc_implementation: str = dspy.OutputField(
        desc="CDC implementation: SCD Type 2 with __END_AT column handling"
    )
    partitioning_strategy: str = dspy.OutputField(
        desc="Partitioning columns and reasoning"
    )


class DesignDataQuality(dspy.Signature):
    """Design data quality checks for Silver layer.
    
    Rules from Playbook 2.4 Step 2:
    - Keep validations simple and strategic
    - Validate data before moving to Gold
    """
    
    table_name: str = dspy.InputField(desc="Table name")
    table_schema: str = dspy.InputField(desc="Table schema")
    business_rules: str = dspy.InputField(desc="Business rules for validation")
    
    quality_checks: str = dspy.OutputField(
        desc="Data quality checks with expectations (NOT NULL, ranges, referential)"
    )
    failure_handling: str = dspy.OutputField(
        desc="How to handle failed records: quarantine, alert, or reject"
    )


# =============================================================================
# Phase 3: Gold Layer Signatures (Playbook Section 3)
# =============================================================================


class DesignGoldTable(dspy.Signature):
    """Design Gold layer table structure.
    
    Rules from Playbook 3.1-3.2:
    - Gold is for broad user consumption
    - Often split into current and history tables
    - Always create view abstraction layer
    - Views provide 1-to-1 mapping to tables
    """
    
    silver_source: str = dspy.InputField(desc="Source Silver table")
    table_purpose: str = dspy.InputField(desc="Business purpose")
    aggregation_spec: str = dspy.InputField(desc="Aggregation specifications")
    
    gold_table_ddl: str = dspy.OutputField(
        desc="Gold table DDL with proper types"
    )
    gold_view_ddl: str = dspy.OutputField(
        desc="Consumer-facing view DDL. MUST name every column explicitly, NO SELECT *"
    )
    history_table_ddl: str = dspy.OutputField(
        desc="History table DDL if Type 2 history needed, empty otherwise"
    )


class DesignGovernance(dspy.Signature):
    """Design Gold layer governance framework.
    
    Rules from Playbook 3.3:
    - Range validation (count between x and y)
    - Cross-field validation
    - Statistical validation (std dev)
    - Trend validation (distinct values)
    - Boundary validation (max values)
    - Temporal validation (timestamps)
    """
    
    table_name: str = dspy.InputField(desc="Gold table name")
    table_schema: str = dspy.InputField(desc="Table schema")
    business_rules: str = dspy.InputField(desc="Business rules (max 5 per Playbook)")
    
    governance_rules: str = dspy.OutputField(
        desc="Governance rules with SQL expressions for each rule type"
    )
    acl_configuration: str = dspy.OutputField(
        desc="ACL configuration: REVOKE from pipeline DBs, GRANT to consumer groups"
    )


class DesignNamingConventions(dspy.Signature):
    """Apply naming conventions per Playbook 3.4 Step 12.
    
    Rules:
    - Partition columns: p_yyyymm, p_column_name
    - Table prefix: t_tablename or tablename_t
    - History suffix: t_tablename_hist
    - View names: match table without prefix
    """
    
    tables: str = dspy.InputField(desc="List of tables to name")
    
    naming_scheme: str = dspy.OutputField(
        desc="Complete naming scheme for tables, views, history tables, partitions"
    )


# =============================================================================
# Architecture Overview Signature
# =============================================================================


class GenerateDesignDocument(dspy.Signature):
    """Generate the complete design document for coder agent.
    
    The design document should include:
    1. Executive summary
    2. Bronze layer design with ingestion steps
    3. Silver layer design with transformation steps
    4. Gold layer design with aggregation steps
    5. Implementation order and dependencies
    6. Risk escalations if any
    """
    
    project_name: str = dspy.InputField(desc="Project name")
    bronze_design: str = dspy.InputField(desc="Bronze layer design details")
    silver_design: str = dspy.InputField(desc="Silver layer design details")
    gold_design: str = dspy.InputField(desc="Gold layer design details")
    governance_design: str = dspy.InputField(desc="Governance and security design")
    
    design_document: str = dspy.OutputField(
        desc="Complete design document in Markdown with numbered implementation steps for coder agent"
    )
    implementation_steps: str = dspy.OutputField(
        desc="Ordered list of implementation steps with dependencies"
    )
    escalation_flags: str = dspy.OutputField(
        desc="Any risks requiring escalation per Playbook guidelines"
    )

