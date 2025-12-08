"""
DSPy Modules for Lakehouse Architect Agent.

These modules compose signatures into reusable workflows
that follow the Playbook methodology for Lakehouse design.
"""

import dspy

from .signatures import (
    AnalyzeSchemaPattern,
    AssessDataMutability,
    AssessRisks,
    ClassifyDataSource,
    DesignBronzeTable,
    DesignDatabaseArchitecture,
    DesignGoldTable,
    DesignGoldView,
    DesignGovernanceRules,
    DesignSilverTable,
    DesignTransformationMapping,
    GenerateDesignDocument,
)


# =============================================================================
# Phase 1: Source Qualification Module
# =============================================================================


class SourceQualifier(dspy.Module):
    """Qualify source tables according to Playbook methodology.

    Performs:
    1. Data classification (Historized/Fact/Dimensional/Lookup)
    2. Mutability assessment (Final/Correctable)
    3. Schema pattern analysis
    """

    def __init__(self):
        super().__init__()
        self.classify = dspy.ChainOfThought(ClassifyDataSource)
        self.assess_mutability = dspy.ChainOfThought(AssessDataMutability)

    def forward(
        self,
        table_name: str,
        table_schema: str,
        has_update_timestamp: bool = False,
        has_primary_key: bool = True,
        sample_data: str = "",
        business_context: str = "",
    ):
        # Step 1: Classify the data source
        classification = self.classify(
            table_name=table_name,
            table_schema=table_schema,
            sample_data=sample_data,
            business_context=business_context,
        )

        # Step 2: Assess mutability
        mutability = self.assess_mutability(
            table_name=table_name,
            table_schema=table_schema,
            has_update_timestamp=has_update_timestamp,
            has_primary_key=has_primary_key,
            business_context=business_context,
        )

        return dspy.Prediction(
            table_name=table_name,
            classification=classification.classification,
            classification_reasoning=classification.reasoning,
            design_considerations=classification.design_considerations,
            mutability_type=mutability.mutability_type,
            change_pattern=mutability.change_pattern,
            ingestion_strategy=mutability.ingestion_strategy,
            mutability_reasoning=mutability.reasoning,
        )


class SchemaAnalyzer(dspy.Module):
    """Analyze schema patterns across related tables."""

    def __init__(self):
        super().__init__()
        self.analyze = dspy.ChainOfThought(AnalyzeSchemaPattern)

    def forward(
        self,
        source_tables: str,
        relationships: str,
        primary_fact_table: str = "",
    ):
        analysis = self.analyze(
            source_tables=source_tables,
            relationships=relationships,
            primary_fact_table=primary_fact_table,
        )

        return dspy.Prediction(
            schema_pattern=analysis.schema_pattern,
            transformation_complexity=analysis.transformation_complexity,
            denormalization_plan=analysis.denormalization_plan,
            reasoning=analysis.reasoning,
        )


# =============================================================================
# Phase 1: Bronze Layer Design Module
# =============================================================================


class BronzeDesigner(dspy.Module):
    """Design Bronze layer tables with proper ingestion strategy.

    Follows Playbook rules:
    - Zero transformation from source
    - Add metadata columns
    - Route to proper storage (not DBFS)
    - Define validation checks
    """

    def __init__(self):
        super().__init__()
        self.qualify = SourceQualifier()
        self.design_table = dspy.ChainOfThought(DesignBronzeTable)

    def forward(
        self,
        table_name: str,
        table_schema: str,
        has_update_timestamp: bool = False,
        has_primary_key: bool = True,
        sample_data: str = "",
        business_context: str = "",
        estimated_row_count: str = "",
        update_frequency: str = "",
    ):
        # First qualify the source
        qualification = self.qualify(
            table_name=table_name,
            table_schema=table_schema,
            has_update_timestamp=has_update_timestamp,
            has_primary_key=has_primary_key,
            sample_data=sample_data,
            business_context=business_context,
        )

        # Then design the Bronze table
        design = self.design_table(
            table_name=table_name,
            table_schema=table_schema,
            data_classification=qualification.classification,
            mutability_type=qualification.mutability_type,
            estimated_row_count=estimated_row_count,
            update_frequency=update_frequency,
        )

        return dspy.Prediction(
            qualification=qualification,
            bronze_table_ddl=design.bronze_table_ddl,
            storage_location=design.storage_location,
            ingestion_strategy=design.ingestion_strategy,
            validation_checks=design.validation_checks,
        )


# =============================================================================
# Phase 2: Silver Layer Design Module
# =============================================================================


class SilverDesigner(dspy.Module):
    """Design Silver layer with transformations and CDC.

    Follows Playbook rules:
    - Flatten low-cardinality dimensions
    - Keep high-cardinality dimensions separate
    - Apply all business mappings
    - Implement CDC with _valid_to timestamps
    """

    def __init__(self):
        super().__init__()
        self.design_mapping = dspy.ChainOfThought(DesignTransformationMapping)
        self.design_table = dspy.ChainOfThought(DesignSilverTable)

    def forward(
        self,
        bronze_table: str,
        target_silver_table: str,
        primary_key_columns: str,
        related_bronze_tables: str = "",
        business_mappings: str = "",
        cardinality_info: str = "",
        common_query_patterns: str = "",
        join_keys: str = "",
        estimated_daily_records: str = "",
    ):
        # Step 1: Design the transformation mapping
        mapping = self.design_mapping(
            bronze_table=bronze_table,
            target_silver_table=target_silver_table,
            related_bronze_tables=related_bronze_tables,
            business_mappings=business_mappings,
            cardinality_info=cardinality_info,
        )

        # Step 2: Design the Silver table
        table_design = self.design_table(
            table_name=target_silver_table,
            transformation_mapping=mapping.transformation_sql,
            primary_key_columns=primary_key_columns,
            common_query_patterns=common_query_patterns,
            join_keys=join_keys,
            estimated_daily_records=estimated_daily_records,
        )

        return dspy.Prediction(
            transformation_sql=mapping.transformation_sql,
            flattened_dimensions=mapping.flattened_dimensions,
            kept_separate_dimensions=mapping.kept_separate_dimensions,
            cdc_columns=mapping.cdc_columns,
            data_type_conversions=mapping.data_type_conversions,
            silver_table_ddl=table_design.silver_table_ddl,
            partition_strategy=table_design.partition_strategy,
            zorder_columns=table_design.zorder_columns,
            validation_rules=table_design.validation_rules,
            optimization_schedule=table_design.optimization_schedule,
        )


# =============================================================================
# Phase 3: Gold Layer Design Module
# =============================================================================


class GoldDesigner(dspy.Module):
    """Design Gold layer for enterprise consumption.

    Follows Playbook rules:
    - Current + History table pattern
    - Governance rules (≤5 per table)
    - View abstraction layer
    - Proper naming conventions
    """

    def __init__(self):
        super().__init__()
        self.design_table = dspy.ChainOfThought(DesignGoldTable)
        self.design_governance = dspy.ChainOfThought(DesignGovernanceRules)
        self.design_view = dspy.ChainOfThought(DesignGoldView)

    def forward(
        self,
        silver_source_table: str,
        table_purpose: str,
        table_schema: str,
        requires_history: bool = True,
        history_retention_period: str = "",
        consumer_query_patterns: str = "",
        business_rules: str = "",
        critical_columns: str = "",
    ):
        # Step 1: Design Gold table(s)
        table_design = self.design_table(
            silver_source_table=silver_source_table,
            table_purpose=table_purpose,
            requires_history=requires_history,
            history_retention_period=history_retention_period,
            consumer_query_patterns=consumer_query_patterns,
        )

        # Step 2: Design governance rules
        governance = self.design_governance(
            table_name=silver_source_table.replace("silver.", ""),
            table_schema=table_schema,
            business_rules=business_rules,
            critical_columns=critical_columns,
        )

        # Step 3: Design consumer view
        # Extract table name for view design
        base_name = silver_source_table.replace("silver.", "").replace("dw_silver.", "")
        gold_table_name = f"{base_name}_t"

        view_design = self.design_view(
            gold_table_name=gold_table_name,
            table_schema=table_schema,
            partition_columns=table_design.partition_strategy,
            consumer_facing_name=base_name,
        )

        return dspy.Prediction(
            current_table_ddl=table_design.current_table_ddl,
            history_table_ddl=table_design.history_table_ddl,
            table_type=table_design.table_type,
            partition_strategy=table_design.partition_strategy,
            zorder_strategy=table_design.zorder_strategy,
            governance_rules=governance.governance_rules,
            failure_handling=governance.failure_handling,
            rule_execution_timing=governance.rule_execution_timing,
            view_ddl=view_design.view_ddl,
            column_mappings=view_design.column_mappings,
            schema_evolution_notes=view_design.schema_evolution_notes,
        )


# =============================================================================
# Architecture & Risk Module
# =============================================================================


class ArchitectureDesigner(dspy.Module):
    """Design overall database architecture and assess risks.

    Follows Playbook rules:
    - Separate Gold storage from Bronze/Silver
    - Proper ACL configuration
    - Risk escalation triggers
    """

    def __init__(self):
        super().__init__()
        self.design_architecture = dspy.ChainOfThought(DesignDatabaseArchitecture)
        self.assess_risks = dspy.ChainOfThought(AssessRisks)

    def forward(
        self,
        project_name: str,
        cloud_provider: str,
        bronze_tables: str,
        silver_tables: str,
        gold_tables: str,
        source_inventory: str,
        mapping_clarity: str,
        consumer_groups: str = "",
        large_non_type2_tables: str = "",
        data_sensitivity: str = "",
    ):
        # Step 1: Design database architecture
        architecture = self.design_architecture(
            project_name=project_name,
            cloud_provider=cloud_provider,
            bronze_tables=bronze_tables,
            silver_tables=silver_tables,
            gold_tables=gold_tables,
            consumer_groups=consumer_groups,
        )

        # Step 2: Assess risks
        risks = self.assess_risks(
            source_inventory=source_inventory,
            mapping_clarity=mapping_clarity,
            large_non_type2_tables=large_non_type2_tables,
            data_sensitivity=data_sensitivity,
        )

        return dspy.Prediction(
            database_definitions=architecture.database_definitions,
            storage_architecture=architecture.storage_architecture,
            acl_configuration=architecture.acl_configuration,
            naming_conventions=architecture.naming_conventions,
            identified_risks=risks.identified_risks,
            mitigation_strategies=risks.mitigation_strategies,
            escalation_required=risks.escalation_required,
            escalation_reason=risks.escalation_reason,
        )


# =============================================================================
# Full Design Document Generator
# =============================================================================


class LakehouseArchitect(dspy.Module):
    """Complete Lakehouse Architect that generates full design documents.

    Orchestrates all phases:
    1. Source Qualification & Bronze Design
    2. Silver Layer Design
    3. Gold Layer Design
    4. Architecture & Risk Assessment
    5. Design Document Generation
    """

    def __init__(self):
        super().__init__()
        self.bronze_designer = BronzeDesigner()
        self.silver_designer = SilverDesigner()
        self.gold_designer = GoldDesigner()
        self.architecture_designer = ArchitectureDesigner()
        self.generate_document = dspy.ChainOfThought(GenerateDesignDocument)

    def forward(
        self,
        project_name: str,
        cloud_provider: str,
        source_tables: list[dict],
        business_mappings: str = "",
        consumer_groups: str = "",
        business_rules: str = "",
        data_sensitivity: str = "",
    ):
        """
        Generate complete Lakehouse design.

        Args:
            project_name: Name of the project/domain
            cloud_provider: AWS, Azure, or GCP
            source_tables: List of dicts with keys:
                - name: table name
                - schema: DDL or column definitions
                - has_update_timestamp: bool
                - has_primary_key: bool
                - sample_data: optional
                - business_context: optional
                - estimated_row_count: optional
                - update_frequency: optional
            business_mappings: Transformation rules from customer
            consumer_groups: User groups for ACL design
            business_rules: Governance rules from customer
            data_sensitivity: Data classification levels
        """
        # Phase 1: Bronze design for each table
        bronze_designs = []
        source_inventory = []

        for table in source_tables:
            bronze = self.bronze_designer(
                table_name=table.get("name"),
                table_schema=table.get("schema"),
                has_update_timestamp=table.get("has_update_timestamp", False),
                has_primary_key=table.get("has_primary_key", True),
                sample_data=table.get("sample_data", ""),
                business_context=table.get("business_context", ""),
                estimated_row_count=table.get("estimated_row_count", ""),
                update_frequency=table.get("update_frequency", ""),
            )
            bronze_designs.append(bronze)
            source_inventory.append(
                f"{table.get('name')}: {bronze.qualification.classification}, "
                f"{bronze.qualification.mutability_type}"
            )

        # Phase 2: Silver design (simplified - one per bronze)
        silver_designs = []
        for i, table in enumerate(source_tables):
            silver = self.silver_designer(
                bronze_table=f"dw_bronze.{table.get('name')}",
                target_silver_table=f"dw_silver.{table.get('name')}",
                primary_key_columns=table.get("primary_key", "id"),
                business_mappings=business_mappings,
            )
            silver_designs.append(silver)

        # Phase 3: Gold design (simplified - one per silver)
        gold_designs = []
        for i, table in enumerate(source_tables):
            gold = self.gold_designer(
                silver_source_table=f"dw_silver.{table.get('name')}",
                table_purpose=table.get("business_context", ""),
                table_schema=table.get("schema"),
                requires_history=True,
                business_rules=business_rules,
            )
            gold_designs.append(gold)

        # Phase 4: Architecture and risks
        bronze_table_names = ", ".join([t.get("name") for t in source_tables])
        silver_table_names = bronze_table_names  # Same names in silver
        gold_table_names = bronze_table_names  # Same base names in gold

        architecture = self.architecture_designer(
            project_name=project_name,
            cloud_provider=cloud_provider,
            bronze_tables=bronze_table_names,
            silver_tables=silver_table_names,
            gold_tables=gold_table_names,
            source_inventory="\n".join(source_inventory),
            mapping_clarity="Provided" if business_mappings else "Not provided",
            consumer_groups=consumer_groups,
            data_sensitivity=data_sensitivity,
        )

        # Compile design components
        source_analysis = self._compile_source_analysis(bronze_designs)
        bronze_design = self._compile_bronze_design(bronze_designs)
        silver_design = self._compile_silver_design(silver_designs)
        gold_design = self._compile_gold_design(gold_designs)
        security_design = (
            f"Database Definitions:\n{architecture.database_definitions}\n\n"
            f"ACL Configuration:\n{architecture.acl_configuration}"
        )
        risk_assessment = (
            f"Identified Risks:\n{architecture.identified_risks}\n\n"
            f"Mitigations:\n{architecture.mitigation_strategies}"
        )

        # Generate final document
        document = self.generate_document(
            source_analysis=source_analysis,
            bronze_design=bronze_design,
            silver_design=silver_design,
            gold_design=gold_design,
            security_design=security_design,
            risk_assessment=risk_assessment,
            project_context=f"{project_name} on {cloud_provider}",
        )

        return dspy.Prediction(
            design_document=document.design_document,
            bronze_designs=bronze_designs,
            silver_designs=silver_designs,
            gold_designs=gold_designs,
            architecture=architecture,
            escalation_required=architecture.escalation_required,
        )

    def _compile_source_analysis(self, bronze_designs) -> str:
        """Compile source analysis section from bronze designs."""
        lines = ["## Source Analysis\n"]
        for design in bronze_designs:
            q = design.qualification
            lines.append(f"### {q.table_name}")
            lines.append(f"- Classification: {q.classification}")
            lines.append(f"- Mutability: {q.mutability_type}")
            lines.append(f"- Change Pattern: {q.change_pattern}")
            lines.append(f"- Ingestion Strategy: {q.ingestion_strategy}")
            lines.append(f"- Considerations: {q.design_considerations}\n")
        return "\n".join(lines)

    def _compile_bronze_design(self, bronze_designs) -> str:
        """Compile bronze design section."""
        lines = ["## Bronze Layer Design\n"]
        for design in bronze_designs:
            lines.append(f"### {design.qualification.table_name}")
            lines.append(f"```sql\n{design.bronze_table_ddl}\n```")
            lines.append(f"- Storage: {design.storage_location}")
            lines.append(f"- Ingestion: {design.ingestion_strategy}")
            lines.append(f"- Validations: {design.validation_checks}\n")
        return "\n".join(lines)

    def _compile_silver_design(self, silver_designs) -> str:
        """Compile silver design section."""
        lines = ["## Silver Layer Design\n"]
        for design in silver_designs:
            lines.append("### Transformation")
            lines.append(f"```sql\n{design.transformation_sql}\n```")
            lines.append(f"- Flattened: {design.flattened_dimensions}")
            lines.append(f"- Kept Separate: {design.kept_separate_dimensions}")
            lines.append(f"- CDC Columns: {design.cdc_columns}")
            lines.append(f"\n```sql\n{design.silver_table_ddl}\n```")
            lines.append(f"- Partition: {design.partition_strategy}")
            lines.append(f"- Z-Order: {design.zorder_columns}")
            lines.append(f"- Optimization: {design.optimization_schedule}\n")
        return "\n".join(lines)

    def _compile_gold_design(self, gold_designs) -> str:
        """Compile gold design section."""
        lines = ["## Gold Layer Design\n"]
        for design in gold_designs:
            lines.append("### Current Table")
            lines.append(f"```sql\n{design.current_table_ddl}\n```")
            if design.history_table_ddl:
                lines.append("### History Table")
                lines.append(f"```sql\n{design.history_table_ddl}\n```")
            lines.append(f"- Type: {design.table_type}")
            lines.append(f"- Governance Rules: {design.governance_rules}")
            lines.append(f"- Failure Handling: {design.failure_handling}")
            lines.append("\n### Consumer View")
            lines.append(f"```sql\n{design.view_ddl}\n```\n")
        return "\n".join(lines)

