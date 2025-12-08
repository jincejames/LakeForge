"""
DSPY Modules for Lakehouse Design.

These modules orchestrate the signatures to produce design artifacts.
"""

import logging
from pathlib import Path

import dspy

from .signatures import (
    DesignBronzeIngestion,
    DesignBronzeValidation,
    DesignDataQuality,
    DesignGovernance,
    DesignGoldTable,
    DesignNamingConventions,
    DesignSilverTransformation,
    GenerateDesignDocument,
    QualifySource,
)

logger = logging.getLogger(__name__)


class SourceQualifier(dspy.Module):
    """Qualifies source tables per Playbook 1.2 rules."""

    def __init__(self):
        super().__init__()
        self.qualify = dspy.ChainOfThought(QualifySource)

    def forward(
        self,
        table_name: str,
        table_schema: str,
        sample_data: str = "",
        business_context: str = "",
    ) -> dspy.Prediction:
        return self.qualify(
            table_name=table_name,
            table_schema=table_schema,
            sample_data=sample_data,
            business_context=business_context,
        )


class BronzeDesigner(dspy.Module):
    """Designs Bronze layer ingestion and validation."""

    def __init__(self):
        super().__init__()
        self.design_ingestion = dspy.ChainOfThought(DesignBronzeIngestion)
        self.design_validation = dspy.ChainOfThought(DesignBronzeValidation)

    def forward(
        self,
        table_name: str,
        source_type: str,
        data_class: str,
        data_finality: str,
        has_update_timestamp: bool,
        primary_key: str,
        estimated_row_count: str = "",
        update_frequency: str = "",
    ) -> dspy.Prediction:
        ingestion = self.design_ingestion(
            table_name=table_name,
            source_type=source_type,
            data_class=data_class,
            data_finality=data_finality,
            has_update_timestamp=has_update_timestamp,
            estimated_row_count=estimated_row_count,
            update_frequency=update_frequency,
        )

        validation = self.design_validation(
            table_name=table_name,
            primary_key=primary_key,
        )

        return dspy.Prediction(
            ingestion=ingestion,
            validation=validation,
        )


class SilverDesigner(dspy.Module):
    """Designs Silver layer transformations and quality checks."""

    def __init__(self):
        super().__init__()
        self.design_transformation = dspy.ChainOfThought(DesignSilverTransformation)
        self.design_quality = dspy.ChainOfThought(DesignDataQuality)

    def forward(
        self,
        bronze_table: str,
        bronze_schema: str,
        target_silver_table: str,
        transformation_rules: str,
        business_rules: str = "",
    ) -> dspy.Prediction:
        transformation = self.design_transformation(
            bronze_table=bronze_table,
            bronze_schema=bronze_schema,
            target_silver_table=target_silver_table,
            transformation_rules=transformation_rules,
        )

        quality = self.design_quality(
            table_name=target_silver_table,
            table_schema=transformation.silver_ddl,
            business_rules=business_rules,
        )

        return dspy.Prediction(
            transformation=transformation,
            quality=quality,
        )


class GoldDesigner(dspy.Module):
    """Designs Gold layer tables, views, and governance."""

    def __init__(self):
        super().__init__()
        self.design_table = dspy.ChainOfThought(DesignGoldTable)
        self.design_governance = dspy.ChainOfThought(DesignGovernance)
        self.design_naming = dspy.ChainOfThought(DesignNamingConventions)

    def forward(
        self,
        silver_source: str,
        table_purpose: str,
        aggregation_spec: str,
        business_rules: str = "",
    ) -> dspy.Prediction:
        table = self.design_table(
            silver_source=silver_source,
            table_purpose=table_purpose,
            aggregation_spec=aggregation_spec,
        )

        governance = self.design_governance(
            table_name=table_purpose.split()[0] if table_purpose else "gold_table",
            table_schema=table.gold_table_ddl,
            business_rules=business_rules,
        )

        return dspy.Prediction(
            table=table,
            governance=governance,
        )


class DocumentGenerator(dspy.Module):
    """Generates the final design document."""

    def __init__(self):
        super().__init__()
        self.generate = dspy.ChainOfThought(GenerateDesignDocument)

    def forward(
        self,
        project_name: str,
        bronze_design: str,
        silver_design: str,
        gold_design: str,
        governance_design: str,
    ) -> dspy.Prediction:
        return self.generate(
            project_name=project_name,
            bronze_design=bronze_design,
            silver_design=silver_design,
            gold_design=gold_design,
            governance_design=governance_design,
        )


class LakehouseArchitect(dspy.Module):
    """
    Main orchestrator module that runs the full design pipeline.
    
    Takes specs folder as input and produces a complete design document.
    """

    def __init__(self):
        super().__init__()
        self.source_qualifier = SourceQualifier()
        self.bronze_designer = BronzeDesigner()
        self.silver_designer = SilverDesigner()
        self.gold_designer = GoldDesigner()
        self.doc_generator = DocumentGenerator()

    def forward(
        self,
        project_name: str,
        specs: dict,
    ) -> dspy.Prediction:
        """
        Run the full architecture design pipeline.
        
        Args:
            project_name: Name of the project
            specs: Dictionary containing:
                - bronze_tables: List of bronze table specs
                - silver_tables: List of silver table specs  
                - gold_tables: List of gold table specs
                - transformations_bronze_to_silver: Transformation specs
                - transformations_silver_to_gold: Transformation specs
                - pipeline_config: Pipeline configuration
        
        Returns:
            Complete design prediction with all artifacts
        """
        logger.info("Starting architecture design for: %s", project_name)
        
        # Extract specs
        bronze_specs = specs.get("bronze_tables", [])
        silver_specs = specs.get("silver_tables", [])
        gold_specs = specs.get("gold_tables", [])
        bronze_to_silver = specs.get("transformations_bronze_to_silver", [])
        silver_to_gold = specs.get("transformations_silver_to_gold", [])
        pipeline_config = specs.get("pipeline_config", {})
        
        # Phase 1: Qualify and design Bronze
        logger.info("Phase 1: Designing Bronze layer")
        bronze_designs = []
        for table in bronze_specs:
            qualification = self.source_qualifier(
                table_name=table.get("table_name", ""),
                table_schema=self._format_schema(table),
                business_context=table.get("notes", ""),
            )
            
            bronze = self.bronze_designer(
                table_name=table.get("table_name", ""),
                source_type=pipeline_config.get("source_type", "API"),
                data_class=qualification.data_class,
                data_finality=qualification.data_finality,
                has_update_timestamp=True,  # From LakeFlow
                primary_key=self._find_primary_key(table),
            )
            
            bronze_designs.append({
                "table": table.get("table_name"),
                "qualification": qualification,
                "design": bronze,
            })
        
        # Phase 2: Design Silver transformations
        logger.info("Phase 2: Designing Silver layer")
        silver_designs = []
        for table in silver_specs:
            table_name = table.get("table_name", "")
            bronze_table = table_name.replace("silver.", "bronze.")
            
            # Find transformation rules for this table
            rules = [t for t in bronze_to_silver 
                     if t.get("target_table") == table_name]
            
            silver = self.silver_designer(
                bronze_table=bronze_table,
                bronze_schema=self._get_bronze_schema(bronze_specs, bronze_table),
                target_silver_table=table_name,
                transformation_rules=self._format_transformations(rules),
                business_rules=specs.get("data_quality_rules", ""),
            )
            
            silver_designs.append({
                "table": table_name,
                "design": silver,
            })
        
        # Phase 3: Design Gold layer
        logger.info("Phase 3: Designing Gold layer")
        gold_designs = []
        for table in gold_specs:
            table_name = table.get("table_name", "")
            
            # Find transformation rules for this table
            rules = [t for t in silver_to_gold 
                     if t.get("target_table") == table_name]
            
            gold = self.gold_designer(
                silver_source="silver.incident_flattened",  # Primary silver source
                table_purpose=table.get("description", table_name),
                aggregation_spec=self._format_transformations(rules),
                business_rules=specs.get("business_rules", ""),
            )
            
            gold_designs.append({
                "table": table_name,
                "design": gold,
            })
        
        # Generate final document
        logger.info("Generating design document")
        document = self.doc_generator(
            project_name=project_name,
            bronze_design=self._format_bronze_summary(bronze_designs),
            silver_design=self._format_silver_summary(silver_designs),
            gold_design=self._format_gold_summary(gold_designs),
            governance_design=self._extract_governance(gold_designs),
        )
        
        return dspy.Prediction(
            design_document=document.design_document,
            implementation_steps=document.implementation_steps,
            escalation_flags=document.escalation_flags,
            bronze_designs=bronze_designs,
            silver_designs=silver_designs,
            gold_designs=gold_designs,
        )

    def _format_schema(self, table_spec: dict) -> str:
        """Format table spec into schema string."""
        columns = []
        for key, value in table_spec.items():
            if key not in ["table_name", "notes", "additional_info"]:
                columns.append(f"{key}: {value}")
        return ", ".join(columns)

    def _find_primary_key(self, table_spec: dict) -> str:
        """Find primary key from table spec."""
        for col_name, details in table_spec.items():
            if isinstance(details, str) and "primary key" in details.lower():
                return col_name
        return "sys_id"  # Default for ServiceNow

    def _get_bronze_schema(self, bronze_specs: list, table_name: str) -> str:
        """Get schema for a bronze table."""
        for spec in bronze_specs:
            if spec.get("table_name") == table_name:
                return self._format_schema(spec)
        return ""

    def _format_transformations(self, rules: list) -> str:
        """Format transformation rules as string."""
        if not rules:
            return "No specific transformation rules provided"
        
        formatted = []
        for rule in rules:
            formatted.append(
                f"- {rule.get('transformation_name')}: {rule.get('description')} "
                f"[SQL: {rule.get('expression_sql')}]"
            )
        return "\n".join(formatted)

    def _format_bronze_summary(self, designs: list) -> str:
        """Format bronze designs for document generation."""
        summaries = []
        for d in designs:
            summaries.append(f"""
Table: {d['table']}
Classification: {d['qualification'].data_class}
Finality: {d['qualification'].data_finality}
Ingestion Strategy: {d['design'].ingestion.ingestion_strategy}
Storage: {d['design'].ingestion.storage_location}
""")
        return "\n".join(summaries)

    def _format_silver_summary(self, designs: list) -> str:
        """Format silver designs for document generation."""
        summaries = []
        for d in designs:
            summaries.append(f"""
Table: {d['table']}
DDL: {d['design'].transformation.silver_ddl}
CDC: {d['design'].transformation.cdc_implementation}
Quality Checks: {d['design'].quality.quality_checks}
""")
        return "\n".join(summaries)

    def _format_gold_summary(self, designs: list) -> str:
        """Format gold designs for document generation."""
        summaries = []
        for d in designs:
            summaries.append(f"""
Table: {d['table']}
DDL: {d['design'].table.gold_table_ddl}
View: {d['design'].table.gold_view_ddl}
""")
        return "\n".join(summaries)

    def _extract_governance(self, gold_designs: list) -> str:
        """Extract governance rules from gold designs."""
        governance = []
        for d in gold_designs:
            governance.append(d["design"].governance.governance_rules)
        return "\n\n".join(governance)

