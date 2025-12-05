"""
Lakehouse Architect Agent - Main Entry Point.

This module provides the primary interface for generating
Lakehouse design documents using DSPy-powered prompts.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import dspy

from .config import (
    PLAYBOOK_RULES,
    clear_history,
    configure_for_design,
    configure_lm,
    get_history_summary,
    inspect_history,
)
from .modules import (
    ArchitectureDesigner,
    BronzeDesigner,
    GoldDesigner,
    LakehouseArchitect,
    SchemaAnalyzer,
    SilverDesigner,
    SourceQualifier,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes for Input
# =============================================================================


@dataclass
class SourceTable:
    """Represents a source table to be designed into the Lakehouse."""

    name: str
    schema: str  # DDL or column definitions
    has_update_timestamp: bool = False
    has_primary_key: bool = True
    primary_key: str = "id"
    sample_data: str = ""
    business_context: str = ""
    estimated_row_count: str = ""
    update_frequency: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary for module input."""
        return {
            "name": self.name,
            "schema": self.schema,
            "has_update_timestamp": self.has_update_timestamp,
            "has_primary_key": self.has_primary_key,
            "primary_key": self.primary_key,
            "sample_data": self.sample_data,
            "business_context": self.business_context,
            "estimated_row_count": self.estimated_row_count,
            "update_frequency": self.update_frequency,
        }


@dataclass
class ProjectSpec:
    """Specification for a Lakehouse project."""

    name: str
    cloud_provider: Literal["AWS", "Azure", "GCP"]
    source_tables: list[SourceTable]
    business_mappings: str = ""
    consumer_groups: str = ""
    business_rules: str = ""
    data_sensitivity: str = ""
    output_path: str = "DESIGN_DOCUMENT.md"


# =============================================================================
# Architect Agent
# =============================================================================


class ArchitectAgent:
    """
    Lakehouse Architect Agent for generating design documents.

    This agent follows the Playbook methodology to create comprehensive
    design documents for Bronze → Silver → Gold implementations.

    Example usage:
        agent = ArchitectAgent()

        # Define source tables
        customers = SourceTable(
            name="customers",
            schema="id INT, name STRING, email STRING, created_at TIMESTAMP",
            has_update_timestamp=True,
            business_context="Customer master data, updated when profile changes"
        )

        orders = SourceTable(
            name="orders",
            schema="id INT, customer_id INT, total DECIMAL, order_date DATE",
            has_update_timestamp=True,
            business_context="Order transactions, core fact table"
        )

        # Create project spec
        project = ProjectSpec(
            name="ecommerce_lakehouse",
            cloud_provider="AWS",
            source_tables=[customers, orders],
            business_mappings="Join customers to orders on customer_id",
            consumer_groups="analysts, data_scientists",
            business_rules="Total must be positive, order_date <= today"
        )

        # Generate design document
        result = agent.generate_design(project)
        agent.save_document(result, "DESIGN_DOCUMENT.md")
    """

    def __init__(
        self,
        model: str = "claude-opus",
        complexity: Literal["simple", "standard", "complex"] | None = None,
    ):
        """
        Initialize the Architect Agent.

        Args:
            model: Model to use (key from AVAILABLE_MODELS or full path)
            complexity: If provided, uses optimized settings for complexity level
        """
        if complexity:
            configure_for_design(complexity)
        else:
            configure_lm(model)

        # Initialize modules
        self.lakehouse_architect = LakehouseArchitect()
        self.source_qualifier = SourceQualifier()
        self.schema_analyzer = SchemaAnalyzer()
        self.bronze_designer = BronzeDesigner()
        self.silver_designer = SilverDesigner()
        self.gold_designer = GoldDesigner()
        self.architecture_designer = ArchitectureDesigner()

        logger.info("ArchitectAgent initialized")

    def generate_design(self, project: ProjectSpec) -> dspy.Prediction:
        """
        Generate complete Lakehouse design document.

        Args:
            project: Project specification with all source tables and requirements

        Returns:
            DSPy Prediction containing the design document and all components
        """
        logger.info("Generating design for project: %s", project.name)

        # Convert source tables to dict format
        source_tables = [t.to_dict() for t in project.source_tables]

        # Run the full architect pipeline
        result = self.lakehouse_architect(
            project_name=project.name,
            cloud_provider=project.cloud_provider,
            source_tables=source_tables,
            business_mappings=project.business_mappings,
            consumer_groups=project.consumer_groups,
            business_rules=project.business_rules,
            data_sensitivity=project.data_sensitivity,
        )

        # Check for escalation
        if result.escalation_required:
            logger.warning(
                "ESCALATION REQUIRED: %s",
                result.architecture.escalation_reason,
            )

        return result

    def qualify_source(self, table: SourceTable) -> dspy.Prediction:
        """
        Qualify a single source table.

        Useful for incremental design or validation of individual tables.

        Args:
            table: Source table to qualify

        Returns:
            Qualification result with classification and mutability
        """
        return self.source_qualifier(
            table_name=table.name,
            table_schema=table.schema,
            has_update_timestamp=table.has_update_timestamp,
            has_primary_key=table.has_primary_key,
            sample_data=table.sample_data,
            business_context=table.business_context,
        )

    def design_bronze(self, table: SourceTable) -> dspy.Prediction:
        """
        Design Bronze layer for a single table.

        Args:
            table: Source table specification

        Returns:
            Bronze design including DDL, storage, and ingestion strategy
        """
        return self.bronze_designer(
            table_name=table.name,
            table_schema=table.schema,
            has_update_timestamp=table.has_update_timestamp,
            has_primary_key=table.has_primary_key,
            sample_data=table.sample_data,
            business_context=table.business_context,
            estimated_row_count=table.estimated_row_count,
            update_frequency=table.update_frequency,
        )

    def design_silver(
        self,
        bronze_table: str,
        target_silver_table: str,
        primary_key_columns: str,
        **kwargs,
    ) -> dspy.Prediction:
        """
        Design Silver layer transformation.

        Args:
            bronze_table: Source Bronze table name
            target_silver_table: Target Silver table name
            primary_key_columns: Primary key column(s)
            **kwargs: Additional arguments (business_mappings, etc.)

        Returns:
            Silver design including transformation and table specification
        """
        return self.silver_designer(
            bronze_table=bronze_table,
            target_silver_table=target_silver_table,
            primary_key_columns=primary_key_columns,
            **kwargs,
        )

    def design_gold(
        self,
        silver_source_table: str,
        table_purpose: str,
        table_schema: str,
        **kwargs,
    ) -> dspy.Prediction:
        """
        Design Gold layer tables and views.

        Args:
            silver_source_table: Source Silver table name
            table_purpose: Business purpose description
            table_schema: Table schema definition
            **kwargs: Additional arguments (business_rules, etc.)

        Returns:
            Gold design including tables, governance, and views
        """
        return self.gold_designer(
            silver_source_table=silver_source_table,
            table_purpose=table_purpose,
            table_schema=table_schema,
            **kwargs,
        )

    def save_document(
        self,
        result: dspy.Prediction,
        output_path: str | Path = "DESIGN_DOCUMENT.md",
    ) -> Path:
        """
        Save the generated design document to a file.

        Args:
            result: Result from generate_design()
            output_path: Path to save the document

        Returns:
            Path where document was saved
        """
        output_path = Path(output_path)
        output_path.write_text(result.design_document)
        logger.info("Design document saved to: %s", output_path)
        return output_path

    @staticmethod
    def get_playbook_rules() -> dict:
        """Get the Playbook rules for reference."""
        return PLAYBOOK_RULES

    @staticmethod
    def inspect_history(n: int | None = None, log_level: int = logging.INFO) -> list[dict]:
        """
        Inspect and log DSPy LM call history.

        Shows the prompts sent and responses received for each step.

        Args:
            n: Number of recent calls to inspect (None = all)
            log_level: Logging level for output

        Returns:
            List of history entries as dicts
        """
        return inspect_history(n=n, log_level=log_level)

    @staticmethod
    def get_history_summary() -> dict:
        """
        Get a summary of the DSPy history.

        Returns:
            Dict with total_calls and per-call statistics
        """
        return get_history_summary()

    @staticmethod
    def clear_history():
        """Clear the DSPy LM history."""
        clear_history()


# =============================================================================
# Convenience Functions
# =============================================================================


def quick_design(
    project_name: str,
    cloud_provider: Literal["AWS", "Azure", "GCP"],
    source_tables: list[dict],
    output_path: str = "DESIGN_DOCUMENT.md",
    **kwargs,
) -> Path:
    """
    Quick function to generate a design document.

    Args:
        project_name: Name of the project
        cloud_provider: Cloud platform
        source_tables: List of source table dicts with keys:
            - name: table name
            - schema: DDL or column definitions
            - has_update_timestamp: bool (optional)
            - has_primary_key: bool (optional)
            - business_context: str (optional)
        output_path: Where to save the document
        **kwargs: Additional project spec arguments

    Returns:
        Path to the saved document

    Example:
        quick_design(
            project_name="sales_lakehouse",
            cloud_provider="AWS",
            source_tables=[
                {"name": "customers", "schema": "id INT, name STRING"},
                {"name": "orders", "schema": "id INT, customer_id INT, total DECIMAL"},
            ],
            business_mappings="Join on customer_id",
        )
    """
    # Convert dicts to SourceTable objects
    tables = [SourceTable(**t) for t in source_tables]

    # Create project spec
    project = ProjectSpec(
        name=project_name,
        cloud_provider=cloud_provider,
        source_tables=tables,
        output_path=output_path,
        **kwargs,
    )

    # Generate and save
    agent = ArchitectAgent(complexity="standard")
    result = agent.generate_design(project)
    return agent.save_document(result, output_path)

