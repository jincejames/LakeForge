"""
Lakehouse Architect Agent - DSPy-powered design document generation.

This package provides tools for generating comprehensive Lakehouse design
documents following the medallion architecture pattern (Bronze → Silver → Gold).

Example usage:
    from architect import ArchitectAgent, SourceTable, ProjectSpec

    # Initialize agent
    agent = ArchitectAgent()

    # Define source tables
    customers = SourceTable(
        name="customers",
        schema="id INT, name STRING, email STRING, updated_at TIMESTAMP",
        has_update_timestamp=True,
        business_context="Customer master data"
    )

    # Create project
    project = ProjectSpec(
        name="ecommerce",
        cloud_provider="AWS",
        source_tables=[customers],
    )

    # Generate design
    result = agent.generate_design(project)
    agent.save_document(result, "DESIGN_DOCUMENT.md")

Quick usage:
    from architect import quick_design

    quick_design(
        project_name="sales",
        cloud_provider="Azure",
        source_tables=[
            {"name": "orders", "schema": "id INT, total DECIMAL"}
        ]
    )
"""

from .architect_agent import (
    ArchitectAgent,
    ProjectSpec,
    SourceTable,
    quick_design,
)
from .config import (
    AVAILABLE_MODELS,
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

__all__ = [
    # Main Agent
    "ArchitectAgent",
    "ProjectSpec",
    "SourceTable",
    "quick_design",
    # Configuration
    "configure_lm",
    "configure_for_design",
    "AVAILABLE_MODELS",
    "PLAYBOOK_RULES",
    # History Inspection
    "inspect_history",
    "get_history_summary",
    "clear_history",
    # Modules
    "LakehouseArchitect",
    "SourceQualifier",
    "SchemaAnalyzer",
    "BronzeDesigner",
    "SilverDesigner",
    "GoldDesigner",
    "ArchitectureDesigner",
    # Signatures
    "ClassifyDataSource",
    "AssessDataMutability",
    "AnalyzeSchemaPattern",
    "DesignBronzeTable",
    "DesignTransformationMapping",
    "DesignSilverTable",
    "DesignGoldTable",
    "DesignGovernanceRules",
    "DesignGoldView",
    "DesignDatabaseArchitecture",
    "AssessRisks",
    "GenerateDesignDocument",
]

__version__ = "0.1.0"

