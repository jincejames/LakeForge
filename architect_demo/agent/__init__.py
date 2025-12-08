"""
Lakehouse Architect Agent Package.

A DSPY-powered agent that generates design documents for Databricks lakehouse
implementations following the Bronze → Silver → Gold methodology.
"""

from .architect_agent import ArchitectAgent, quick_design
from .config import (
    AVAILABLE_MODELS,
    DEFAULT_MODEL,
    MLflowTracker,
    configure_lm,
    load_specs_from_folder,
)
from .modules import (
    BronzeDesigner,
    DocumentGenerator,
    GoldDesigner,
    LakehouseArchitect,
    SilverDesigner,
    SourceQualifier,
)
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

__all__ = [
    # Main agent
    "ArchitectAgent",
    "quick_design",
    # Config
    "AVAILABLE_MODELS",
    "DEFAULT_MODEL",
    "MLflowTracker",
    "configure_lm",
    "load_specs_from_folder",
    # Modules
    "LakehouseArchitect",
    "SourceQualifier",
    "BronzeDesigner",
    "SilverDesigner",
    "GoldDesigner",
    "DocumentGenerator",
    # Signatures (for fine-tuning)
    "QualifySource",
    "DesignBronzeIngestion",
    "DesignBronzeValidation",
    "DesignSilverTransformation",
    "DesignDataQuality",
    "DesignGoldTable",
    "DesignGovernance",
    "DesignNamingConventions",
    "GenerateDesignDocument",
]

