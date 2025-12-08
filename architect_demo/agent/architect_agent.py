"""
Lakehouse Architect Agent - Main Entry Point.

This agent creates design documents for Databricks lakehouse implementations
using DSPY for prompt optimization and MLflow for tracking.
"""

import logging
from pathlib import Path
from typing import Any

import dspy

from .config import (
    DEFAULT_MODEL,
    MLflowTracker,
    clear_history,
    configure_lm,
    get_history_summary,
    inspect_history,
    load_specs_from_folder,
    log_dspy_history,
)
from .modules import LakehouseArchitect

logger = logging.getLogger(__name__)


class ArchitectAgent:
    """
    Lakehouse Architect Agent for generating design documents.
    
    This agent:
    1. Takes a specs folder with schema, transformation, and pipeline specs
    2. Uses DSPY signatures codifying Playbook rules
    3. Generates a design document with implementation steps for coder agent
    4. Tracks progress in MLflow
    
    Example usage:
        agent = ArchitectAgent(model="claude-sonnet")
        
        result = agent.generate_design(
            project_name="servicenow_lakehouse",
            specs_folder="architect_demo/specs",
        )
        
        agent.save_document(result, "DESIGN_DOCUMENT.md")
    """
    
    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        experiment_name: str = "lakehouse-architect",
        mlflow_tracking_uri: str | None = None,
    ):
        """
        Initialize the Architect Agent.
        
        Args:
            model: Model key or full path (see config.AVAILABLE_MODELS)
            experiment_name: MLflow experiment name
            mlflow_tracking_uri: MLflow tracking URI
        """
        self.model = model
        self.lm = configure_lm(model)
        
        # Initialize MLflow tracker
        self.tracker = MLflowTracker(
            experiment_name=experiment_name,
            tracking_uri=mlflow_tracking_uri,
        )
        
        # Initialize the main architect module
        self.architect = LakehouseArchitect()
        
        logger.info("ArchitectAgent initialized with model: %s", model)
    
    def generate_design(
        self,
        project_name: str,
        specs_folder: str,
        run_name: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> dspy.Prediction:
        """
        Generate a complete lakehouse design document.
        
        Args:
            project_name: Name of the project
            specs_folder: Path to folder containing specs
            run_name: MLflow run name (defaults to project_name)
            tags: Additional MLflow tags
            
        Returns:
            DSPy Prediction containing design_document, implementation_steps, etc.
        """
        run_name = run_name or f"design-{project_name}"
        
        # Start MLflow tracking
        self.tracker.start_run(
            run_name=run_name,
            tags={
                "project_name": project_name,
                "model": self.model,
                **(tags or {}),
            },
        )
        
        try:
            # Log parameters
            self.tracker.log_params({
                "project_name": project_name,
                "specs_folder": specs_folder,
                "model": self.model,
            })
            
            # Load specs
            logger.info("Loading specs from: %s", specs_folder)
            specs = load_specs_from_folder(specs_folder)
            
            # Log specs summary
            self.tracker.log_metrics({
                "num_bronze_tables": len(specs.get("bronze_tables", [])),
                "num_silver_tables": len(specs.get("silver_tables", [])),
                "num_gold_tables": len(specs.get("gold_tables", [])),
                "num_bronze_silver_transforms": len(specs.get("transformations_bronze_to_silver", [])),
                "num_silver_gold_transforms": len(specs.get("transformations_silver_to_gold", [])),
            })
            
            # Run the architect
            logger.info("Generating design for: %s", project_name)
            result = self.architect(
                project_name=project_name,
                specs=specs,
            )
            
            # Log DSPY history
            history_stats = log_dspy_history(self.tracker)
            logger.info("LM calls: %d, Total tokens: %d", 
                       history_stats.get("total_lm_calls", 0),
                       history_stats.get("total_tokens", 0))
            
            # Log the design document
            self.tracker.log_text(result.design_document, "design_document.md")
            self.tracker.log_text(result.implementation_steps, "implementation_steps.txt")
            
            # Log escalation flags if any
            if result.escalation_flags and result.escalation_flags.strip():
                logger.warning("ESCALATION FLAGS: %s", result.escalation_flags)
                self.tracker.log_text(result.escalation_flags, "escalation_flags.txt")
            
            self.tracker.end_run(status="FINISHED")
            return result
            
        except Exception as e:
            logger.exception("Design generation failed")
            self.tracker.end_run(status="FAILED")
            raise
    
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
    
    def save_implementation_steps(
        self,
        result: dspy.Prediction,
        output_path: str | Path = "IMPLEMENTATION_STEPS.md",
    ) -> Path:
        """
        Save implementation steps to a separate file.
        
        Args:
            result: Result from generate_design()
            output_path: Path to save the steps
            
        Returns:
            Path where steps were saved
        """
        output_path = Path(output_path)
        output_path.write_text(result.implementation_steps)
        logger.info("Implementation steps saved to: %s", output_path)
        return output_path
    
    @staticmethod
    def inspect_history(n: int | None = None, log_level: int = logging.INFO) -> list[dict]:
        """Inspect DSPY LM call history."""
        return inspect_history(n=n, log_level=log_level)
    
    @staticmethod
    def get_history_summary() -> dict:
        """Get summary of DSPY history."""
        return get_history_summary()
    
    @staticmethod
    def clear_history() -> None:
        """Clear DSPY LM history."""
        clear_history()


# =============================================================================
# Convenience Function
# =============================================================================


def quick_design(
    project_name: str,
    specs_folder: str,
    output_path: str = "DESIGN_DOCUMENT.md",
    model: str = DEFAULT_MODEL,
) -> Path:
    """
    Quick function to generate and save a design document.
    
    Args:
        project_name: Name of the project
        specs_folder: Path to specs folder
        output_path: Where to save the document
        model: Model to use
        
    Returns:
        Path to saved document
        
    Example:
        quick_design(
            project_name="servicenow_lakehouse",
            specs_folder="architect_demo/specs",
            output_path="output/DESIGN_DOCUMENT.md",
        )
    """
    agent = ArchitectAgent(model=model)
    result = agent.generate_design(
        project_name=project_name,
        specs_folder=specs_folder,
    )
    return agent.save_document(result, output_path)

