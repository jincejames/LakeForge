"""
Configuration and MLflow integration for Architect Agent.
"""

import logging
import os
from typing import Any

import databricks_dspy
import dspy
import mlflow

logger = logging.getLogger(__name__)

# =============================================================================
# Model Configuration
# =============================================================================

AVAILABLE_MODELS = {
    "claude-sonnet": "databricks/databricks-claude-sonnet-4",
    "claude-opus": "databricks/databricks-claude-opus-4-5", 
}

DEFAULT_MODEL = "claude-sonnet"


def configure_lm(model_key: str = DEFAULT_MODEL) -> dspy.LM:
    """
    Configure the DSPY language model.
    
    Uses databricks_dspy for Databricks-hosted models.
    Authentication handled via:
    - databricks auth login (OAuth U2M)
    - Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.)
    """
    model_path = AVAILABLE_MODELS.get(model_key, model_key)
    
    lm = databricks_dspy.DatabricksLM(model=model_path)
    dspy.configure(lm=lm)
    
    logger.info("Configured DSPY with model: %s", model_path)
    return lm


# =============================================================================
# MLflow Integration
# =============================================================================


class MLflowTracker:
    """Tracks architect agent runs in MLflow."""
    
    def __init__(
        self,
        experiment_name: str = "lakehouse-architect",
        tracking_uri: str | None = None,
    ):
        """
        Initialize MLflow tracking.
        
        Args:
            experiment_name: MLflow experiment name
            tracking_uri: MLflow tracking URI (uses default if not provided)
        """
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        
        mlflow.set_experiment(experiment_name)
        self.experiment_name = experiment_name
        self.run = None
        
    def start_run(self, run_name: str, tags: dict[str, str] | None = None) -> None:
        """Start an MLflow run."""
        self.run = mlflow.start_run(run_name=run_name, tags=tags)
        logger.info("Started MLflow run: %s", run_name)
        
    def log_params(self, params: dict[str, Any]) -> None:
        """Log parameters to MLflow."""
        if self.run:
            mlflow.log_params(params)
            
    def log_metrics(self, metrics: dict[str, float], step: int | None = None) -> None:
        """Log metrics to MLflow."""
        if self.run:
            mlflow.log_metrics(metrics, step=step)
            
    def log_artifact(self, local_path: str, artifact_path: str | None = None) -> None:
        """Log an artifact to MLflow."""
        if self.run:
            mlflow.log_artifact(local_path, artifact_path)
            
    def log_text(self, text: str, artifact_file: str) -> None:
        """Log text as an artifact."""
        if self.run:
            mlflow.log_text(text, artifact_file)
            
    def log_dict(self, dictionary: dict, artifact_file: str) -> None:
        """Log a dictionary as JSON artifact."""
        if self.run:
            mlflow.log_dict(dictionary, artifact_file)
            
    def end_run(self, status: str = "FINISHED") -> None:
        """End the MLflow run."""
        if self.run:
            mlflow.end_run(status=status)
            self.run = None
            logger.info("Ended MLflow run with status: %s", status)
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "FAILED" if exc_type else "FINISHED"
        self.end_run(status=status)
        return False


def log_dspy_history(tracker: MLflowTracker) -> dict:
    """
    Log DSPY LM call history to MLflow.
    
    Returns summary statistics.
    """
    lm = dspy.settings.lm
    if not lm or not hasattr(lm, "history"):
        return {}
    
    history = lm.history
    
    # Log summary metrics
    total_calls = len(history)
    total_input_tokens = sum(h.get("usage", {}).get("prompt_tokens", 0) for h in history)
    total_output_tokens = sum(h.get("usage", {}).get("completion_tokens", 0) for h in history)
    
    metrics = {
        "total_lm_calls": total_calls,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_tokens": total_input_tokens + total_output_tokens,
    }
    
    tracker.log_metrics(metrics)
    
    # Log full history as artifact
    tracker.log_dict({"history": history}, "dspy_history.json")
    
    return metrics


# =============================================================================
# Specs Loading
# =============================================================================


def load_specs_from_folder(specs_folder: str) -> dict:
    """
    Load specification files from a folder.
    
    Expected structure:
    - schema_specs/bronze_tables.csv
    - schema_specs/silver_tables.csv
    - schema_specs/gold_tables.csv
    - transformation_specs/transformations_bronze_to_silver.csv
    - transformation_specs/transformations_silver_to_gold.csv
    - pipeline_specs/lakeflow_connect_pipeline.yaml
    
    Returns:
        Dictionary with all specs loaded
    """
    import csv
    from pathlib import Path
    
    import yaml
    
    specs_path = Path(specs_folder)
    specs = {}
    
    # Load CSV specs
    csv_files = {
        "bronze_tables": "schema_specs/bronze_tables.csv",
        "silver_tables": "schema_specs/silver_tables.csv",
        "gold_tables": "schema_specs/gold_tables.csv",
        "transformations_bronze_to_silver": "transformation_specs/transformations_bronze_to_silver.csv",
        "transformations_silver_to_gold": "transformation_specs/transformations_silver_to_gold.csv",
    }
    
    for key, rel_path in csv_files.items():
        file_path = specs_path / rel_path
        if file_path.exists():
            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                specs[key] = list(reader)
            logger.info("Loaded %s: %d rows", key, len(specs[key]))
        else:
            specs[key] = []
            logger.warning("Spec file not found: %s", file_path)
    
    # Load YAML pipeline config
    pipeline_files = list((specs_path / "pipeline_specs").glob("*.yaml"))
    if pipeline_files:
        with open(pipeline_files[0], encoding="utf-8") as f:
            specs["pipeline_config"] = yaml.safe_load(f)
        logger.info("Loaded pipeline config from: %s", pipeline_files[0])
    else:
        specs["pipeline_config"] = {}
    
    # Load markdown specs for additional context
    md_files = list(specs_path.rglob("*.md"))
    specs["markdown_specs"] = {}
    for md_file in md_files:
        with open(md_file, encoding="utf-8") as f:
            specs["markdown_specs"][md_file.stem] = f.read()
    
    return specs


# =============================================================================
# History Inspection (for debugging)
# =============================================================================


def inspect_history(n: int | None = None, log_level: int = logging.INFO) -> list[dict]:
    """
    Inspect and log DSPY LM call history.
    
    Args:
        n: Number of recent calls to show (None = all)
        log_level: Logging level for output
        
    Returns:
        List of history entries
    """
    lm = dspy.settings.lm
    if not lm or not hasattr(lm, "history"):
        logger.warning("No LM history available")
        return []
    
    history = lm.history[-n:] if n else lm.history
    
    for i, entry in enumerate(history):
        logger.log(log_level, "=== LM Call %d ===", i + 1)
        logger.log(log_level, "Model: %s", entry.get("model", "unknown"))
        logger.log(log_level, "Prompt tokens: %d", entry.get("usage", {}).get("prompt_tokens", 0))
        logger.log(log_level, "Completion tokens: %d", entry.get("usage", {}).get("completion_tokens", 0))
        
    return history


def get_history_summary() -> dict:
    """Get summary statistics from DSPY history."""
    lm = dspy.settings.lm
    if not lm or not hasattr(lm, "history"):
        return {"total_calls": 0}
    
    history = lm.history
    return {
        "total_calls": len(history),
        "total_input_tokens": sum(h.get("usage", {}).get("prompt_tokens", 0) for h in history),
        "total_output_tokens": sum(h.get("usage", {}).get("completion_tokens", 0) for h in history),
    }


def clear_history() -> None:
    """Clear the DSPY LM history."""
    lm = dspy.settings.lm
    if lm and hasattr(lm, "history"):
        lm.history.clear()
        logger.info("Cleared LM history")

