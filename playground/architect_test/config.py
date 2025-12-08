"""
Configuration for Lakehouse Architect Agent.

Handles LM setup with Databricks authentication options.
"""

import json
import logging
from typing import Literal

import databricks_dspy
import dspy

logger = logging.getLogger(__name__)


# =============================================================================
# DSPy History Inspection
# =============================================================================


def inspect_history(n: int | None = None, log_level: int = logging.INFO) -> list[dict]:
    """
    Inspect and log DSPy LM call history.

    Args:
        n: Number of recent calls to inspect (None = all)
        log_level: Logging level for output

    Returns:
        List of history entries as dicts
    """
    lm = dspy.settings.lm
    if lm is None:
        logger.warning("No LM configured - cannot inspect history")
        return []

    history = getattr(lm, "history", [])
    if not history:
        logger.info("No history available")
        return []

    # Get last n entries
    entries = history[-n:] if n else history

    for i, entry in enumerate(entries):
        step_num = len(history) - len(entries) + i + 1
        _log_history_entry(entry, step_num, log_level)

    return entries


def _log_history_entry(entry: dict, step_num: int, log_level: int = logging.INFO):
    """Log a single history entry with formatting."""
    separator = "=" * 80

    # Extract key info
    prompt = entry.get("prompt", entry.get("messages", "N/A"))
    response = entry.get("response", entry.get("outputs", "N/A"))
    kwargs = entry.get("kwargs", {})

    # Format prompt (handle both string and message list formats)
    if isinstance(prompt, list):
        prompt_str = "\n".join(
            f"[{m.get('role', 'unknown')}]: {m.get('content', '')[:500]}..."
            if len(m.get("content", "")) > 500
            else f"[{m.get('role', 'unknown')}]: {m.get('content', '')}"
            for m in prompt
        )
    else:
        prompt_str = str(prompt)[:1000] + "..." if len(str(prompt)) > 1000 else str(prompt)

    # Format response
    if isinstance(response, list) and response:
        response_str = str(response[0])[:1000]
        if len(str(response[0])) > 1000:
            response_str += "..."
    else:
        response_str = str(response)[:1000]
        if len(str(response)) > 1000:
            response_str += "..."

    log_msg = f"""
{separator}
STEP {step_num}
{separator}
PROMPT:
{prompt_str}

RESPONSE:
{response_str}

KWARGS: {json.dumps(kwargs, indent=2, default=str) if kwargs else 'None'}
{separator}
"""
    logger.log(log_level, log_msg)


def get_history_summary() -> dict:
    """
    Get a summary of the DSPy history.

    Returns:
        Dict with history statistics
    """
    lm = dspy.settings.lm
    if lm is None:
        return {"error": "No LM configured"}

    history = getattr(lm, "history", [])

    return {
        "total_calls": len(history),
        "calls": [
            {
                "step": i + 1,
                "prompt_length": len(str(entry.get("prompt", entry.get("messages", "")))),
                "response_length": len(str(entry.get("response", entry.get("outputs", "")))),
            }
            for i, entry in enumerate(history)
        ],
    }


def clear_history():
    """Clear the DSPy LM history."""
    lm = dspy.settings.lm
    if lm is not None and hasattr(lm, "history"):
        lm.history.clear()
        logger.info("DSPy history cleared")


# =============================================================================
# Model Configuration
# =============================================================================

# Available Databricks Foundation Models
AVAILABLE_MODELS = {
    "claude-opus": "databricks/databricks-claude-opus-4-5",
    "claude-opus-200k": "databricks/databricks-claude-opus-4-5",  # 200k context
    "claude-sonnet": "databricks/databricks-claude-sonnet-4",
    "llama-70b": "databricks/databricks-meta-llama-3-1-70b-instruct",
    "mixtral": "databricks/databricks-mixtral-8x7b-instruct",
}

# Model-specific default settings
MODEL_DEFAULTS = {
    "claude-opus-200k": {"temperature": 1.0, "max_tokens": 64000},
}

DEFAULT_MODEL = "claude-opus"


def configure_lm(
    model: str = DEFAULT_MODEL,
    temperature: float | None = None,
    max_tokens: int | None = None,
) -> dspy.LM:
    """
    Configure the language model for the architect agent.

    Authentication is handled automatically by databricks-dspy using:
    1. OAuth U2M (Interactive) - Run: databricks auth login --host <workspace-url>
    2. OAuth M2M (Service Principal) - Set env vars:
       - DATABRICKS_HOST
       - DATABRICKS_CLIENT_ID
       - DATABRICKS_CLIENT_SECRET
    3. PAT (Personal Access Token) - Set env vars:
       - DATABRICKS_HOST
       - DATABRICKS_TOKEN
    4. Azure Managed Identity - Set env vars:
       - DATABRICKS_HOST
       - ARM_USE_MSI=true

    Args:
        model: Model key from AVAILABLE_MODELS or full model path
        temperature: Sampling temperature (0.0-1.0), uses model default if None
        max_tokens: Maximum tokens in response, uses model default if None

    Returns:
        Configured DSPy LM instance
    """
    # Resolve model name
    model_path = AVAILABLE_MODELS.get(model, model)

    # Get model-specific defaults
    defaults = MODEL_DEFAULTS.get(model, {})
    final_temperature = temperature if temperature is not None else defaults.get("temperature", 0.7)
    final_max_tokens = max_tokens if max_tokens is not None else defaults.get("max_tokens", 4096)

    logger.info(
        "Configuring LM: model_key=%s, model_path=%s, temperature=%.1f, max_tokens=%d",
        model,
        model_path,
        final_temperature,
        final_max_tokens,
    )

    lm = databricks_dspy.DatabricksLM(
        model=model_path,
        temperature=final_temperature,
        max_tokens=final_max_tokens,
    )

    dspy.configure(lm=lm)
    return lm


def configure_for_design(
    complexity: Literal["simple", "standard", "complex"] = "standard",
) -> dspy.LM:
    """
    Configure LM with settings optimized for design document generation.

    Args:
        complexity: Expected complexity level
            - simple: Fewer tables, straightforward mappings
            - standard: Typical lakehouse with multiple tables
            - complex: Many tables, intricate relationships, high governance needs

    Returns:
        Configured DSPy LM instance
    """
    settings = {
        "simple": {"model": "claude-sonnet", "temperature": 0.5, "max_tokens": 2048},
        "standard": {"model": "claude-opus", "temperature": 0.7, "max_tokens": 4096},
        "complex": {"model": "claude-opus-200k"},  # Uses MODEL_DEFAULTS: temp=1.0, 200k tokens
    }

    config = settings.get(complexity, settings["standard"])
    return configure_lm(**config)


# =============================================================================
# Playbook Rules (for reference in prompts)
# =============================================================================

PLAYBOOK_RULES = {
    "bronze": {
        "principles": [
            "Zero transformation from source (mirror perfectly)",
            "Add metadata columns: _ingested_at, _source_file, _schema_version",
            "Never use DBFS blob storage",
            "Route to proper storage with production-grade throughput",
        ],
        "ingestion_strategies": {
            "small_non_type2": "Full pull each run, append timestamp",
            "type2_sources": "Incremental by update timestamp",
            "large_non_type2": "CDC with customer-agreed approach",
        },
        "validations": [
            "Count of distinct keys",
            "Time range verification",
            "Number of files per partition",
            "Average file size check",
        ],
    },
    "silver": {
        "principles": [
            "Flatten low-cardinality dimensions into facts",
            "Keep high-cardinality dimensions separate",
            "Apply all business mappings at this layer",
            "Implement CDC with _valid_to timestamp",
        ],
        "design_outputs": [
            "Partition strategy based on query patterns",
            "Z-order columns for join keys",
            "Validation rules",
            "Expected record counts and growth rates",
        ],
    },
    "gold": {
        "principles": [
            "Current + History table pattern",
            "Views as abstraction layer (never direct table access)",
            "Governance rules (≤5 per table)",
            "Proper naming conventions",
        ],
        "view_rules": [
            "Name every column explicitly (NO SELECT *)",
            "Map partition columns in predicate for pruning",
            "Alias every column for future remapping",
        ],
        "governance_rule_types": [
            "Range validation",
            "Cross-field validation",
            "Statistical validation",
            "Trend validation",
            "Boundary validation",
            "Temporal validation",
        ],
    },
    "architecture": {
        "databases": {
            "dw_bronze": "Raw data copies",
            "dw_silver": "Transformed, validated data",
            "gold_etl": "Persisted Gold tables",
            "gold": "Consumer-facing views only",
            "gold_hist": "Historical data tables",
        },
        "naming_conventions": {
            "partition_columns": "p_ prefix (e.g., p_yyyymm)",
            "physical_tables": "_t suffix (e.g., campaign_t)",
            "history_tables": "_hist suffix (e.g., campaign_t_hist)",
            "views": "No suffix (e.g., campaign)",
        },
        "critical_rules": [
            "Gold must be in dedicated bucket/storage account",
            "Separate Gold from Bronze/Silver to avoid contention",
            "Do NOT use bucketing",
            "Do NOT apply auto-optimize to Z-ordered tables",
        ],
    },
    "risk_triggers": {
        "non_type2_large": {
            "trigger": "No incremental capture strategy",
            "action": "Document explicit plan, get customer signoff",
        },
        "unclear_mappings": {
            "trigger": "Transformation logic not clearly defined",
            "action": "HALT - escalate immediately, project at risk",
        },
        "data_quality": {
            "trigger": "Validation failures in testing",
            "action": "Delegate to customer SME for investigation",
        },
    },
}

