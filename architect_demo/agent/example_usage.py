"""
Example usage of the Lakehouse Architect Agent.

This example shows how to:
1. Initialize the agent with MLflow tracking
2. Generate a design document from specs
3. Save the output for the coder agent
"""

import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

from architect_demo.agent import ArchitectAgent, quick_design

# =============================================================================
# Example 1: Full control with ArchitectAgent
# =============================================================================


def example_full_control():
    """Use ArchitectAgent for full control over the process."""
    
    # Initialize agent with model choice and MLflow experiment
    agent = ArchitectAgent(
        model="claude-opus",  # or "claude-opus" for more complex designs
        experiment_name="servicenow-lakehouse-design",
    )
    
    # Generate design from specs folder
    result = agent.generate_design(
        project_name="servicenow_incident_lakehouse",
        specs_folder="architect_demo/specs",
        tags={
            "source_system": "ServiceNow",
            "use_case": "Incident Analytics",
        },
    )
    
    # Save outputs
    output_dir = Path("architect_demo/output")
    output_dir.mkdir(exist_ok=True)
    
    agent.save_document(result, output_dir / "DESIGN_DOCUMENT.md")
    agent.save_implementation_steps(result, output_dir / "IMPLEMENTATION_STEPS.md")
    
    # Inspect what the LLM did
    summary = agent.get_history_summary()
    logging.info("Total LM calls: %d", summary["total_calls"])
    logging.info("Total tokens: %d", summary["total_input_tokens"] + summary["total_output_tokens"])
    
    # Check for escalations
    if result.escalation_flags and result.escalation_flags.strip():
        logging.warning("ESCALATION REQUIRED:\n%s", result.escalation_flags)
    
    return result


# =============================================================================
# Example 2: Quick one-liner
# =============================================================================


def example_quick():
    """Use quick_design for simple one-liner usage."""
    
    output_path = quick_design(
        project_name="servicenow_incident_lakehouse",
        specs_folder="architect_demo/specs",
        output_path="architect_demo/output/DESIGN_DOCUMENT.md",
        model="claude-sonnet",
    )
    
    logging.info("Design document saved to: %s", output_path)
    return output_path


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    # Run the full control example
    result = example_full_control()
    
    # Print a preview of the design document
    logging.info("=== Design Document Preview ===")
    logging.info(result.design_document[:1000] + "...")

