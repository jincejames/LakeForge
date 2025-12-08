from architect_demo.agent import ArchitectAgent
import logging

agent = ArchitectAgent(model="claude-opus")

result = agent.generate_design(
    project_name="servicenow_incident_lakehouse",
    specs_folder="architect_demo/specs",
)

agent.save_document(result, "DESIGN_DOCUMENT.md")

# Inspect DSPy history - see all LLM calls made
agent.inspect_history()

# Or get just a summary
summary = agent.get_history_summary()
logging.info("History summary: %s", summary)