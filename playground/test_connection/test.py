import logging
import sys
from pathlib import Path

# Configure logging to see DSPy steps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Add parent directory to path so we can import architect
sys.path.insert(0, str(Path(__file__).parent.parent))

from architect import ArchitectAgent, ProjectSpec, SourceTable

orders = SourceTable(
    name="orders",
    schema="id INT, total DECIMAL",
)

project = ProjectSpec(
    name="retail",
    cloud_provider="AWS",
    source_tables=[orders],
)

# Use claude-opus-200k model (temperature=1.0, max_tokens=200000)
agent = ArchitectAgent(model="claude-opus-200k")
result = agent.generate_design(project)
agent.save_document(result, "DESIGN_DOCUMENT2.md")

# Inspect DSPy history - see all LLM calls made
agent.inspect_history()

# Or get just a summary
summary = agent.get_history_summary()
logging.info("History summary: %s", summary)