import os

import databricks_dspy
import dspy

# =============================================================================
# Authentication Options (pick one):
# =============================================================================

# Option 1: OAuth U2M (User-to-Machine) - Interactive browser login
# Just run: databricks auth login --host https://your-workspace.cloud.databricks.com
# Then run this script - no env vars needed!

# Option 2: OAuth M2M (Service Principal) - for automation
# os.environ["DATABRICKS_HOST"] = "https://your-workspace.cloud.databricks.com"
# os.environ["DATABRICKS_CLIENT_ID"] = "your-client-id"
# os.environ["DATABRICKS_CLIENT_SECRET"] = "your-client-secret"

# Option 3: PAT (less recommended)
# os.environ["DATABRICKS_HOST"] = "https://your-workspace.cloud.databricks.com"
# os.environ["DATABRICKS_TOKEN"] = "your-personal-access-token"

# Option 4: Azure-managed identity (if running on Azure)
# os.environ["DATABRICKS_HOST"] = "https://your-workspace.azuredatabricks.net"
# os.environ["ARM_USE_MSI"] = "true"

# =============================================================================

dspy.configure(lm=databricks_dspy.DatabricksLM(model="databricks/databricks-claude-opus-4-5"))

predict = dspy.Predict("question->answer")

print(predict(question="what is your context window?"))