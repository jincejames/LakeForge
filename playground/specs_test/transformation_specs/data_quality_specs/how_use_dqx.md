# How to Install the library via pip:

```
pip install databricks-labs-dqx
```


# How to Applying checks defined using metadata
- Users can save and load checks as metadata (list of dictionaries) from a storage through several supported methods, as described here. When loading checks from a storage, they are always returned as metadata (list of dictionaries). You can convert checks from metadata to classes and back using serialization methods described here.

- In the example below, checks are defined in YAML syntax for convenience and then loaded into a list of dictionaries.

```
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.sdk import WorkspaceClient


dq_engine = DQEngine(WorkspaceClient())

# checks can also be loaded from a storage
checks: list[dict] = yaml.safe_load("""
  - criticality: warn
    check:
      function: is_not_null
      arguments:
        column: col3

  - criticality: warn
    check:
      function: is_in_range
      arguments:
        column: age
        min_limit: 18
        max_limit: 120

  - criticality: warn
    check:
      function: is_in_list
      arguments:
        column: country
        allowed:
          - Germany
          - France
          - Spain

  - name: start_before_end
    criticality: error
    check:
      function: sql_expression
      arguments:
        expression: end_time >= start_time
        columns:
          - start_time
          - end_time

  - name: email_invalid_format
    criticality: error
    check:
      function: regex_match
      arguments:
        column: email
        regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$

  - criticality: error
    check:
      function: is_not_less_than
      arguments:
        column: amount
        limit: 0
""")

input_df = spark.read.table("catalog.schema.input")

# Option 1: apply quality checks on the DataFrame and output results as a single DataFrame
valid_and_invalid_df = dq_engine.apply_checks_by_metadata(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_and_invalid_df,
  output_config=OutputConfig(location="catalog.schema.output"),
)

# Option 2: apply quality checks on the DataFrame and provide valid and invalid (quarantined) DataFrames
valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
dq_engine.save_results_in_table(
  output_df=valid_df,
  quarantine_df=invalid_df,
  output_config=OutputConfig(location="catalog.schema.valid"),
  quarantine_config=OutputConfig(location="catalog.schema.quarantine"),
)

# Option 3 End-to-End approach: apply quality checks on the input table and save results to valid and invalid (quarantined) tables
dq_engine.apply_checks_by_metadata_and_save_in_table(
    checks=checks,
    input_config=InputConfig(location="catalog.schema.input"),
    output_config=OutputConfig(location="catalog.schema.valid"),
    quarantine_config=OutputConfig(location="catalog.schema.quarantine"),
)

# Option 4 End-to-End approach: apply quality checks on the input table and save results to an output table
dq_engine.apply_checks_by_metadata_and_save_in_table(
    checks=checks,
    input_config=InputConfig(location="catalog.schema.input"),
    output_config=OutputConfig(location="catalog.schema.output"),
)
```