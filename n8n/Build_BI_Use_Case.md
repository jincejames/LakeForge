- Your are a Data and BI engineer who is going to build, test and deploy a End to End  BI use case by following the technical specification & instrucions written by your Lead Data and BI Architect as follows:

- General Instructions :
    - IMPORTANT: Always configure Job to run on serverless compute (omit cluster configuration in jobs yml). As jobs run on databricks serverless compute (without retries), do not install dependent libraries on the cluster, instead add installation(consider most recent version) command using pip in the scripts or notebooks itself.
    - Remember to modularize the code i.e create common utils as python modules. Make sure notebooks are outside of src/ folder and make relative path reference to the modules / utils, e.g. demo/notebooks and demo/src then
    ```
        cwd = os.getcwd()
        p = os.path.join(cwd, '..', 'src')
        if os.path.isdir(p) and p not in sys.path:
            sys.path.insert(0, p)
    ```
    - Make sure the unit tests are tested locally to avoid issues when running on databricks workspace.
    - Make sure to use a python linter and code formatter tools and improve code quality and consistency. also review the code and refactor as needed.
    - Make sure and double check if all the steps listed below are in the TODO list.
    - Always remember to read these instructions one more time whenever face error during any steps.
    - DO NOT CREATE:  markdown files such as readme, guide , summary, report or checklist unless i ask for it.

- Project Build and Test steps as follows:
    - Step 1: Initial a project inside a folder with name "demo" using databricks asset bundle.
    - Step 2: Build bundle configuration from workspace and catalog details (only dev is needed) in specs/databricks_specs folder.
    - Step 3: Create python notebooks needed for schema and tables creation as per the specs/schema_specs folder, catalog must exists do not include creation code in the notebooks.
    - Step 4: Create python notebooks needed for loading the tables and job resource to orchestrate them as in specs/transformation_specs. 
    - Step 5: Include data quality quality controls in the transformation python notebooks as per the specs/transformation_specs/data_quality_specs, please use existing `databricks-labs-dqx` library version 0.9.3, refer to the docs in https://databrickslabs.github.io/dqx/, on how to use it (do not create own implementation).
    - Step 6: Build AI BI Dashboard resource json(sample json as in specs/dashboard_specs/sample_dashboard.json) as per each dashboard specs in specs/dashboard_specs. Refer to the official document one how to generate dashboard json. once json generated , make sure to refer it in the resources yml. As this is complex task, suggest to use databricks sdk (`databricks-sdk>=0.57.0`) for dashboard json creation.
    - Step 7: Must Create proper unit tests for the common utils, should test these utils , do do not create own isolated tests.
    - Step 8: Run the unit tests on local spark (create & consider dedicated virtual envrionment and install all the needed libararies including databricks-labs-dqx) and fix if there are any issues.
    - Step 9: Also generate script and join to generate and load sample data into bronze tables.
    - Step 10: Before deploying the notebooks make sure the transformation code has mapped the target table column data types properly. 
    - Step 11: Deploy project code and jobs & dashboard resources using databricks cli (bundle) into dev workspace. Must exclude the files to be ignored (e.g. as per .gitignore).
    - Step 12: Run the deployed jobs in the correct order.



