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
