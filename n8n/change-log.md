# Change Log

## Step 1: Initialize Databricks Asset Bundle Project

**Date:** 2024-12-08

**Action:** Initialized a Databricks Asset Bundle project inside the `demo` folder.

**Changes Made:**
- Created `demo/` folder structure
- Created `demo/databricks.yml` - Main bundle configuration file with:
  - Include configuration for resources
  - Environment variables setup
  - Dev target configuration (default, development mode)
- Created folder structure:
  - `demo/src/` - For Python modules and utilities
  - `demo/notebooks/` - For Databricks notebooks
  - `demo/resources/` - For bundle resource definitions (jobs, dashboards, etc.)

**Status:** ✅ Completed

## Step 2: Build Bundle Configuration from Workspace and Catalog Details

**Date:** 2024-12-08

**Action:** Built bundle configuration from workspace and catalog details for dev environment in `specs/databricks_specs` folder.

**Changes Made:**
- Created `specs/databricks_specs/` folder structure
- Created `specs/databricks_specs/workspace_catalog_config.yml` - Configuration specification file containing:
  - Workspace configuration for dev environment (host, root_path)
  - Catalog configuration for dev environment (catalog name)
  - Additional configuration settings (database prefix, storage root)
- Updated `demo/databricks.yml` to include:
  - Workspace configuration variables (databricks_host, workspace_root_path)
  - Catalog configuration variable (catalog_name)
  - Workspace target configuration in dev target (host, root_path)
  - Catalog variable reference in dev target variables

**Status:** ✅ Completed
