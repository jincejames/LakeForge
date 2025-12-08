# Schema Specifications - ServiceNow Incident

## Bronze Layer

- Table `bronze.incident` is ingested directly via LakeFlow Connect
- Schema matches ServiceNow incident table with nested STRUCT types preserved
- LakeFlow adds SCD2 columns: `__START_AT`, `__END_AT`, `_databricks_deleted`
- No transformation applied - raw data preservation

## Silver Layer

- `silver.incident_flattened`: Full historical view with all struct fields extracted
  - All STRUCT<link,value> columns flattened to `{column}_id` (extracts .value)
  - Derived columns for state/priority/urgency/impact names
  - Calculated time metrics (time_to_resolve_hours, etc.)
  - Preserves SCD2 columns for temporal queries

- `silver.incident_current`: Current-state view for operational reporting
  - Filtered to current records only (`__END_AT IS NULL AND _databricks_deleted = false`)
  - Optimized subset of columns for common queries

## Gold Layer

- `gold.incident_daily_metrics`: Daily aggregated KPIs for executive dashboards
- `gold.incident_by_category`: Category/subcategory analysis with monthly grain
- `gold.incident_by_assignment_group`: Team performance metrics
- `gold.incident_backlog_snapshot`: Daily backlog snapshots with aging buckets
- `gold.incident_trend_weekly`: Week-over-week trend analysis

## Data Quality Expectations

- `sys_id` must be non-null and unique per SCD2 version
- `opened_at` must be before `resolved_at` (when both exist)
- `resolved_at` must be before or equal to `closed_at` (when both exist)
- Priority values must be in range 1-5
- State values must be valid ServiceNow states (1,2,3,6,7,8)

