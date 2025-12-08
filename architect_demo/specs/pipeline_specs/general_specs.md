# LakeFlow Connect Pipeline Specifications

## ServiceNow Incident Ingestion

- Use LakeFlow Connect with SERVICENOW source type for managed ingestion
- Schedule: Every 2 hours (cron: `0 0 */2 * * ?`)
- SCD Type 2 historization enabled for tracking incident state changes over time
- Primary key: `sys_id` (ServiceNow unique identifier)
- Destination: `main.servicenow_incident_lakeforge.incident`

## Key Design Decisions

- LakeFlow Connect handles schema evolution automatically
- SCD Type 2 provides `__START_AT` and `__END_AT` columns for temporal queries
- `_databricks_deleted` column tracks soft deletes from source
- No custom API code required - LakeFlow manages authentication, pagination, and rate limiting

