# Transformation Specifications - ServiceNow Incident

## Bronze to Silver Transformations

### Struct Flattening Strategy

The ServiceNow incident table contains many reference fields stored as `STRUCT<link:STRING, value:STRING>` where:
- `link`: API URL to the referenced record (not used in analytics)
- `value`: The `sys_id` of the referenced record (the foreign key)

**Optimized Approach (Use This)**:
```sql
-- Extract value directly from struct using dot notation
assigned_to.value AS assigned_to_id
```

**Avoid**: Python-based recursive JSON flattening at row level (as in legacy API approach) - this is slow and not parallelizable.

### Naming Conventions for Flattened Fields

- Reference fields: `{original_field}_id` (e.g., `assigned_to` → `assigned_to_id`)
- Derived names: `{field}_name` (e.g., `state` → `state_name`)
- Calculated times: `{metric}_hours` or `{metric}_seconds`
- Dates derived from timestamps: `{field}_date` (e.g., `opened_at` → `opened_date`)

### State/Priority Mapping

These mappings are ServiceNow-specific and should be maintained:

| Code | State Name |
|------|------------|
| 1 | New |
| 2 | In Progress |
| 3 | On Hold |
| 6 | Resolved |
| 7 | Closed |
| 8 | Canceled |

| Code | Priority Name |
|------|---------------|
| 1 | Critical |
| 2 | High |
| 3 | Moderate |
| 4 | Low |
| 5 | Planning |

### Time Calculations

All time calculations should use `UNIX_TIMESTAMP` for accurate hour calculations:
```sql
ROUND((UNIX_TIMESTAMP(resolved_at) - UNIX_TIMESTAMP(opened_at)) / 3600.0, 2)
```

## Silver to Gold Transformations

### Aggregation Patterns

- Use `FILTER (WHERE ...)` clause for conditional counts (Databricks SQL syntax)
- Use `PERCENTILE_CONT` for percentile calculations
- Use window functions for share/trend calculations
- Handle division by zero with `NULLIF`

### Time Grains

- Daily metrics: Use `DATE` columns directly
- Weekly: Use `DATE_TRUNC('week', date_column)` for ISO weeks
- Monthly: Use `DATE_TRUNC('month', date_column)`

### SLA Compliance Calculation

```sql
ROUND(sla_met_count * 100.0 / NULLIF(sla_met_count + sla_missed_count, 0), 2)
```

## Data Quality Checks

Apply before silver layer write:
- `sys_id` NOT NULL
- `opened_at` NOT NULL  
- `opened_at <= resolved_at` (when resolved_at is not null)
- `resolved_at <= closed_at` (when both exist)
- `priority BETWEEN 1 AND 5`
- `state IN (1, 2, 3, 6, 7, 8)`

