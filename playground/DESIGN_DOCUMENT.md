# Design Document: End-to-End BI Use Case - Lakehouse Implementation

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Database Schema Design](#database-schema-design)
5. [Transformation Logic](#transformation-logic)
6. [Data Quality Controls](#data-quality-controls)
7. [Dashboard Specifications](#dashboard-specifications)
8. [Step-by-Step Implementation Guide](#step-by-step-implementation-guide)
9. [Technical Requirements](#technical-requirements)

---

## 1. Overview

This document provides comprehensive design specifications for building an end-to-end BI use case using Databricks Lakehouse architecture. The solution follows a medallion architecture (Bronze → Silver → Gold) with integrated data quality controls and AI-powered BI dashboards.

**Use Case**: E-commerce Analytics Platform
- Track orders, customers, products, inventory, payments, and web events
- Provide business metrics for sales, product performance, customer lifetime value, and cohort retention
- Enable AI-powered customer churn prediction features

---

## 2. Prerequisites

Before starting implementation, ensure the following are in place:

1. **Databricks Workspace**
   - Serverless compute enabled
   - SQL warehouse created and running
   - Workspace URL: `https://dbc-2ff08614-a866.cloud.databricks.com`
   - SQL Warehouse ID: `85fcef036597adbd`

2. **Catalog Setup**
   - Catalog name: `demo_dev`
   - Catalog attached to workspace
   - Attached to shared metastore

3. **Access & Permissions**
   - Principal with workspace access
   - Serverless SQL warehouse access
   - Serverless job execution permissions
   - Catalog-level grants: CREATE SCHEMA, CREATE TABLE

4. **Local Development Environment**
   - Databricks CLI installed and configured (default profile)
   - Python version matching Databricks runtime
   - Virtual environment for local testing

---

## 3. Architecture

### 3.1 Medallion Architecture Overview

```
┌─────────────┐
│   Bronze    │  Raw ingestion (as-is data)
│   Layer     │  - orders_raw, customers_raw, products_raw
└──────┬──────┘  - order_items_raw, payments_raw, inventory_raw
       │         - web_events_raw
       ↓
┌─────────────┐
│   Silver    │  Cleansed & conformed data
│   Layer     │  - dim_customer (SCD2), dim_product, dim_time
└──────┬──────┘  - fact_order, fact_order_item, fact_web_event
       │
       ↓
┌─────────────┐
│    Gold     │  Business-ready aggregates
│   Layer     │  - sales_dashboard_metrics
└─────────────┘  - product_sales_by_month
                 - customer_lifetime_value_features
                 - cohort_retention
```

### 3.2 Compute Strategy
- **All jobs run on Databricks Serverless compute** (no cluster configuration)
- No library installation on clusters; use pip install in scripts/notebooks
- Job retries disabled (serverless default)

### 3.3 Code Organization
```
demo/
├── src/                    # Python modules & utils
│   ├── common/
│   ├── transformations/
│   └── data_quality/
├── notebooks/              # Databricks notebooks (outside src/)
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── data_generation/
├── tests/                  # Unit tests
├── resources/              # Databricks bundle resources
│   ├── jobs.yml
│   └── dashboards.yml
└── databricks.yml          # Bundle configuration
```

---

## 4. Database Schema Design

### 4.1 Bronze Layer Tables

All bronze tables follow raw ingestion pattern with metadata columns:
- `_ingest_time` (TIMESTAMP): System-generated ingestion timestamp
- `_file_name` (STRING): Source file/object name
- `_source` (STRING): Source system identifier

#### Table: `bronze.orders_raw`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | NO | Source order identifier (raw) |
| customer_id | STRING | YES | Source customer identifier |
| order_timestamp | STRING | YES | Order timestamp in source format |
| status | STRING | YES | Order status from source |
| currency_code | STRING | YES | ISO currency code (as provided) |
| total_amount | STRING | YES | Total order amount (as provided) |
| channel | STRING | YES | Order channel (web, store, app) |
| _ingest_time | TIMESTAMP | NO | Ingestion time (system) |
| _file_name | STRING | YES | Source file name |
| _source | STRING | YES | Source system |

#### Table: `bronze.order_items_raw`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_item_id | STRING | NO | Source order item id |
| order_id | STRING | NO | Parent order id |
| product_id | STRING | YES | Product id |
| quantity | STRING | YES | Item quantity (type-cast in silver) |
| unit_price | STRING | YES | Unit price |
| tax_amount | STRING | YES | Tax amount |
| discount_amount | STRING | YES | Discount amount |
| _ingest_time | TIMESTAMP | NO | Ingestion time |
| _file_name | STRING | YES | Source file name |
| _source | STRING | YES | Source system |

#### Table: `bronze.customers_raw`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_id | STRING | NO | Source customer id |
| first_name | STRING | YES | First name |
| last_name | STRING | YES | Last name |
| email | STRING | YES | Email address (sensitive) |
| phone | STRING | YES | Phone number |
| country_code | STRING | YES | Country (normalize in silver) |
| created_at | STRING | YES | Customer creation time |
| updated_at | STRING | YES | Last update time |
| _ingest_time | TIMESTAMP | NO | Ingestion time |
| _source | STRING | YES | Source application |

#### Table: `bronze.products_raw`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_id | STRING | NO | Source product id |
| name | STRING | YES | Product name |
| category | STRING | YES | Category (normalize in silver) |
| list_price | STRING | YES | List price |
| currency_code | STRING | YES | Currency code |
| is_active | STRING | YES | Active flag (cast to BOOLEAN) |
| _ingest_time | TIMESTAMP | NO | Ingestion time |

#### Table: `bronze.web_events_raw`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| event_id | STRING | NO | Event id |
| customer_id | STRING | YES | Customer id (nullable for anonymous) |
| session_id | STRING | YES | Session id |
| event_type | STRING | YES | Event type (view, add_to_cart) |
| event_timestamp | STRING | YES | Event timestamp |
| source | STRING | YES | Source (web, app) |
| _ingest_time | TIMESTAMP | NO | Ingestion time |

#### Table: `bronze.payments_raw`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| payment_id | STRING | NO | Payment id |
| order_id | STRING | YES | Order id |
| amount | STRING | YES | Payment amount |
| currency_code | STRING | YES | Currency code |
| status | STRING | YES | Payment status |
| paid_at | STRING | YES | Payment timestamp |
| _ingest_time | TIMESTAMP | NO | Ingestion time |

#### Table: `bronze.inventory_raw`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_id | STRING | NO | Product id |
| location_id | STRING | NO | Warehouse/store id |
| quantity_on_hand | STRING | YES | Quantity on hand |
| updated_at | STRING | YES | Update timestamp |
| _ingest_time | TIMESTAMP | NO | Ingestion time |

---

### 4.2 Silver Layer Tables

#### Table: `silver.dim_customer` (SCD Type 2)
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_id | STRING | NO | Business key |
| first_name | STRING | YES | First name (standardized UTF-8) |
| last_name | STRING | YES | Last name (standardized UTF-8) |
| email | STRING | YES | Email (optionally hashed for PII) |
| phone | STRING | YES | Phone (normalized format) |
| country_code | STRING | YES | ISO country (uppercase, conformed) |
| created_at | TIMESTAMP | YES | Created timestamp (parsed) |
| updated_at | TIMESTAMP | YES | Updated timestamp (parsed) |
| effective_from | TIMESTAMP | NO | SCD2 start date (derived) |
| effective_to | TIMESTAMP | YES | SCD2 end date (derived) |
| is_current | BOOLEAN | NO | SCD2 current record flag |

#### Table: `silver.dim_product`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_id | STRING | NO | Business key |
| name | STRING | YES | Product name (trimmed) |
| category | STRING | YES | Conformed category |
| list_price | DECIMAL(12,2) | YES | List price (cast) |
| currency_code | STRING | YES | ISO currency (uppercase) |
| is_active | BOOLEAN | YES | Active flag (cast) |
| updated_at | TIMESTAMP | YES | Updated timestamp (parsed) |

#### Table: `silver.dim_time`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| date_key | INT | NO | YYYYMMDD surrogate key |
| date | DATE | NO | Calendar date |
| year | INT | NO | Year |
| month | INT | NO | Month (1-12) |
| week_of_year | INT | NO | ISO week |
| day_of_week | INT | NO | Day of week (1-7) |

**Note**: Explicit schema casting required for `dim_time`.

#### Table: `silver.fact_order`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_id | STRING | NO | Order id (business key) |
| customer_id | STRING | YES | Customer id (FK to dim_customer) |
| order_date | DATE | YES | Order date (derived from timestamp) |
| order_timestamp | TIMESTAMP | YES | Order timestamp (parsed) |
| status | STRING | YES | Conformed order status |
| total_amount | DECIMAL(12,2) | YES | Order amount (cast) |
| currency_code | STRING | YES | ISO currency (uppercase) |
| channel | STRING | YES | Conformed channel |
| payment_status | STRING | YES | Payment status (joined from payments) |

#### Table: `silver.fact_order_item`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| order_item_id | STRING | NO | Order item id (business key) |
| order_id | STRING | NO | Order id (FK to fact_order) |
| product_id | STRING | YES | Product id (FK to dim_product) |
| quantity | INT | YES | Quantity (cast) |
| unit_price | DECIMAL(12,2) | YES | Unit price (cast) |
| extended_amount | DECIMAL(12,2) | YES | Quantity * unit_price (derived) |
| tax_amount | DECIMAL(12,2) | YES | Tax (cast) |
| discount_amount | DECIMAL(12,2) | YES | Discount (cast) |

#### Table: `silver.fact_web_event`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| event_id | STRING | NO | Event id (business key) |
| customer_id | STRING | YES | Customer id (FK to dim_customer) |
| session_id | STRING | YES | Session id (normalized) |
| event_type | STRING | YES | Conformed event type |
| event_timestamp | TIMESTAMP | YES | Event time (parsed UTC) |
| source | STRING | YES | Conformed source |

---

### 4.3 Gold Layer Tables

#### Table: `gold.sales_dashboard_metrics`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| metric_date | DATE | NO | Date grain (from dim_time) |
| total_orders | BIGINT | NO | Orders count (aggregated) |
| total_sales | DECIMAL(18,2) | NO | Total revenue (aggregated) |
| avg_order_value | DECIMAL(12,2) | YES | Avg order value (aggregated) |
| orders_web | BIGINT | YES | Orders via web (filtered agg) |
| orders_store | BIGINT | YES | Orders via store (filtered agg) |

#### Table: `gold.product_sales_by_month`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_id | STRING | NO | Product id (FK to dim_product) |
| month | DATE | NO | Month grain (first day of month) |
| units_sold | BIGINT | YES | Units sold (aggregated) |
| revenue | DECIMAL(18,2) | YES | Revenue (aggregated) |
| returns_rate | DECIMAL(5,2) | YES | % returns (derived) |
| revenue_share | DECIMAL(5,2) | YES | Product % share (derived) |

#### Table: `gold.customer_lifetime_value_features`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_id | STRING | NO | Customer id (FK to dim_customer) |
| total_spent | DECIMAL(18,2) | YES | Lifetime revenue (aggregated) |
| order_count | BIGINT | YES | Total orders (aggregated) |
| avg_days_between_orders | DECIMAL(8,2) | YES | Avg inter-order days (derived) |
| recency_days | INT | YES | Days since last order (derived) |
| churn_risk_score | DECIMAL(5,2) | YES | Model-ready score (derived feature) |

#### Table: `gold.cohort_retention`
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cohort_month | DATE | NO | Month of first order (derived) |
| month_offset | INT | NO | Months since cohort start (derived) |
| active_customers | BIGINT | YES | Active customers (aggregated) |
| retention_rate | DECIMAL(5,2) | YES | % retained (derived) |

---

## 5. Transformation Logic

### 5.1 Bronze to Silver Transformations

#### 5.1.1 Orders Processing (`bronze.orders_raw` → `silver.fact_order`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| type_cast_amount | Cast order amount to DECIMAL | `CAST(total_amount AS DECIMAL(12,2))` | Handle non-numeric via expectation |
| parse_order_ts | Parse timestamp to UTC | `to_timestamp(order_timestamp) AT TIME ZONE 'UTC'` | Handle invalid via quarantine |
| normalize_currency | Uppercase currency code | `upper(currency_code)` | ISO set validation |
| normalize_status | Map raw status to conformed set | `CASE status WHEN 'shipped' THEN 'SHIPPED' ... END` | Define canonical list |
| deduplicate_orders | Drop duplicates by latest ingest | `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingest_time DESC) = 1` | Use _record_hash if available |

#### 5.1.2 Order Items Processing (`bronze.order_items_raw` → `silver.fact_order_item`)

| Transformation | Description | SQL Expression |
|----------------|-------------|----------------|
| type_cast_item_fields | Cast quantity/prices | `CAST(quantity AS INT), CAST(unit_price AS DECIMAL(12,2))` |
| derive_extended_amount | Compute line total | `quantity * unit_price` |

#### 5.1.3 Customers Processing (`bronze.customers_raw` → `silver.dim_customer`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| parse_customer_ts | Parse created/updated timestamps | `to_timestamp(created_at), to_timestamp(updated_at)` | UTC normalization |
| scd2_changes | Apply SCD2 on customer attributes | `APPLY CHANGES INTO dim_customer ...` | Use business key customer_id |
| normalize_country | Conform ISO country | `upper(trim(country_code))` | Map aliases |

#### 5.1.4 Products Processing (`bronze.products_raw` → `silver.dim_product`)

| Transformation | Description | SQL Expression |
|----------------|-------------|----------------|
| cast_product_fields | Cast list_price/is_active | `CAST(list_price AS DECIMAL(12,2)), CAST(is_active AS BOOLEAN)` |

#### 5.1.5 Web Events Processing (`bronze.web_events_raw` → `silver.fact_web_event`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| parse_event_ts | Parse event timestamp to UTC | `to_timestamp(event_timestamp) AT TIME ZONE 'UTC'` | Late/out-of-order handling |
| normalize_event_type | Map raw event types | `CASE event_type WHEN 'view' THEN 'VIEW' ... END` | Define canonical list |

#### 5.1.6 Payment Enrichment (`bronze.payments_raw` → `silver.fact_order`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| join_payment_status | Enrich orders with payment_status | `LEFT JOIN payments ON order_id` | Prefer latest paid_at |

---

### 5.2 Silver to Gold Transformations

#### 5.2.1 Daily Sales Metrics (`silver.fact_order` → `gold.sales_dashboard_metrics`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| agg_daily_sales | Daily totals and AOV | `SUM(total_amount), COUNT(*) as total_orders, SUM(total_amount)/NULLIF(COUNT(*),0)` | Group by order_date |
| split_by_channel | Web vs store counts | `SUM(CASE WHEN channel='WEB' THEN 1 ELSE 0 END)` | Group by order_date |

#### 5.2.2 Product Sales by Month (`silver.fact_order_item` → `gold.product_sales_by_month`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| monthly_product_sales | Units & revenue per product/month | `SUM(quantity), SUM(extended_amount)` | Group by product_id, month(order_date) |

#### 5.2.3 Customer Lifetime Value Features (`silver.dim_customer`, `silver.fact_order` → `gold.customer_lifetime_value_features`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| clv_features | Spend, frequency, recency | `SUM(total_amount), COUNT(order_id), DATEDIFF(max(order_date), min(order_date))` | Window functions per customer_id |

#### 5.2.4 Cohort Retention (`silver.fact_order` → `gold.cohort_retention`)

| Transformation | Description | SQL Expression | Notes |
|----------------|-------------|----------------|-------|
| cohort_build | Build cohorts and retention | `MIN(order_date) as cohort_month; retention over month_offset` | Use dim_time joins |

---

## 6. Data Quality Controls

### 6.1 DQX Library Configuration

**Library**: `databricks-labs-dqx` version `0.9.3`
**Documentation**: https://databrickslabs.github.io/dqx/

#### Installation
```python
pip install databricks-labs-dqx==0.9.3
```

#### Usage Pattern
```python
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import InputConfig, OutputConfig
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Apply checks and split valid/invalid records
valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
```

---

### 6.2 Data Quality Checks Catalog

#### Bronze to Silver Checks

##### silver.fact_order
| Check Name | Criticality | Function | Column | Parameters | Message |
|------------|-------------|----------|--------|------------|---------|
| valid_order_id | error | is_not_null | order_id | - | Ensure key presence |
| non_negative_amount | error | is_not_less_than | total_amount | min_limit: 0 | No negative revenue |
| valid_currency | warn | is_in_list | currency_code | allowed: ['USD','EUR','GBP','INR','JPY'] | Conform to ISO set |
| no_duplicate_order | error | sql_expression | - | expression: `COUNT(DISTINCT order_id) = COUNT(order_id)` | Prevent duplicates |
| parsable_order_ts | error | sql_expression | - | expression: `to_timestamp(order_timestamp) IS NOT NULL` | Timestamp integrity |

##### silver.fact_order_item
| Check Name | Criticality | Function | Column | Parameters | Message |
|------------|-------------|----------|--------|------------|---------|
| valid_quantity | error | is_in_range | quantity | min: 1, max: 9999 | Reasonable quantity bounds |

##### silver.dim_customer
| Check Name | Criticality | Function | Column | Parameters | Message |
|------------|-------------|----------|--------|------------|---------|
| valid_email_format | warn | regex_match | email | regex: `^[A-Za-z0-9.*%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,}$` | Soft check on email |

##### silver.dim_product
| Check Name | Criticality | Function | Column | Parameters | Message |
|------------|-------------|----------|--------|------------|---------|
| valid_price | error | is_not_less_than | list_price | min_limit: 0 | No negative prices |

#### Silver to Gold Checks

##### gold.product_sales_by_month
| Check Name | Criticality | Function | Column | Parameters | Message |
|------------|-------------|----------|--------|------------|---------|
| valid_monthly_units | error | is_not_less_than | units_sold | min_limit: 0 | No negative units |

##### gold.customer_lifetime_value_features
| Check Name | Criticality | Function | Column | Parameters | Message |
|------------|-------------|----------|--------|------------|---------|
| referential_integrity | error | sql_expression | - | expression: `customer_id IN (SELECT customer_id FROM silver.dim_customer WHERE is_current = true)` | Gold references current dims |

##### gold.cohort_retention
| Check Name | Criticality | Function | Column | Parameters | Message |
|------------|-------------|----------|--------|------------|---------|
| valid_retention_bounds | error | is_in_range | retention_rate | min: 0, max: 100 | Percent in [0,100] |

---

## 7. Dashboard Specifications

### 7.1 Dashboard Catalog

| Dashboard Name | Purpose | Primary Gold Table |
|----------------|---------|-------------------|
| Sales Overview | Monitor daily revenue and orders | gold.sales_dashboard_metrics |
| Product Performance | Track monthly product sales and returns | gold.product_sales_by_month |
| Customer 360 & AI Features | Analyze CLV | gold.customer_lifetime_value_features |
| Cohort Retention | Measure retention by acquisition cohort | gold.cohort_retention |
| Executive Summary | Top-level KPIs and trends | Multiple (rollups) |

---

### 7.2 Dashboard: Sales Overview

#### Widgets/Metrics

| Widget | Type | Source Table | Metric | Grain | Parameters | Notes |
|--------|------|--------------|--------|-------|------------|-------|
| Total Sales | Counter | gold.sales_dashboard_metrics | total_sales (SUM) | Day | :date_range, :channel | Primary KPI |
| Orders by Channel | Line | gold.sales_dashboard_metrics | orders_web, orders_store (SUM) | Day | :date_range | Stacked bar |
| AOV Trend | Line | gold.sales_dashboard_metrics | avg_order_value (AVG) | Day | :date_range | Sparkline or line |

#### Filters
- Date Range Picker: `metric_date`
- Multi-Select: `channel`

---

### 7.3 Dashboard: Product Performance

#### Widgets/Metrics

| Widget | Type | Source Table | Metric | Grain | Parameters | Notes |
|--------|------|--------------|--------|-------|------------|-------|
| Top Products by Revenue | Bar (horizontal) | gold.product_sales_by_month | revenue (SUM) | Month | :month | Top N products |
| Returns Rate Heatmap | Heatmap | gold.product_sales_by_month | returns_rate (AVG) | Month | :month | Quality hotspot |

#### Filters
- Single-Select: `category`

---

### 7.4 Dashboard: Customer 360 & AI Features

#### Widgets/Metrics

| Widget | Type | Source Table | Metric | Grain | Parameters | Notes |
|--------|------|--------------|--------|-------|------------|-------|
| Churn Risk Scatter | Scatter | gold.customer_lifetime_value_features | churn_risk_score (AVG) | Customer | :country, :risk_decile | Segment focus |
| Recency Histogram | Histogram | gold.customer_lifetime_value_features | recency_days (AVG) | Customer | :country | Engagement distribution |

#### Filters
- Multi-Select: `country_code`

---

### 7.5 Dashboard: Cohort Retention

#### Widgets/Metrics

| Widget | Type | Source Table | Metric | Grain | Parameters | Notes |
|--------|------|--------------|--------|-------|------------|-------|
| Cohort Matrix | Heatmap | gold.cohort_retention | retention_rate (AVG) | Cohort Month + Month Offset | :cohort_month | Heatmap grid |

---

### 7.6 Dashboard: Executive Summary

#### Widgets/Metrics

| Widget | Type | Source Table | Metric | Grain | Parameters | Notes |
|--------|------|--------------|--------|-------|------------|-------|
| 12-Mo Sales Trend | Line | gold.sales_dashboard_metrics | total_sales (SUM) | Month | :month | Sparkline |
| Channel Mini Charts | Line | gold.sales_dashboard_metrics | total_orders (SUM) | Month | :month, :channel | Small multiples |

---

### 7.7 Dimensions Catalog

| Dimension | Grain | Source Table | Key Column | Attributes | SCD Type |
|-----------|-------|--------------|------------|------------|----------|
| date | Day | gold.sales_dashboard_metrics | metric_date | year, month, week_of_year, day_of_week | None |
| month | Month | gold.product_sales_by_month | month | year, month_number | None |
| channel | Channel | gold.sales_dashboard_metrics | channel | channel_group | None |
| product | Product | gold.product_sales_by_month | product_id | name, category, is_active | None |
| customer | Customer | gold.customer_lifetime_value_features | customer_id | name, email, country_code | SCD2 (silver) |
| cohort | Acquisition Cohort | gold.cohort_retention | cohort_month | year, month_number | None |

---

### 7.8 Measures Catalog

| Measure | Definition | Source Table | Field | Aggregation | Grain | Dimensionality |
|---------|------------|--------------|-------|-------------|-------|----------------|
| total_sales | Sum of revenue over period | gold.sales_dashboard_metrics | total_sales | SUM | Day | date; channel |
| total_orders | Count of orders over period | gold.sales_dashboard_metrics | total_orders | SUM | Day | date; channel |
| avg_order_value | Revenue per order | gold.sales_dashboard_metrics | avg_order_value | AVG | Day | date; channel |
| orders_web | Orders via web channel | gold.sales_dashboard_metrics | orders_web | SUM | Day | date |
| orders_store | Orders via store channel | gold.sales_dashboard_metrics | orders_store | SUM | Day | date |
| units_sold | Total units sold per product-month | gold.product_sales_by_month | units_sold | SUM | Month | product; month |
| product_revenue | Total product revenue per month | gold.product_sales_by_month | revenue | SUM | Month | product; month |
| returns_rate | Percentage of returned units | gold.product_sales_by_month | returns_rate | AVG | Month | product; month |
| customer_total_spent | Lifetime revenue per customer | gold.customer_lifetime_value_features | total_spent | SUM | Customer | customer |
| customer_order_count | Lifetime orders per customer | gold.customer_lifetime_value_features | order_count | SUM | Customer | customer |
| recency_days | Days since last order | gold.customer_lifetime_value_features | recency_days | AVG | Customer | customer |
| churn_risk_score | Model-ready churn risk score (0–100) | gold.customer_lifetime_value_features | churn_risk_score | AVG | Customer | customer |
| active_customers | Active customers in cohort/time | gold.cohort_retention | active_customers | SUM | Cohort Month + Month Offset | cohort_month; month_offset |
| retention_rate | Retention percentage | gold.cohort_retention | retention_rate | AVG | Cohort Month + Month Offset | cohort_month; month_offset |

---

### 7.9 Dashboard JSON Generation Notes

- Reference: `specs/dashboard_specs/sample_dashboard.json`
- Use Databricks SDK (`databricks-sdk>=0.57.0`) for dashboard JSON creation
- Generate SQL queries with datasets/queryLines (new line char at end of each line)
- Validate SQL queries using SQL Statement Execution REST API
- Configure all widgets correctly per visuals_spec.csv
- Deploy as dashboard resources using Databricks Asset Bundle

---

## 8. Step-by-Step Implementation Guide

### Step 1: Initialize Project with Databricks Asset Bundle

**Objective**: Create the project structure using Databricks Asset Bundle.

**Actions**:
1. Create a folder named `demo` in the workspace
2. Initialize Databricks Asset Bundle:
   ```bash
   mkdir demo
   databricks bundle init
   ```
3. Set up the following directory structure:
   ```
   demo/
   ├── databricks.yml
   ├── src/
   │   ├── common/
   │   ├── transformations/
   │   └── data_quality/
   ├── notebooks/
   │   ├── bronze/
   │   ├── silver/
   │   ├── gold/
   │   └── data_generation/
   ├── tests/
   └── resources/
       ├── jobs.yml
       └── dashboards.yml
   ```

**Deliverables**:
- Initialized `databricks.yml` bundle configuration
- Proper folder structure

---

### Step 2: Build Bundle Configuration

**Objective**: Configure Databricks Asset Bundle with workspace and catalog details.

**Actions**:
1. Read workspace specifications from `specs/databricks_specs/workspace_spec.csv`
2. Configure `databricks.yml` with:
   - Environment: `dev`
   - Workspace URL: `https://dbc-2ff08614-a866.cloud.databricks.com`
   - Catalog: `demo_dev`
   - SQL Warehouse ID: `85fcef036597adbd`
3. Set up serverless compute configuration (no cluster specs)
4. Configure bundle to exclude files per `.gitignore`

**Example Configuration**:
```yaml
bundle:
  name: demo

targets:
  dev:
    mode: development
    workspace:
      host: https://dbc-2ff08614-a866.cloud.databricks.com
    default_catalog: demo_dev
    resources:
      jobs:
        sql_warehouse_id: 85fcef036597adbd
```

**Deliverables**:
- Configured `databricks.yml`
- Environment-specific settings for `dev`

---

### Step 3: Create Schema and Table Creation Notebooks

**Objective**: Create Python notebooks for schema and table creation per `specs/schema_specs`.

**Actions**:

#### 3.1 Create Bronze Schema Notebook
- **Location**: `notebooks/bronze/create_bronze_schema.py`
- **Content**:
  - Install required libraries via pip (most recent versions)
  - Create bronze schema (if not exists)
  - Create all bronze tables per `specs/schema_specs/bronze_tables.csv`
  - Ensure column datatypes match specs exactly
  - Support schema changes (add columns)
  - **DO NOT** include catalog creation (catalog must exist)

#### 3.2 Create Silver Schema Notebook
- **Location**: `notebooks/silver/create_silver_schema.py`
- **Content**:
  - Create silver schema (if not exists)
  - Create all silver tables per `specs/schema_specs/silver_tables.csv`
  - Include SCD2 setup for `dim_customer`
  - Ensure explicit schema casting for `dim_time`
  - Support schema changes (add columns)

#### 3.3 Create Gold Schema Notebook
- **Location**: `notebooks/gold/create_gold_schema.py`
- **Content**:
  - Create gold schema (if not exists)
  - Create all gold tables per `specs/schema_specs/gold_tables.csv`
  - Safe cast columns to match types and lengths
  - Support schema changes (add columns)

**Key Requirements**:
- Use relative path references to `src/` modules
- Example path setup:
  ```python
  import os, sys
  cwd = os.getcwd()
  p = os.path.join(cwd, '..', 'src')
  if os.path.isdir(p) and p not in sys.path:
      sys.path.insert(0, p)
  ```

**Deliverables**:
- `notebooks/bronze/create_bronze_schema.py`
- `notebooks/silver/create_silver_schema.py`
- `notebooks/gold/create_gold_schema.py`

---

### Step 4: Create Transformation Notebooks and Job Resources

**Objective**: Create Python notebooks for data transformations and orchestration jobs per `specs/transformation_specs`.

**Actions**:

#### 4.1 Bronze to Silver Transformation Notebooks

**Notebook: `notebooks/silver/load_dim_customer.py`**
- Source: `bronze.customers_raw`
- Target: `silver.dim_customer`
- Transformations (per `specs/transformation_specs/transformations_bronze_to_silver.csv`):
  - Parse timestamps (created_at, updated_at)
  - Apply SCD2 logic using `APPLY CHANGES INTO`
  - Normalize country codes
- Install libraries via pip in notebook

**Notebook: `notebooks/silver/load_dim_product.py`**
- Source: `bronze.products_raw`
- Target: `silver.dim_product`
- Transformations:
  - Cast list_price to DECIMAL(12,2)
  - Cast is_active to BOOLEAN

**Notebook: `notebooks/silver/load_fact_order.py`**
- Source: `bronze.orders_raw`, `bronze.payments_raw`
- Target: `silver.fact_order`
- Transformations:
  - Cast total_amount to DECIMAL(12,2)
  - Parse order_timestamp to UTC
  - Uppercase currency_code
  - Normalize status (CASE statement)
  - Deduplicate by order_id (ROW_NUMBER)
  - LEFT JOIN with payments to enrich payment_status

**Notebook: `notebooks/silver/load_fact_order_item.py`**
- Source: `bronze.order_items_raw`
- Target: `silver.fact_order_item`
- Transformations:
  - Cast quantity to INT
  - Cast prices to DECIMAL(12,2)
  - Derive extended_amount (quantity * unit_price)

**Notebook: `notebooks/silver/load_fact_web_event.py`**
- Source: `bronze.web_events_raw`
- Target: `silver.fact_web_event`
- Transformations:
  - Parse event_timestamp to UTC
  - Normalize event_type (CASE statement)

**Notebook: `notebooks/silver/load_dim_time.py`**
- Generate time dimension with explicit schema casting

#### 4.2 Silver to Gold Transformation Notebooks

**Notebook: `notebooks/gold/load_sales_dashboard_metrics.py`**
- Source: `silver.fact_order`
- Target: `gold.sales_dashboard_metrics`
- Transformations (per `specs/transformation_specs/transformations_silver_to_gold.csv`):
  - Aggregate daily: SUM(total_amount), COUNT(*), AVG(total_amount/order_count)
  - Split by channel: orders_web, orders_store

**Notebook: `notebooks/gold/load_product_sales_by_month.py`**
- Source: `silver.fact_order_item`
- Target: `gold.product_sales_by_month`
- Transformations:
  - Group by product_id, month(order_date)
  - SUM(quantity), SUM(extended_amount)

**Notebook: `notebooks/gold/load_customer_lifetime_value_features.py`**
- Source: `silver.dim_customer`, `silver.fact_order`
- Target: `gold.customer_lifetime_value_features`
- Transformations:
  - Window functions per customer_id
  - SUM(total_amount), COUNT(order_id)
  - DATEDIFF for recency

**Notebook: `notebooks/gold/load_cohort_retention.py`**
- Source: `silver.fact_order`
- Target: `gold.cohort_retention`
- Transformations:
  - MIN(order_date) as cohort_month
  - Calculate retention over month_offset
  - Join with dim_time

#### 4.3 Create Job Resources
- **Location**: `resources/jobs.yml`
- Define job tasks to orchestrate notebooks in correct order:
  1. Schema creation (bronze → silver → gold)
  2. Bronze data loading
  3. Silver transformations
  4. Gold aggregations
- Use serverless compute (no cluster configuration)

**Deliverables**:
- All transformation notebooks (bronze → silver → gold)
- `resources/jobs.yml` with job definitions

---

### Step 5: Include Data Quality Controls

**Objective**: Integrate `databricks-labs-dqx` (v0.9.3) for data quality checks per `specs/transformation_specs/data_quality_specs`.

**Actions**:

#### 5.1 Install DQX Library
- Add `pip install databricks-labs-dqx==0.9.3` to transformation notebooks

#### 5.2 Implement DQ Checks
- Reference `specs/transformation_specs/data_quality_specs/data_quality_controls.csv`
- Reference `specs/transformation_specs/data_quality_specs/how_use_dqx.md`
- Use DQEngine with metadata-based checks:
  ```python
  from databricks.labs.dqx.engine import DQEngine
  from databricks.sdk import WorkspaceClient
  
  dq_engine = DQEngine(WorkspaceClient())
  checks = [...]  # Load from metadata
  valid_df, invalid_df = dq_engine.apply_checks_by_metadata_and_split(input_df, checks)
  ```

#### 5.3 Create Common DQ Utility Module
- **Location**: `src/data_quality/dq_utils.py`
- Provide reusable functions to:
  - Load DQ checks from metadata
  - Apply checks using DQEngine
  - Save valid/quarantine records

#### 5.4 Update Transformation Notebooks
- Integrate DQ checks into silver transformation notebooks
- Handle error criticality (fail job) vs. warn criticality (log)
- Save quarantine records to separate tables

**Deliverables**:
- `src/data_quality/dq_utils.py`
- Updated transformation notebooks with DQ integration

---

### Step 6: Build AI BI Dashboard Resources

**Objective**: Generate Databricks AI BI Dashboard JSON files per `specs/dashboard_specs` and deploy as resources.

**Actions**:

#### 6.1 Install Databricks SDK
- Install `databricks-sdk>=0.57.0`

#### 6.2 Create Dashboard Generation Script
- **Location**: `src/dashboards/dashboard_generator.py`
- Use Databricks SDK to programmatically generate dashboard JSON
- Reference `specs/dashboard_specs/sample_dashboard.json` as template
- Follow specs from:
  - `dashboard_catalog.csv`
  - `dashboard_to_table_mapping.csv`
  - `dimensions_catalog.csv`
  - `measures_catalog.csv`
  - `visuals_spec.csv`

#### 6.3 Generate Dashboard JSONs
For each dashboard in `dashboard_catalog.csv`:
1. **Sales Overview**
   - Generate SQL queries with datasets/queryLines
   - Create widgets: Counter (Total Sales), Line (AOV Trend, Orders by Channel)
   - Add filters: Date Range Picker, Multi-Select (channel)

2. **Product Performance**
   - Horizontal Bar (Top Products by Revenue)
   - Heatmap (Returns Rate)
   - Filter: Single-Select (category)

3. **Customer 360 & AI Features**
   - Scatter (Churn Risk Score)
   - Histogram (Recency Days)
   - Filter: Multi-Select (country_code)

4. **Cohort Retention**
   - Heatmap (Cohort Matrix)

5. **Executive Summary**
   - Line (12-Mo Sales Trend, Channel Mini Charts)

#### 6.4 Validate SQL Queries
- Use SQL Statement Execution REST API to validate queries
- Ensure queries run successfully before deployment

#### 6.5 Configure Dashboard Resources
- **Location**: `resources/dashboards.yml`
- Reference generated JSON files
- Configure deployment parameters

**Key Requirements**:
- QueryLines must have newline char at end of each line
- All widgets configured correctly per visuals_spec.csv
- Validate all SQL queries

**Deliverables**:
- `src/dashboards/dashboard_generator.py`
- Generated dashboard JSON files (one per dashboard)
- `resources/dashboards.yml`

---

### Step 7: Create Unit Tests for Common Utils

**Objective**: Write proper unit tests for common utilities.

**Actions**:

#### 7.1 Create Test Files
- **Location**: `tests/`
- Test modules:
  - `tests/test_common_utils.py` - Test common utilities
  - `tests/test_transformations.py` - Test transformation logic
  - `tests/test_data_quality.py` - Test DQ utilities

#### 7.2 Test Scenarios
- Data type casting functions
- Timestamp parsing logic
- Normalization functions (currency, country codes)
- DQ check loading and application
- SCD2 logic

#### 7.3 Use Testing Framework
- Use `pytest` for test framework
- Mock Databricks SDK where necessary
- Create sample test data

**Deliverables**:
- Unit test files in `tests/` directory
- Test coverage for all common utilities

---

### Step 8: Run Unit Tests on Local Spark

**Objective**: Execute unit tests locally to verify correctness before deployment.

**Actions**:

#### 8.1 Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On macOS/Linux
# venv\Scripts\activate  # On Windows
```

#### 8.2 Install Dependencies
```bash
pip install pyspark pytest
pip install databricks-labs-dqx==0.9.3
pip install databricks-sdk>=0.57.0
# Install other required libraries
```

#### 8.3 Run Unit Tests
```bash
pytest tests/ -v
```

#### 8.4 Fix Issues
- Address any failing tests
- Ensure all tests pass before proceeding

**Deliverables**:
- All unit tests passing locally

---

### Step 9: Generate and Load Sample Data into Bronze Tables

**Objective**: Create scripts to generate sample data and load into bronze tables.

**Actions**:

#### 9.1 Create Data Generation Script
- **Location**: `notebooks/data_generation/generate_sample_data.py`
- Generate synthetic data for:
  - orders_raw
  - order_items_raw
  - customers_raw
  - products_raw
  - web_events_raw
  - payments_raw
  - inventory_raw
- Include metadata columns (_ingest_time, _file_name, _source)
- Generate realistic data distributions

#### 9.2 Create Data Loading Job
- Add job definition in `resources/jobs.yml`
- Load generated data into bronze tables
- Use serverless compute

**Deliverables**:
- `notebooks/data_generation/generate_sample_data.py`
- Job definition for data loading

---

### Step 10: Verify Column Data Type Mappings

**Objective**: Ensure transformation code properly maps target table column data types.

**Actions**:

#### 10.1 Review All Transformation Notebooks
- Check bronze → silver transformations
- Check silver → gold transformations

#### 10.2 Validate Data Type Mappings
- Compare transformation code against:
  - `specs/schema_specs/bronze_tables.csv`
  - `specs/schema_specs/silver_tables.csv`
  - `specs/schema_specs/gold_tables.csv`

#### 10.3 Verify Safe Casting
- Ensure DECIMAL precision/scale matches specs
- Verify TIMESTAMP parsing
- Check BOOLEAN casting
- Validate INT casting

#### 10.4 Test with Sample Data
- Run transformations on sample data
- Verify output schema matches target table schema

**Deliverables**:
- Verified data type mappings
- Corrected any mismatches

---

### Step 11: Deploy Project to Dev Workspace

**Objective**: Deploy code, jobs, and dashboards to Databricks dev workspace using Asset Bundle.

**Actions**:

#### 11.1 Configure Exclusions
- Update `databricks.yml` to exclude files per `.gitignore`
- Typical exclusions:
  - `venv/`
  - `__pycache__/`
  - `*.pyc`
  - `.git/`
  - Test files (optional)

#### 11.2 Validate Bundle
```bash
databricks bundle validate -t dev
```

#### 11.3 Deploy Bundle
```bash
databricks bundle deploy -t dev
```

#### 11.4 Verify Deployment
- Check workspace for deployed notebooks
- Verify jobs are created
- Confirm dashboard resources are deployed

**Deliverables**:
- Successfully deployed bundle to dev workspace
- All resources visible in workspace

---

### Step 12: Run Deployed Jobs in Correct Order

**Objective**: Execute jobs in the correct sequence to build the lakehouse.

**Actions**:

#### 12.1 Define Job Execution Order
1. **Schema Creation Jobs**:
   - Create bronze schema and tables
   - Create silver schema and tables
   - Create gold schema and tables

2. **Data Loading Jobs**:
   - Generate and load sample data into bronze tables

3. **Bronze → Silver Transformation Jobs**:
   - Load dim_customer
   - Load dim_product
   - Load dim_time
   - Load fact_order
   - Load fact_order_item
   - Load fact_web_event

4. **Silver → Gold Transformation Jobs**:
   - Load sales_dashboard_metrics
   - Load product_sales_by_month
   - Load customer_lifetime_value_features
   - Load cohort_retention

#### 12.2 Execute Jobs
- Use Databricks UI or CLI to trigger jobs
- Monitor job execution
- Review logs for any errors

#### 12.3 Validate Results
- Query gold tables to verify data
- Check DQ quarantine tables for rejected records
- Validate dashboard data sources

**Deliverables**:
- All jobs executed successfully
- Data populated in bronze, silver, and gold layers
- Dashboards functional with live data

---

## 9. Technical Requirements

### 9.1 Code Quality Standards

#### Linting and Formatting
- Use `pylint` or `flake8` for linting
- Use `black` for code formatting
- Ensure code passes linting checks before deployment

#### Code Review
- Review and refactor code as needed
- Ensure modular design with reusable utilities
- Follow DRY (Don't Repeat Yourself) principle

### 9.2 Modular Code Structure

#### Common Utilities
- **Location**: `src/common/`
- Utilities for:
  - Database connections
  - Configuration management
  - Logging
  - Error handling

#### Transformation Utilities
- **Location**: `src/transformations/`
- Reusable transformation functions:
  - Type casting
  - Timestamp parsing
  - Normalization logic
  - Aggregation helpers

#### Data Quality Utilities
- **Location**: `src/data_quality/`
- DQ check management
- Quarantine handling
- Metadata loading

### 9.3 Notebook Path References

All notebooks must reference `src/` modules using relative paths:

```python
import os
import sys

# Add src to path
cwd = os.getcwd()
p = os.path.join(cwd, '..', 'src')
if os.path.isdir(p) and p not in sys.path:
    sys.path.insert(0, p)

# Now import modules
from common import db_utils
from transformations import cast_utils
from data_quality import dq_utils
```

### 9.4 Library Installation

**Important**: Do NOT install libraries on cluster. Use pip install in scripts/notebooks.

Example:
```python
# At top of notebook
!pip install databricks-labs-dqx==0.9.3
!pip install databricks-sdk>=0.57.0
```

### 9.5 Serverless Compute Configuration

- **All jobs run on serverless compute**
- NO cluster configuration in jobs YAML
- Omit cluster specifications
- Jobs run without retries (serverless default)

Example job configuration:
```yaml
resources:
  jobs:
    load_silver_job:
      name: load_silver_job
      tasks:
        - task_key: load_dim_customer
          notebook_task:
            notebook_path: notebooks/silver/load_dim_customer.py
          # NO cluster configuration
```

### 9.6 Important Reminders

1. **DO NOT CREATE** markdown files (README, guide, summary, report, checklist) unless explicitly asked
2. **Always** read instructions again when facing errors
3. **Ensure** all steps are in TODO list before starting
4. **Test locally** before deploying to avoid runtime issues
5. **Double-check** column data types match specs
6. **Safe cast** all columns to avoid data type mismatch errors

---

## Summary

This design document provides a comprehensive blueprint for building an end-to-end BI use case on Databricks Lakehouse. It includes:

- Complete database schema (Bronze, Silver, Gold layers)
- Detailed transformation logic
- Data quality controls with DQX integration
- Dashboard specifications with metrics and dimensions
- Step-by-step implementation guide (12 steps)
- Technical requirements and best practices

By following this design document systematically, you will create a production-ready BI solution with:
- Robust data quality checks
- SCD2 dimension management
- AI-powered dashboards
- Modular, testable code
- Serverless deployment on Databricks

---

**Next Step**: Use this design document to create the project following the 12-step implementation guide.

