# DESIGN_DOCUMENT.md
## Retail Data Lakehouse - Technical Design Document

---

## Table of Contents
1. [Executive Summary](#1-executive-summary)
2. [Source Analysis](#2-source-analysis)
3. [Bronze Layer Design](#3-bronze-layer-design)
4. [Silver Layer Design](#4-silver-layer-design)
5. [Gold Layer Design](#5-gold-layer-design)
6. [Security & Access Control](#6-security--access-control)
7. [Operational Considerations](#7-operational-considerations)
8. [Risk Register](#8-risk-register)

---

## 1. Executive Summary

### 1.1 Project Overview
This document provides the complete technical design for the **Retail Data Lakehouse** implementation on **AWS**. The solution implements a medallion architecture (Bronze → Silver → Gold) using Delta Lake to deliver a scalable, governed, and performant data platform for retail analytics.

### 1.2 Architecture Summary
| Layer | Purpose | Storage Location | Key Characteristics |
|-------|---------|------------------|---------------------|
| Bronze | Raw data ingestion | `s3://retail-lakehouse-bronze/` | Exact source copy, metadata enrichment |
| Silver | Cleansed & validated | `s3://retail-lakehouse-silver/` | SCD Type 2, data quality enforcement |
| Gold | Business consumption | `s3://retail-lakehouse-gold/` | Aggregated, consumer-ready views |

### 1.3 Scope
This design covers the following source entities:
- **orders** - Core transactional fact table

### 1.4 Key Design Decisions
| Decision | Rationale |
|----------|-----------|
| SCD Type 2 for orders | Correctable mutability requires full history tracking |
| Date-based partitioning | Optimizes time-series queries common in retail analytics |
| Consumer views in Gold | Provides schema abstraction and security boundary |
| Delta Lake format | Enables ACID transactions, time travel, and CDC |

### 1.5 Document Status
| Version | Date | Author | Status |
|---------|------|--------|--------|
| 1.0 | Current | Data Engineering Team | Draft |

---

## 2. Source Analysis

### 2.1 Source Inventory

| Source Table | Classification | Mutability | Change Pattern | Ingestion Strategy |
|--------------|----------------|------------|----------------|-------------------|
| orders | Fact | Correctable | Type 2 (historize) | full_pull |

### 2.2 Detailed Source Analysis

#### 2.2.1 orders

**Classification:** Fact Table

**Mutability:** Correctable
- Records may be modified after initial creation
- Corrections must be tracked historically

**Change Pattern:** Type 2 (Historize)
- All changes create new record versions
- Historical state preserved with validity timestamps

**Ingestion Strategy:** Full Pull
- Complete table extraction on each load
- Comparison against existing data for change detection

**Source Schema:**
| Column | Data Type | Description |
|--------|-----------|-------------|
| id | INT | Primary key identifier |
| total | DECIMAL(38,10) | Order total amount |

**Design Considerations:**
1. **Partitioning Strategy**: Partition by order date for optimal query performance on time-based analysis
2. **Foreign Key Relationships**: Maintain proper key relationships with dimensional tables (Customer, Product, Store, Date)
3. **Incremental Loading**: Implement merge patterns for efficient data ingestion
4. **Grain Definition**: Clarify if each row represents one order or order line items
5. **Additional Measures**: Consider enriching with quantity, discount, tax if available
6. **Timestamp Columns**: Add audit columns (created_at, updated_at) for lineage tracking
7. **Data Quality**: Implement validation rules for total field (non-negative, reasonable ranges)

---

## 3. Bronze Layer Design

### 3.1 Design Principles
- **Exact Source Mirror**: No transformations applied to source data
- **Metadata Enrichment**: Add ingestion tracking columns
- **Immutable History**: Preserve all ingested data
- **Change Data Feed**: Enable CDC for downstream processing

### 3.2 Storage Configuration

| Attribute | Value |
|-----------|-------|
| Storage Location | `s3://retail-lakehouse-bronze/dw_bronze/` |
| File Format | Delta Lake |
| Partition Strategy | `DATE(_ingested_at)` |

### 3.3 Table Definitions

#### 3.3.1 bronze.orders

```sql
CREATE TABLE IF NOT EXISTS bronze.orders (
    -- Source columns (exact mirror, no transformation)
    id INT,
    total DECIMAL(38, 10),
    
    -- Bronze metadata columns
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _source_file STRING NOT NULL,
    _schema_version STRING NOT NULL DEFAULT '1.0',
    _ingestion_batch_id STRING NOT NULL,
    _source_system STRING NOT NULL DEFAULT 'source_orders',
    _row_hash STRING
)
USING DELTA
PARTITIONED BY (DATE(_ingested_at))
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze',
    'data_classification' = 'fact',
    'mutability_type' = 'correctable',
    'description' = 'Bronze layer orders table - raw ingestion with metadata'
);
```

**Storage Path:** `s3://retail-lakehouse-bronze/dw_bronze/orders/`

### 3.4 Ingestion Configuration

| Parameter | Value |
|-----------|-------|
| Ingestion Mode | incremental_by_timestamp |
| Load Frequency | As defined by orchestration |
| Batch Size | Configurable |

### 3.5 Bronze Validations

| Validation | Description |
|------------|-------------|
| distinct_key_count | Verify unique key counts match expectations |
| time_range | Validate ingestion timestamps within expected range |
| file_count | Confirm expected number of source files processed |
| avg_file_size | Monitor file sizes for anomaly detection |

---

## 4. Silver Layer Design

### 4.1 Design Principles
- **Data Cleansing**: Standardize formats, handle nulls, trim whitespace
- **Type Enforcement**: Cast to appropriate data types with validation
- **Normalization**: Standardize categorical values
- **SCD Type 2**: Maintain full history with validity tracking
- **Change Detection**: Hash-based comparison for efficient processing

### 4.2 Storage Configuration

| Attribute | Value |
|-----------|-------|
| Storage Location | `s3://retail-lakehouse-silver/dw_silver/` |
| File Format | Delta Lake |
| Partition Strategy | `order_date` |
| Clustering | `order_id`, `customer_id`, `_is_current` |

### 4.3 Table Definitions

#### 4.3.1 silver.orders

```sql
CREATE OR REPLACE TABLE dw_silver.orders (
    -- Surrogate Key (Silver layer identity)
    order_sk BIGINT GENERATED ALWAYS AS IDENTITY 
        COMMENT 'Surrogate key - unique identifier for each version of an order record',
    
    -- Natural/Business Key
    order_id STRING NOT NULL 
        COMMENT 'Business key from source system - identifies the order',
    
    -- Order Core Attributes
    customer_id STRING 
        COMMENT 'Customer identifier - join key to customer dimension',
    order_date DATE NOT NULL 
        COMMENT 'Date the order was placed - used for partitioning',
    order_timestamp TIMESTAMP 
        COMMENT 'Exact timestamp when order was created',
    order_status STRING 
        COMMENT 'Original order status from source (cleansed)',
    order_status_normalized STRING NOT NULL 
        COMMENT 'Standardized status: PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED, RETURNED, REFUNDED, UNKNOWN',
    
    -- Financial Attributes
    subtotal_amount DECIMAL(18,2) NOT NULL DEFAULT 0 
        COMMENT 'Order subtotal before tax, shipping, discounts',
    tax_amount DECIMAL(18,2) NOT NULL DEFAULT 0 
        COMMENT 'Tax amount applied to order',
    shipping_amount DECIMAL(18,2) NOT NULL DEFAULT 0 
        COMMENT 'Shipping charges',
    discount_amount DECIMAL(18,2) NOT NULL DEFAULT 0 
        COMMENT 'Total discounts applied',
    total_amount DECIMAL(18,2) NOT NULL DEFAULT 0 
        COMMENT 'Final order total',
    currency_code STRING NOT NULL DEFAULT 'USD' 
        COMMENT 'ISO currency code',
    
    -- Shipping Information (Flattened Low-Cardinality)
    shipping_method STRING 
        COMMENT 'Original shipping method from source',
    shipping_method_category STRING NOT NULL DEFAULT 'OTHER' 
        COMMENT 'Normalized: STANDARD, EXPRESS, OVERNIGHT, PICKUP, OTHER',
    
    -- Payment Information (Flattened Low-Cardinality)
    payment_method STRING 
        COMMENT 'Original payment method from source',
    payment_method_category STRING NOT NULL DEFAULT 'OTHER' 
        COMMENT 'Normalized: CARD, DIGITAL, BANK, CASH, OTHER',
    
    -- Geographic Attributes (Flattened Low-Cardinality)
    shipping_country STRING 
        COMMENT 'Shipping destination country code',
    shipping_region STRING 
        COMMENT 'Shipping destination region/state',
    billing_country STRING 
        COMMENT 'Billing address country code',
    billing_region STRING 
        COMMENT 'Billing address region/state',
    
    -- SCD Type 2 CDC Tracking Columns
    _valid_from TIMESTAMP NOT NULL 
        COMMENT 'Record version start timestamp - when this version became effective',
    _valid_to TIMESTAMP 
        COMMENT 'Record version end timestamp - NULL for current records',
    _is_current BOOLEAN NOT NULL DEFAULT TRUE 
        COMMENT 'Flag indicating if this is the current version of the record',
    
    -- Data Lineage & Audit Columns
    _source_system STRING NOT NULL DEFAULT 'UNKNOWN' 
        COMMENT 'Source system identifier for lineage tracking',
    _bronze_load_timestamp TIMESTAMP 
        COMMENT 'Timestamp when record was loaded to Bronze layer',
    _silver_load_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() 
        COMMENT 'Timestamp when record was loaded/updated in Silver layer',
    _record_hash STRING NOT NULL 
        COMMENT 'MD5 hash of business columns for change detection',
    
    -- Constraints
    CONSTRAINT orders_pk PRIMARY KEY (order_sk),
    CONSTRAINT orders_total_check CHECK (total_amount >= 0),
    CONSTRAINT orders_status_check CHECK (order_status_normalized IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED', 'REFUNDED', 'UNKNOWN')),
    CONSTRAINT orders_shipping_category_check CHECK (shipping_method_category IN ('STANDARD', 'EXPRESS', 'OVERNIGHT', 'PICKUP', 'OTHER')),
    CONSTRAINT orders_payment_category_check CHECK (payment_method_category IN ('CARD', 'DIGITAL', 'BANK', 'CASH', 'OTHER'))
)
USING DELTA
PARTITIONED BY (p_order_date DATE GENERATED ALWAYS AS (order_date))
CLUSTER BY (order_id, customer_id, _is_current)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration' = 'interval 60 days',
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported',
    'comment' = 'Silver layer orders table with SCD Type 2 history tracking. Contains cleansed, validated, and normalized order data.'
);
```

### 4.4 Transformation Logic

#### 4.4.1 Bronze to Silver Transformation

```sql
-- Bronze to Silver Transformation: dw_bronze.orders → dw_silver.orders
-- Implements SCD Type 2 with CDC tracking

MERGE INTO dw_silver.orders AS target
USING (
    SELECT 
        -- Natural Key
        TRIM(order_id) AS order_id,
        
        -- Order Attributes with cleansing
        TRIM(customer_id) AS customer_id,
        CAST(order_date AS DATE) AS order_date,
        CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
        UPPER(TRIM(order_status)) AS order_status,
        
        -- Normalize order status
        CASE UPPER(TRIM(order_status))
            WHEN 'PENDING' THEN 'PENDING'
            WHEN 'PEND' THEN 'PENDING'
            WHEN 'PROCESSING' THEN 'PROCESSING'
            WHEN 'PROC' THEN 'PROCESSING'
            WHEN 'SHIPPED' THEN 'SHIPPED'
            WHEN 'SHIP' THEN 'SHIPPED'
            WHEN 'DELIVERED' THEN 'DELIVERED'
            WHEN 'DELIV' THEN 'DELIVERED'
            WHEN 'CANCELLED' THEN 'CANCELLED'
            WHEN 'CANCELED' THEN 'CANCELLED'
            WHEN 'CANCEL' THEN 'CANCELLED'
            WHEN 'RETURNED' THEN 'RETURNED'
            WHEN 'REFUNDED' THEN 'REFUNDED'
            ELSE 'UNKNOWN'
        END AS order_status_normalized,
        
        -- Financial with type conversion and null handling
        CAST(COALESCE(subtotal_amount, 0) AS DECIMAL(18,2)) AS subtotal_amount,
        CAST(COALESCE(tax_amount, 0) AS DECIMAL(18,2)) AS tax_amount,
        CAST(COALESCE(shipping_amount, 0) AS DECIMAL(18,2)) AS shipping_amount,
        CAST(COALESCE(discount_amount, 0) AS DECIMAL(18,2)) AS discount_amount,
        CAST(COALESCE(total_amount, 0) AS DECIMAL(18,2)) AS total_amount,
        UPPER(COALESCE(TRIM(currency_code), 'USD')) AS currency_code,
        
        -- Shipping method normalization
        UPPER(TRIM(shipping_method)) AS shipping_method,
        CASE 
            WHEN UPPER(TRIM(shipping_method)) IN ('STANDARD', 'GROUND', 'ECONOMY') THEN 'STANDARD'
            WHEN UPPER(TRIM(shipping_method)) IN ('EXPRESS', '2DAY', 'TWO_DAY') THEN 'EXPRESS'
            WHEN UPPER(TRIM(shipping_method)) IN ('OVERNIGHT', 'NEXT_DAY', 'PRIORITY') THEN 'OVERNIGHT'
            WHEN UPPER(TRIM(shipping_method)) IN ('PICKUP', 'STORE_PICKUP', 'CURBSIDE') THEN 'PICKUP'
            ELSE 'OTHER'
        END AS shipping_method_category,
        
        -- Payment method normalization
        UPPER(TRIM(payment_method)) AS payment_method,
        CASE 
            WHEN UPPER(TRIM(payment_method)) IN ('CREDIT', 'DEBIT', 'CREDIT_CARD', 'DEBIT_CARD', 'VISA', 'MASTERCARD', 'AMEX') THEN 'CARD'
            WHEN UPPER(TRIM(payment_method)) IN ('PAYPAL', 'VENMO', 'APPLE_PAY', 'GOOGLE_PAY', 'DIGITAL_WALLET') THEN 'DIGITAL'
            WHEN UPPER(TRIM(payment_method)) IN ('BANK_TRANSFER', 'ACH', 'WIRE') THEN 'BANK'
            WHEN UPPER(TRIM(payment_method)) IN ('COD', 'CASH', 'CASH_ON_DELIVERY') THEN 'CASH'
            ELSE 'OTHER'
        END AS payment_method_category,
        
        -- Geographic (flattened)
        UPPER(TRIM(shipping_country)) AS shipping_country,
        UPPER(TRIM(shipping_region)) AS shipping_region,
        UPPER(TRIM(billing_country)) AS billing_country,
        UPPER(TRIM(billing_region)) AS billing_region,
        
        -- Audit columns
        COALESCE(_source_system, 'UNKNOWN') AS _source_system,
        _load_timestamp AS _bronze_load_timestamp,
        CURRENT_TIMESTAMP() AS _silver_load_timestamp,
        
        -- Record hash for change detection
        MD5(CONCAT_WS('|',
            COALESCE(TRIM(order_id), ''),
            COALESCE(TRIM(customer_id), ''),
            COALESCE(CAST(order_date AS STRING), ''),
            COALESCE(UPPER(TRIM(order_status)), ''),
            COALESCE(CAST(total_amount AS STRING), ''),
            COALESCE(UPPER(TRIM(shipping_method)), ''),
            COALESCE(UPPER(TRIM(payment_method)), '')
        )) AS _record_hash
        
    FROM dw_bronze.orders
    WHERE order_id IS NOT NULL
      AND _is_valid = TRUE
) AS source
ON target.order_id = source.order_id 
   AND target._is_current = TRUE

-- When record exists and has changed, expire the old record
WHEN MATCHED AND target._record_hash != source._record_hash THEN
    UPDATE SET
        _valid_to = CURRENT_TIMESTAMP(),
        _is_current = FALSE

-- When no current record exists, insert new
WHEN NOT MATCHED THEN
    INSERT (
        order_id, customer_id, order_date, order_timestamp, order_status, order_status_normalized,
        subtotal_amount, tax_amount, shipping_amount, discount_amount, total_amount, currency_code,
        shipping_method, shipping_method_category, payment_method, payment_method_category,
        shipping_country, shipping_region, billing_country, billing_region,
        _valid_from, _valid_to, _is_current,
        _source_system, _bronze_load_timestamp, _silver_load_timestamp, _record_hash
    )
    VALUES (
        source.order_id, source.customer_id, source.order_date, source.order_timestamp, 
        source.order_status, source.order_status_normalized,
        source.subtotal_amount, source.tax_amount, source.shipping_amount, source.discount_amount, 
        source.total_amount, source.currency_code,
        source.shipping_method, source.shipping_method_category, source.payment_method, source.payment_method_category,
        source.shipping_country, source.shipping_region, source.billing_country, source.billing_region,
        CURRENT_TIMESTAMP(), NULL, TRUE,
        source._source_system, source._bronze_load_timestamp, source._silver_load_timestamp, source._record_hash
    );
```

### 4.5 Flattening Decisions

#### 4.5.1 Flattened into Orders (Low Cardinality)
| Attribute | Normalized Column | Categories |
|-----------|-------------------|------------|
| order_status | order_status_normalized | PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED, RETURNED, REFUNDED, UNKNOWN |
| shipping_method | shipping_method_category | STANDARD, EXPRESS, OVERNIGHT, PICKUP, OTHER |
| payment_method | payment_method_category | CARD, DIGITAL, BANK, CASH, OTHER |
| shipping_country | shipping_country | Country codes |
| shipping_region | shipping_region | Region/state codes |
| billing_country | billing_country | Country codes |
| billing_region | billing_region | Region/state codes |
| currency_code | currency_code | ISO currency codes |

#### 4.5.2 Kept Separate (High Cardinality)
| Entity | Reason |
|--------|--------|
| customer | High cardinality - referenced via customer_id foreign key |
| product/item | High cardinality - should be in separate order_items table |
| address | High cardinality - full address details in separate dim_address table |
| date dimension | Referenced via order_date for calendar attributes |

### 4.6 CDC Columns

| Column | Type | Description |
|--------|------|-------------|
| _valid_from | TIMESTAMP NOT NULL | Start timestamp when record version became active |
| _valid_to | TIMESTAMP NULL | End timestamp when superseded (NULL for current) |
| _is_current | BOOLEAN NOT NULL | Flag indicating current active version |
| _record_hash | STRING | MD5 hash for efficient change detection |
| _bronze_load_timestamp | TIMESTAMP | Original Bronze layer load timestamp |
| _silver_load_timestamp | TIMESTAMP | Silver layer processing timestamp |

### 4.7 Partitioning Strategy

**Partition Column:** `p_order_date` (generated from `order_date`)

**Rationale:**
1. **Time-Based Query Alignment**: Orders predominantly queried by date ranges
2. **Data Volume Management**: Daily partitions create manageable sizes
3. **Data Lifecycle Support**: Facilitates retention policies and incremental processing
4. **SCD Type 2 Consideration**: Historical versions remain co-located with original order date
5. **Partition Size Optimization**: Daily partitions target optimal 256MB-1GB file sizes

### 4.8 Z-Order Configuration

**Z-Order Columns:** `order_id`, `customer_id`, `_is_current`

| Column | Rationale |
|--------|-----------|
| order_id | Natural key for point lookups, high cardinality, critical for MERGE operations |
| customer_id | Primary join key to customer dimension, supports customer analytics |
| _is_current | 95%+ queries filter on current records, improves read performance |

```sql
OPTIMIZE dw_silver.orders
ZORDER BY (order_id, customer_id, _is_current);
```

### 4.9 Optimization Schedule

#### 4.9.1 Daily Optimization (02:00 UTC)
```sql
-- Optimize recent partitions
OPTIMIZE dw_silver.orders
WHERE p_order_date >= DATE_SUB(CURRENT_DATE(), 7)
ZORDER BY (order_id, customer_id, _is_current);

-- Update statistics
ANALYZE TABLE dw_silver.orders COMPUTE STATISTICS FOR ALL COLUMNS;
```

#### 4.9.2 Weekly Optimization (Sunday 03:00 UTC)
```sql
-- Optimize older partitions
OPTIMIZE dw_silver.orders
WHERE p_order_date >= DATE_SUB(CURRENT_DATE(), 90)
  AND p_order_date < DATE_SUB(CURRENT_DATE(), 7)
ZORDER BY (order_id, customer_id, _is_current);

-- Vacuum old files
VACUUM dw_silver.orders RETAIN 168 HOURS;

-- Refresh statistics
ANALYZE TABLE dw_silver.orders COMPUTE STATISTICS FOR ALL COLUMNS;
```

#### 4.9.3 Monthly Optimization (First Sunday 04:00 UTC)
```sql
-- Full table optimization
OPTIMIZE dw_silver.orders
ZORDER BY (order_id, customer_id, _is_current);

-- Extended vacuum
VACUUM dw_silver.orders RETAIN 168 HOURS;

-- Recompute all statistics
ANALYZE TABLE dw_silver.orders COMPUTE STATISTICS FOR ALL COLUMNS;

-- Repair table
MSCK REPAIR TABLE dw_silver.orders;
```

---

## 5. Gold Layer Design

### 5.1 Design Principles
- **Business-Ready**: Optimized for consumption by analysts and applications
- **Current State Table**: Latest version of each record for operational queries
- **History Table**: Full SCD Type 2 history for temporal analysis
- **Consumer Views**: Abstraction layer for schema evolution and security
- **Data Governance**: Validation rules and quality enforcement

### 5.2 Storage Configuration

| Attribute | Value |
|-----------|-------|
| ETL Storage | `s3://retail-lakehouse-gold/gold_etl/` |
| Consumer Views | `s3://retail-lakehouse-gold/gold/` |
| History Storage | `s3://retail-lakehouse-gold/gold_hist/` |
| File Format | Delta Lake |

### 5.3 Table Definitions

#### 5.3.1 Current State Table (dw_gold.orders_t)

```sql
CREATE TABLE IF NOT EXISTS dw_gold.orders_t (
    -- Primary Key
    order_id STRING NOT NULL COMMENT 'Unique order identifier',
    
    -- Order Details
    order_number STRING COMMENT 'Business order number',
    order_date DATE COMMENT 'Date order was placed',
    order_timestamp TIMESTAMP COMMENT 'Timestamp order was placed',
    order_status STRING COMMENT 'Current order status',
    order_type STRING COMMENT 'Type of order (online, in-store, phone, etc.)',
    
    -- Customer Information
    customer_id STRING COMMENT 'Customer identifier',
    customer_name STRING COMMENT 'Customer full name',
    customer_email STRING COMMENT 'Customer email address',
    
    -- Financial Details
    subtotal_amount DECIMAL(18,2) COMMENT 'Order subtotal before tax and discounts',
    discount_amount DECIMAL(18,2) COMMENT 'Total discount applied',
    tax_amount DECIMAL(18,2) COMMENT 'Total tax amount',
    shipping_amount DECIMAL(18,2) COMMENT 'Shipping cost',
    total_amount DECIMAL(18,2) COMMENT 'Final order total',
    currency_code STRING COMMENT 'Currency code (ISO 4217)',
    
    -- Shipping Information
    shipping_address_line1 STRING COMMENT 'Shipping address line 1',
    shipping_address_line2 STRING COMMENT 'Shipping address line 2',
    shipping_city STRING COMMENT 'Shipping city',
    shipping_state STRING COMMENT 'Shipping state/province',
    shipping_postal_code STRING COMMENT 'Shipping postal/zip code',
    shipping_country STRING COMMENT 'Shipping country',
    shipping_method STRING COMMENT 'Shipping method selected',
    
    -- Billing Information
    billing_address_line1 STRING COMMENT 'Billing address line 1',
    billing_address_line2 STRING COMMENT 'Billing address line 2',
    billing_city STRING COMMENT 'Billing city',
    billing_state STRING COMMENT 'Billing state/province',
    billing_postal_code STRING COMMENT 'Billing postal/zip code',
    billing_country STRING COMMENT 'Billing country',
    payment_method STRING COMMENT 'Payment method used',
    
    -- Fulfillment Details
    fulfillment_status STRING COMMENT 'Current fulfillment status',
    shipped_date DATE COMMENT 'Date order was shipped',
    delivered_date DATE COMMENT 'Date order was delivered',
    estimated_delivery_date DATE COMMENT 'Estimated delivery date',
    
    -- Channel and Source
    sales_channel STRING COMMENT 'Sales channel (web, mobile, store)',
    store_id STRING COMMENT 'Store identifier if applicable',
    campaign_id STRING COMMENT 'Marketing campaign identifier',
    
    -- Metadata
    dw_created_timestamp TIMESTAMP COMMENT 'Timestamp record was created in Gold',
    dw_updated_timestamp TIMESTAMP COMMENT 'Timestamp record was last updated in Gold',
    dw_source_system STRING COMMENT 'Source system identifier',
    dw_batch_id STRING COMMENT 'ETL batch identifier',
    
    -- Partition Column
    p_order_year_month STRING COMMENT 'Partition key: YYYY-MM format'
)
USING DELTA
PARTITIONED BY (p_order_year_month)
COMMENT 'Gold layer current-state orders table containing the latest state of each order'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold',
    'layer' = 'consumption',
    'table_type' = 'current',
    'source_table' = 'dw_silver.orders'
);
```

#### 5.3.2 History Table (dw_gold.orders_t_hist)

```sql
CREATE TABLE IF NOT EXISTS dw_gold.orders_t_hist (
    -- Surrogate Key
    order_hist_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for history record',
    
    -- Primary Key
    order_id STRING NOT NULL COMMENT 'Unique order identifier',
    
    -- Order Details
    order_number STRING COMMENT 'Business order number',
    order_date DATE COMMENT 'Date order was placed',
    order_timestamp TIMESTAMP COMMENT 'Timestamp order was placed',
    order_status STRING COMMENT 'Order status at this point in time',
    order_type STRING COMMENT 'Type of order (online, in-store, phone, etc.)',
    
    -- Customer Information
    customer_id STRING COMMENT 'Customer identifier',
    customer_name STRING COMMENT 'Customer full name',
    customer_email STRING COMMENT 'Customer email address',
    
    -- Financial Details
    subtotal_amount DECIMAL(18,2) COMMENT 'Order subtotal before tax and discounts',
    discount_amount DECIMAL(18,2) COMMENT 'Total discount applied',
    tax_amount DECIMAL(18,2) COMMENT 'Total tax amount',
    shipping_amount DECIMAL(18,2) COMMENT 'Shipping cost',
    total_amount DECIMAL(18,2) COMMENT 'Final order total',
    currency_code STRING COMMENT 'Currency code (ISO 4217)',
    
    -- Shipping Information
    shipping_address_line1 STRING COMMENT 'Shipping address line 1',
    shipping_address_line2 STRING COMMENT 'Shipping address line 2',
    shipping_city STRING COMMENT 'Shipping city',
    shipping_state STRING COMMENT 'Shipping state/province',
    shipping_postal_code STRING COMMENT 'Shipping postal/zip code',
    shipping_country STRING COMMENT 'Shipping country',
    shipping_method STRING COMMENT 'Shipping method selected',
    
    -- Billing Information
    billing_address_line1 STRING COMMENT 'Billing address line 1',
    billing_address_line2 STRING COMMENT 'Billing address line 2',
    billing_city STRING COMMENT 'Billing city',
    billing_state STRING COMMENT 'Billing state/province',
    billing_postal_code STRING COMMENT 'Billing postal/zip code',
    billing_country STRING COMMENT 'Billing country',
    payment_method STRING COMMENT 'Payment method used',
    
    -- Fulfillment Details
    fulfillment_status STRING COMMENT 'Fulfillment status at this point in time',
    shipped_date DATE COMMENT 'Date order was shipped',
    delivered_date DATE COMMENT 'Date order was delivered',
    estimated_delivery_date DATE COMMENT 'Estimated delivery date',
    
    -- Channel and Source
    sales_channel STRING COMMENT 'Sales channel (web, mobile, store)',
    store_id STRING COMMENT 'Store identifier if applicable',
    campaign_id STRING COMMENT 'Marketing campaign identifier',
    
    -- Type 2 SCD Fields
    dw_effective_start_timestamp TIMESTAMP NOT NULL COMMENT 'Timestamp when this version became effective',
    dw_effective_end_timestamp TIMESTAMP COMMENT 'Timestamp when this version was superseded (NULL if current)',
    dw_is_current BOOLEAN COMMENT 'Flag indicating if this is the current version',
    dw_change_type STRING COMMENT 'Type of change: INSERT, UPDATE, DELETE',
    dw_change_reason STRING COMMENT 'Reason or description of change',
    
    -- Metadata
    dw_created_timestamp TIMESTAMP COMMENT 'Timestamp record was created in Gold',
    dw_source_system STRING COMMENT 'Source system identifier',
    dw_batch_id STRING COMMENT 'ETL batch identifier',
    
    -- Partition Column
    p_effective_year_month STRING COMMENT 'Partition key based on effective start: YYYY-MM format'
)
USING DELTA
PARTITIONED BY (p_effective_year_month)
COMMENT 'Gold layer Type 2 SCD history table for orders - tracks all historical changes'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'gold',
    'layer' = 'consumption',
    'table_type' = 'history',
    'scd_type' = '2',
    'source_table' = 'dw_silver.orders'
);
```

### 5.4 Consumer View

```sql
-- Consumer-facing view for dw_orders Gold table
-- Consumers should ONLY access this view, never the underlying _t table
-- All columns explicitly named and aliased for schema evolution flexibility

CREATE OR REPLACE VIEW dw_orders AS
SELECT
    -- Business Keys
    t.id                            AS order_id,
    
    -- Measures
    t.total                         AS order_total,
    
    -- Partition column exposed for efficient filtering
    t.p_order_year_month            AS order_year_month
    
FROM dw_orders_t t
WHERE t.p_order_year_month IS NOT NULL  -- Ensures partition pruning is possible
;

-- Grant appropriate permissions to consumer roles
-- GRANT SELECT ON dw_orders TO data_consumers;
-- GRANT SELECT ON dw_orders TO reporting_role;
-- GRANT SELECT ON dw_orders TO analytics_role;
```

### 5.5 Data Governance Rules

| Rule | Type | SQL Validation | Description |
|------|------|----------------|-------------|
| Order Total Bounds | Range | `SELECT CASE WHEN COUNT(*) = COUNT(CASE WHEN total >= 0 AND total <= 1000000 THEN 1 END) THEN 'PASS' ELSE 'FAIL' END FROM dw_orders` | Ensures totals are non-negative and within limits |
| Unique ID Check | Boundary | `SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT id) THEN 'PASS' ELSE 'FAIL' END FROM dw_orders` | Validates all order IDs are unique |
| Outlier Detection | Statistical | `SELECT CASE WHEN STDDEV(total) <= AVG(total) * 3 THEN 'PASS' ELSE 'FAIL' END FROM dw_orders WHERE total > 0` | Detects unusual variance in totals |
| Record Count Growth | Trend | `SELECT CASE WHEN (SELECT COUNT(*) FROM dw_orders) >= (SELECT COUNT(*) * 0.95 FROM dw_orders_previous_snapshot) THEN 'PASS' ELSE 'FAIL' END` | Ensures counts don't drop >5% |
| Minimum Records | Range | `SELECT CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END FROM dw_orders` | Validates table is not empty |

### 5.6 Failure Handling

| Action | Description |
|--------|-------------|
| Quarantine Failed Records | Move to `dw_orders_quarantine` with failure reason and timestamp |
| Alert and Notify | Generate automated alerts to data stewards with rule name, failure count, samples |
| Block Gold Promotion | Critical rule failures (unique ID, range) block data promotion |
| Soft Fail for Statistical | Statistical/trend rules log warnings but allow data through with flags |
| Maintain Audit Trail | Log all validation results in governance audit table |

---

## 6. Security & Access Control

### 6.1 Database Definitions

```sql
-- =============================================================================
-- RETAIL LAKEHOUSE DATABASE DEFINITIONS
-- Cloud Provider: AWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BRONZE LAYER: Raw data ingestion
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS retail_dw_bronze
COMMENT 'Bronze layer - Raw data copies for retail domain'
LOCATION 's3://retail-lakehouse-bronze/dw_bronze/';

CREATE TABLE IF NOT EXISTS retail_dw_bronze.orders (
    _ingestion_timestamp TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
)
USING DELTA
LOCATION 's3://retail-lakehouse-bronze/dw_bronze/orders/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality.tier' = 'bronze'
);

-- -----------------------------------------------------------------------------
-- SILVER LAYER: Cleansed and validated data
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS retail_dw_silver
COMMENT 'Silver layer - Transformed and validated data for retail domain'
LOCATION 's3://retail-lakehouse-silver/dw_silver/';

CREATE TABLE IF NOT EXISTS retail_dw_silver.orders (
    _processed_timestamp TIMESTAMP,
    _bronze_batch_id STRING,
    _data_quality_score DOUBLE
)
USING DELTA
LOCATION 's3://retail-lakehouse-silver/dw_silver/orders/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality.tier' = 'silver'
);

-- -----------------------------------------------------------------------------
-- GOLD ETL LAYER: Persisted Gold tables (ETL/Engineering access)
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS retail_gold_etl
COMMENT 'Gold ETL layer - Persisted business tables for ETL processes'
LOCATION 's3://retail-lakehouse-gold/gold_etl/';

CREATE TABLE IF NOT EXISTS retail_gold_etl.orders (
    _gold_processed_timestamp TIMESTAMP,
    _silver_version LONG
)
USING DELTA
LOCATION 's3://retail-lakehouse-gold/gold_etl/orders/'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'quality.tier' = 'gold'
);

-- -----------------------------------------------------------------------------
-- GOLD LAYER: Consumer-facing views only
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS retail_gold
COMMENT 'Gold layer - Consumer-facing views for retail domain'
LOCATION 's3://retail-lakehouse-gold/gold/';

CREATE OR REPLACE VIEW retail_gold.orders
COMMENT 'Consumer view for orders data'
AS
SELECT *
FROM retail_gold_etl.orders;

-- -----------------------------------------------------------------------------
-- GOLD HISTORY LAYER: Historical data
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS retail_gold_hist
COMMENT 'Gold History layer - Historical snapshots for retail domain'
LOCATION 's3://retail-lakehouse-gold/gold_hist/';

CREATE TABLE IF NOT EXISTS retail_gold_hist.orders (
    _snapshot_date DATE,
    _snapshot_timestamp TIMESTAMP,
    _is_current BOOLEAN
)
USING DELTA
LOCATION 's3://retail-lakehouse-gold/gold_hist/orders/'
PARTITIONED BY (_snapshot_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality.tier' = 'gold_hist'
);
```

### 6.2 Role Definitions

| Role | Description | Access Level |
|------|-------------|--------------|
| retail_etl_role | ETL engineers with full pipeline access | Full CRUD on all layers |
| retail_data_engineer_role | Data engineers for development | Read on Bronze/Silver, Read on Gold |
| retail_consumer_role | Business consumers | Read-only on Gold views |
| retail_admin_role | Administrators | Full access everywhere |

### 6.3 Access Control Configuration

```sql
-- =============================================================================
-- RETAIL LAKEHOUSE - ACCESS CONTROL CONFIGURATION
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ROLE DEFINITIONS
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS retail_etl_role;
COMMENT ON ROLE retail_etl_role IS 'ETL engineers with full pipeline access';

CREATE ROLE IF NOT EXISTS retail_data_engineer_role;
COMMENT ON ROLE retail_data_engineer_role IS 'Data engineers for development';

CREATE ROLE IF NOT EXISTS retail_consumer_role;
COMMENT ON ROLE retail_consumer_role IS 'Business consumers with Gold read access';

CREATE ROLE IF NOT EXISTS retail_admin_role;
COMMENT ON ROLE retail_admin_role IS 'Administrators with full access';

-- -----------------------------------------------------------------------------
-- BRONZE LAYER PERMISSIONS
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE retail_dw_bronze TO ROLE retail_etl_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE retail_dw_bronze TO ROLE retail_etl_role;
GRANT CREATE TABLE ON DATABASE retail_dw_bronze TO ROLE retail_etl_role;

GRANT USAGE ON DATABASE retail_dw_bronze TO ROLE retail_data_engineer_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_dw_bronze TO ROLE retail_data_engineer_role;

REVOKE ALL PRIVILEGES ON DATABASE retail_dw_bronze FROM ROLE retail_consumer_role;

-- -----------------------------------------------------------------------------
-- SILVER LAYER PERMISSIONS
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE retail_dw_silver TO ROLE retail_etl_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE retail_dw_silver TO ROLE retail_etl_role;
GRANT CREATE TABLE ON DATABASE retail_dw_silver TO ROLE retail_etl_role;

GRANT USAGE ON DATABASE retail_dw_silver TO ROLE retail_data_engineer_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_dw_silver TO ROLE retail_data_engineer_role;

REVOKE ALL PRIVILEGES ON DATABASE retail_dw_silver FROM ROLE retail_consumer_role;

-- -----------------------------------------------------------------------------
-- GOLD ETL LAYER PERMISSIONS
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE retail_gold_etl TO ROLE retail_etl_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE retail_gold_etl TO ROLE retail_etl_role;
GRANT CREATE TABLE ON DATABASE retail_gold_etl TO ROLE retail_etl_role;

GRANT USAGE ON DATABASE retail_gold_etl TO ROLE retail_data_engineer_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_gold_etl TO ROLE retail_data_engineer_role;

REVOKE ALL PRIVILEGES ON DATABASE retail_gold_etl FROM ROLE retail_consumer_role;

-- -----------------------------------------------------------------------------
-- GOLD LAYER PERMISSIONS (Consumer Views)
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE retail_gold TO ROLE retail_etl_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_gold TO ROLE retail_etl_role;
GRANT CREATE VIEW ON DATABASE retail_gold TO ROLE retail_etl_role;

GRANT USAGE ON DATABASE retail_gold TO ROLE retail_data_engineer_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_gold TO ROLE retail_data_engineer_role;

GRANT USAGE ON DATABASE retail_gold TO ROLE retail_consumer_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_gold TO ROLE retail_consumer_role;
GRANT SELECT ON VIEW retail_gold.orders TO ROLE retail_consumer_role;

-- -----------------------------------------------------------------------------
-- GOLD HISTORY LAYER PERMISSIONS
-- -----------------------------------------------------------------------------
GRANT USAGE ON DATABASE retail_gold_hist TO ROLE retail_etl_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE retail_gold_hist TO ROLE retail_etl_role;
GRANT CREATE TABLE ON DATABASE retail_gold_hist TO ROLE retail_etl_role;

GRANT USAGE ON DATABASE retail_gold_hist TO ROLE retail_data_engineer_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_gold_hist TO ROLE retail_data_engineer_role;

GRANT USAGE ON DATABASE retail_gold_hist TO ROLE retail_consumer_role;
GRANT SELECT ON ALL TABLES IN DATABASE retail_gold_hist TO ROLE retail_consumer_role;

-- -----------------------------------------------------------------------------
-- ADMIN ROLE PERMISSIONS
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON DATABASE retail_dw_bronze TO ROLE retail_admin_role;
GRANT ALL PRIVILEGES ON DATABASE retail_dw_silver TO ROLE retail_admin_role;
GRANT ALL PRIVILEGES ON DATABASE retail_gold_etl TO ROLE retail_admin_role;
GRANT ALL PRIVILEGES ON DATABASE retail_gold TO ROLE retail_admin_role;
GRANT ALL PRIVILEGES ON DATABASE retail_gold_hist TO ROLE retail_admin_role;

-- -----------------------------------------------------------------------------
-- FUTURE GRANTS
-- -----------------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES IN DATABASE retail_dw_bronze 
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ROLE retail_etl_role;
  
ALTER DEFAULT PRIVILEGES IN DATABASE retail_dw_silver 
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ROLE retail_etl_role;
  
ALTER DEFAULT PRIVILEGES IN DATABASE retail_gold_etl 
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ROLE retail_etl_role;

ALTER DEFAULT PRIVILEGES IN DATABASE retail_gold 
  GRANT SELECT ON TABLES TO ROLE retail_consumer_role;
  
ALTER DEFAULT PRIVILEGES IN DATABASE retail_gold_hist 
  GRANT SELECT ON TABLES TO ROLE retail_consumer_role;
```

### 6.4 Access Matrix

| Database | ETL Role | Data Engineer | Consumer | Admin |
|----------|----------|---------------|----------|-------|
| retail_dw_bronze | CRUD | SELECT | NO ACCESS | ALL |
| retail_dw_silver | CRUD | SELECT | NO ACCESS | ALL |
| retail_gold_etl | CRUD | SELECT | NO ACCESS | ALL |
| retail_gold | SELECT, CREATE VIEW | SELECT | SELECT | ALL |
| retail_gold_hist | CRUD | SELECT | SELECT | ALL |

### 6.5 Security Templates (For Future Implementation)

#### Row-Level Security Template
```sql
-- Example: Filter orders by region for certain consumers
-- CREATE FUNCTION retail_gold.orders_row_filter(region STRING)
-- RETURNS BOOLEAN
-- RETURN (
--   IS_MEMBER('retail_admin_role') OR
--   region = CURRENT_USER_ATTRIBUTE('allowed_region')
-- );

-- ALTER TABLE retail_gold_etl.orders 
-- SET ROW FILTER retail_gold.orders_row_filter ON (region);
```

#### Column-Level Security Template
```sql
-- Example: Mask sensitive columns for non-admin users
-- CREATE FUNCTION retail_gold.mask_pii(value STRING)
-- RETURNS STRING
-- RETURN CASE 
--   WHEN IS_MEMBER('retail_admin_role') THEN value
--   ELSE '***MASKED***'
-- END;

-- ALTER TABLE retail_gold_etl.orders 
-- ALTER COLUMN customer_email SET MASK retail_gold.mask_pii;
```

---

## 7. Operational Considerations

### 7.1 Data Flow Summary

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │────▶│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│   Systems   │     │  (Raw Copy) │     │ (Cleansed)  │     │ (Business)  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                          │                   │                    │
                          ▼                   ▼                    ▼
                    Full Pull           SCD Type 2            Current +
                    + Metadata          + CDC Hash            History
```

### 7.2 Processing Schedule

| Process | Frequency | Time (UTC) | Description |
|---------|-----------|------------|-------------|
| Bronze Ingestion | Daily | 00:00 | Full pull from source |
| Silver Transformation | Daily | 01:00 | Bronze to Silver with SCD Type 2 |
| Gold Refresh | Daily | 01:30 | Silver to Gold current + history |
| Daily Optimization | Daily | 02:00 | Optimize recent partitions |
| Weekly Maintenance | Weekly | Sunday 03:00 | Full optimization + vacuum |
| Monthly Deep Clean | Monthly | 1st Sunday 04:00 | Complete maintenance cycle |

### 7.3 Monitoring Queries

#### Table Health Check
```sql
SELECT
    num_files,
    size_in_bytes / (1024*1024*1024) AS size_gb,
    num_partitions,
    properties
FROM (DESCRIBE DETAIL dw_silver.orders);
```

#### Optimization History
```sql
SELECT 
    operation,
    operationParameters,
    operationMetrics,
    timestamp
FROM (DESCRIBE HISTORY dw_silver.orders)
WHERE operation IN ('OPTIMIZE', 'VACUUM')
ORDER BY timestamp DESC
LIMIT 20;
```

### 7.4 Compaction Thresholds

| Metric | Threshold | Action |
|--------|-----------|--------|
| Small file count | > 100 per partition | Trigger compaction |
| Average file size | < 64MB | Trigger compaction |
| Table size growth | > 20% since last optimization | Trigger optimization |

### 7.5 Retention Policies

| Layer | Data Retention | File Retention | Log Retention |
|-------|----------------|----------------|---------------|
| Bronze | Indefinite | 7 days | 30 days |
| Silver | Indefinite | 30 days | 60 days |
| Gold | Indefinite | 30 days | 60 days |
| Gold History | Per business requirement | 30 days | 60 days |

---

## 8. Risk Register

### 8.1 Identified Risks

| ID | Risk | Severity | Category | Status |
|----|------|----------|----------|--------|
| R001 | Unclear Mappings | **CRITICAL** | Data Quality | Open |
| R002 | Unknown Data Sensitivity | MEDIUM | Security | Open |
| R003 | Single Source Dependency | LOW | Architecture | Open |

### 8.2 Risk Details and Mitigations

#### R001: Unclear Mappings (CRITICAL)

**Description:** Mapping clarity is "Not provided" - transformation logic, field mappings, and business rules are undefined.

**Impact:** 
- Cannot implement accurate transformations
- Risk of incorrect data in Gold layer
- Potential business decision errors

**Mitigation Actions:**
| Action | Owner | Due Date | Status |
|--------|-------|----------|--------|
| HALT implementation work | Project Lead | Immediate | Required |
| Escalate to stakeholders | Project Lead | Immediate | Required |
| Request complete mapping documentation | Data Architect | ASAP | Required |
| Document source to target field mappings | Customer | TBD | Pending |
| Define transformation rules and business logic | Customer | TBD | Pending |
| Specify data type requirements | Customer | TBD | Pending |
| Define null handling and defaults | Customer | TBD | Pending |
| Obtain formal approval before proceeding | Project Lead | TBD | Pending |

**Required Documentation:**
- Source to target field mappings
- Transformation rules and business logic
- Data type specifications
- Handling of nulls, defaults, and edge cases

#### R002: Unknown Data Sensitivity (MEDIUM)

**Description:** Data classification levels not specified, which could impact security requirements, access controls, and compliance obligations.

**Impact:**
- Potential compliance violations
- Inadequate security controls
- Audit findings

**Mitigation Actions:**
| Action | Owner | Due Date | Status |
|--------|-------|----------|--------|
| Request data classification from customer | Data Governance | ASAP | Pending |
| Identify PII, PHI, PCI, or regulated data | Data Governance | TBD | Pending |
| Implement appropriate security controls | Security Team | TBD | Pending |
| Document data handling requirements | Data Architect | TBD | Pending |

#### R003: Single Source Dependency (LOW)

**Description:** Only one source table identified (orders). If this is the complete inventory, risk is low; if incomplete, there may be missing sources.

**Impact:**
- Incomplete data model
- Missing business context
- Rework required if additional sources discovered

**Mitigation Actions:**
| Action | Owner | Due Date | Status |
|--------|-------|----------|--------|
| Confirm source inventory is complete | Customer | ASAP | Pending |
| Verify no additional tables expected | Data Architect | TBD | Pending |
| Document confirmation in project artifacts | Project Lead | TBD | Pending |

### 8.3 Risk Summary Matrix

```
                    IMPACT
              Low    Medium    High
         ┌─────────┬─────────┬─────────┐
    High │         │         │  R001   │
         ├─────────┼─────────┼─────────┤
L   Med  │         │  R002   │         │
I        ├─────────┼─────────┼─────────┤
K   Low  │  R003   │         │         │
E        └─────────┴─────────┴─────────┘
L
I
H
O
O
D
```

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| Bronze Layer | Raw data ingestion layer with exact source copies |
| Silver Layer | Cleansed, validated, and transformed data layer |
| Gold Layer | Business-ready consumption layer |
| SCD Type 2 | Slowly Changing Dimension pattern that maintains full history |
| CDC | Change Data Capture - tracking data changes over time |
| Delta Lake | Open-source storage layer providing ACID transactions |
| Z-Order | Data clustering technique for query optimization |

## Appendix B: Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | Current | Data Engineering Team | Initial design document |

---

*Document generated for the Retail Data Lakehouse project on AWS*