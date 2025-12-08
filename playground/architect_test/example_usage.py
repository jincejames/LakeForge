"""
Example usage of the Lakehouse Architect Agent.

This script demonstrates how to use the DSPy-powered architect
to generate Lakehouse design documents.

Prerequisites:
1. Install dependencies: pip install -r requirements.txt
2. Authenticate with Databricks:
   - Option A: Run `databricks auth login --host <workspace-url>`
   - Option B: Set environment variables (see config.py)
"""

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# =============================================================================
# Example 1: Quick Design (Simplest Usage)
# =============================================================================


def example_quick_design():
    """Generate a design document with minimal code."""
    from architect import quick_design

    quick_design(
        project_name="retail_analytics",
        cloud_provider="AWS",
        source_tables=[
            {
                "name": "customers",
                "schema": """
                    customer_id INT PRIMARY KEY,
                    first_name STRING,
                    last_name STRING,
                    email STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                """,
                "has_update_timestamp": True,
                "business_context": "Customer master data, updated on profile changes",
            },
            {
                "name": "orders",
                "schema": """
                    order_id INT PRIMARY KEY,
                    customer_id INT,
                    order_date DATE,
                    total_amount DECIMAL(10,2),
                    status STRING,
                    created_at TIMESTAMP
                """,
                "has_update_timestamp": False,
                "business_context": "Order transactions - core fact table",
            },
            {
                "name": "products",
                "schema": """
                    product_id INT PRIMARY KEY,
                    name STRING,
                    category STRING,
                    price DECIMAL(10,2),
                    updated_at TIMESTAMP
                """,
                "has_update_timestamp": True,
                "business_context": "Product catalog dimension",
            },
        ],
        business_mappings="""
        - Join orders to customers on customer_id
        - Join order_items to products on product_id
        - Flatten customer address into orders for analytics
        """,
        consumer_groups="analysts, data_scientists, bi_developers",
        business_rules="""
        - total_amount must be >= 0
        - order_date must be <= current_date
        - status must be one of: pending, confirmed, shipped, delivered, cancelled
        """,
        output_path="DESIGN_DOCUMENT.md",
    )


# =============================================================================
# Example 2: Full Agent Usage (More Control)
# =============================================================================


def example_full_agent():
    """Use the full ArchitectAgent for more control."""
    from architect import ArchitectAgent, ProjectSpec, SourceTable

    # Initialize the agent
    agent = ArchitectAgent(complexity="standard")

    # Define source tables with full detail
    customers = SourceTable(
        name="customers",
        schema="""
            customer_id INT PRIMARY KEY,
            first_name STRING,
            last_name STRING,
            email STRING,
            address_line1 STRING,
            city STRING,
            state STRING,
            zip_code STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        """,
        has_update_timestamp=True,
        has_primary_key=True,
        primary_key="customer_id",
        business_context="Customer master data with address information",
        estimated_row_count="5 million",
        update_frequency="Real-time via CDC",
    )

    orders = SourceTable(
        name="orders",
        schema="""
            order_id INT PRIMARY KEY,
            customer_id INT,
            order_date DATE,
            ship_date DATE,
            total_amount DECIMAL(10,2),
            discount_amount DECIMAL(10,2),
            tax_amount DECIMAL(10,2),
            status STRING,
            created_at TIMESTAMP
        """,
        has_update_timestamp=False,
        has_primary_key=True,
        primary_key="order_id",
        business_context="Order transactions - immutable after creation",
        estimated_row_count="50 million",
        update_frequency="Batch daily",
    )

    order_items = SourceTable(
        name="order_items",
        schema="""
            order_item_id INT PRIMARY KEY,
            order_id INT,
            product_id INT,
            quantity INT,
            unit_price DECIMAL(10,2),
            created_at TIMESTAMP
        """,
        has_update_timestamp=False,
        has_primary_key=True,
        primary_key="order_item_id",
        business_context="Order line items - immutable",
        estimated_row_count="200 million",
        update_frequency="Batch daily",
    )

    products = SourceTable(
        name="products",
        schema="""
            product_id INT PRIMARY KEY,
            sku STRING,
            name STRING,
            description STRING,
            category STRING,
            subcategory STRING,
            brand STRING,
            price DECIMAL(10,2),
            cost DECIMAL(10,2),
            updated_at TIMESTAMP
        """,
        has_update_timestamp=True,
        has_primary_key=True,
        primary_key="product_id",
        business_context="Product catalog - prices update frequently",
        estimated_row_count="100,000",
        update_frequency="Real-time via CDC",
    )

    # Create project specification
    project = ProjectSpec(
        name="retail_analytics_platform",
        cloud_provider="AWS",
        source_tables=[customers, orders, order_items, products],
        business_mappings="""
        Bronze → Silver Transformations:
        1. customers: Flatten address, add _valid_from/_valid_to for SCD Type 2
        2. orders: Join with customers to get customer_state for partitioning
        3. order_items: Join with products to get category for analytics
        4. products: Track price history with SCD Type 2

        Silver → Gold Transformations:
        1. dim_customer: Current customer state only
        2. dim_product: Current product catalog
        3. fact_orders: Orders with denormalized customer/product attributes
        4. fact_order_items: Line items with product details
        """,
        consumer_groups="bi_team, analytics_team, data_science_team, executive_dashboard",
        business_rules="""
        1. total_amount = SUM(quantity * unit_price) - discount_amount + tax_amount
        2. order_date <= ship_date (when both exist)
        3. quantity > 0
        4. price >= cost (margin validation)
        5. Daily order count should be within 20% of 30-day average
        """,
        data_sensitivity="Internal Only - No PII in Gold layer",
    )

    # Generate the design
    result = agent.generate_design(project)

    # Check for escalations
    if result.escalation_required:
        logging.warning("ESCALATION REQUIRED!")
        logging.warning("Reason: %s", result.architecture.escalation_reason)

    # Save the document
    output_path = agent.save_document(result, "DESIGN_DOCUMENT.md")
    logging.info("Design document saved to: %s", output_path)

    return result


# =============================================================================
# Example 3: Incremental Design (One Layer at a Time)
# =============================================================================


def example_incremental_design():
    """Design each layer incrementally for more granular control."""
    from architect import ArchitectAgent, SourceTable

    agent = ArchitectAgent(complexity="simple")

    # Define a single table
    orders = SourceTable(
        name="orders",
        schema="order_id INT, customer_id INT, total DECIMAL, order_date DATE",
        has_update_timestamp=False,
        business_context="Order transactions",
    )

    # Step 1: Qualify the source
    qualification = agent.qualify_source(orders)
    logging.info("Classification: %s", qualification.classification)
    logging.info("Mutability: %s", qualification.mutability_type)
    logging.info("Ingestion Strategy: %s", qualification.ingestion_strategy)

    # Step 2: Design Bronze layer
    bronze = agent.design_bronze(orders)
    logging.info("Bronze DDL:\n%s", bronze.bronze_table_ddl)
    logging.info("Storage: %s", bronze.storage_location)

    # Step 3: Design Silver layer
    silver = agent.design_silver(
        bronze_table="dw_bronze.orders",
        target_silver_table="dw_silver.orders",
        primary_key_columns="order_id",
        common_query_patterns="Filter by order_date, join on customer_id",
    )
    logging.info("Silver DDL:\n%s", silver.silver_table_ddl)
    logging.info("Partition Strategy: %s", silver.partition_strategy)

    # Step 4: Design Gold layer
    gold = agent.design_gold(
        silver_source_table="dw_silver.orders",
        table_purpose="Order analytics for BI dashboards",
        table_schema="order_id INT, customer_id INT, total DECIMAL, order_date DATE",
        requires_history=True,
        business_rules="total >= 0, order_date <= current_date",
    )
    logging.info("Gold Table DDL:\n%s", gold.current_table_ddl)
    logging.info("View DDL:\n%s", gold.view_ddl)
    logging.info("Governance Rules:\n%s", gold.governance_rules)


# =============================================================================
# Example 4: Using Individual Signatures
# =============================================================================


def example_signatures():
    """Use individual DSPy signatures for maximum flexibility."""
    import dspy

    from architect import ClassifyDataSource, DesignGovernanceRules, configure_lm

    # Configure the LM
    configure_lm("claude-opus")

    # Use ClassifyDataSource signature directly
    classify = dspy.ChainOfThought(ClassifyDataSource)

    result = classify(
        table_name="customer_transactions",
        table_schema="""
            transaction_id BIGINT,
            customer_id INT,
            amount DECIMAL(12,2),
            transaction_type STRING,
            transaction_date TIMESTAMP,
            created_at TIMESTAMP
        """,
        business_context="High-volume transaction log, append-only",
    )

    logging.info("Classification: %s", result.classification)
    logging.info("Reasoning: %s", result.reasoning)
    logging.info("Design Considerations: %s", result.design_considerations)

    # Use DesignGovernanceRules signature
    governance = dspy.ChainOfThought(DesignGovernanceRules)

    rules = governance(
        table_name="fact_orders",
        table_schema="order_id INT, total DECIMAL, order_date DATE, status STRING",
        business_rules="""
        - Total must be positive
        - Order date cannot be in the future
        - Status must be valid enum value
        """,
        critical_columns="total, order_date, status",
    )

    logging.info("Governance Rules:\n%s", rules.governance_rules)
    logging.info("Failure Handling: %s", rules.failure_handling)


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    # Uncomment the example you want to run:

    # example_quick_design()
    example_full_agent()
    # example_incremental_design()
    # example_signatures()

