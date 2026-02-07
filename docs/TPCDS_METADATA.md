# TPC-DS Metadata and Knowledge Rules

This document describes the TPC-DS metadata and knowledge rules that have been added to the RCA Engine system.

## Overview

TPC-DS metadata has been integrated into the following metadata files:
- `metadata/tables.json` - Table schemas and column definitions
- `metadata/knowledge_base.json` - Table descriptions, relationships, and business terms
- `metadata/semantic_registry.json` - Dimensional models and join paths
- `metadata/lineage.json` - Data lineage relationships
- `metadata/rules.json` - Business rules and best practices

## Knowledge Base (`knowledge_base.json`)

### Tables Added

24 TPC-DS tables have been documented with descriptions:

**Fact Tables:**
- `store_sales` - Sales transactions through store channel
- `store_returns` - Returns for store channel sales
- `catalog_sales` - Sales transactions through catalog channel
- `catalog_returns` - Returns for catalog channel sales
- `web_sales` - Sales transactions through web channel
- `web_returns` - Returns for web channel sales
- `inventory` - Product quantities on-hand at warehouses by week

**Dimension Tables:**
- `customer` - Customer information
- `customer_address` - Customer addresses
- `customer_demographics` - Customer demographic combinations
- `household_demographics` - Household demographics
- `date_dim` - Calendar date dimension
- `item` - Product/item information
- `store` - Store locations and details
- `warehouse` - Warehouse locations and details
- `promotion` - Promotion information
- `call_center` - Call center details
- `catalog_page` - Catalog page details
- `web_site` - Website details
- `web_page` - Web page details
- `time_dim` - Time dimension
- `ship_mode` - Shipping mode information
- `reason` - Return reason codes
- `income_band` - Income band ranges

### Relationships Added

25 relationships have been defined:

**Sales Relationships:**
- `store_sales` → `customer` (made_by)
- `store_sales` → `item` (references)
- `store_sales` → `store` (at)
- `store_sales` → `date_dim` (on)
- `store_sales` → `promotion` (uses)
- `web_sales` → `customer` (made_by)
- `web_sales` → `item` (references)
- `web_sales` → `date_dim` (on)
- `web_sales` → `web_site` (on)
- `web_sales` → `warehouse` (from)
- `catalog_sales` → `customer` (made_by)
- `catalog_sales` → `item` (references)
- `catalog_sales` → `date_dim` (on)
- `catalog_sales` → `call_center` (through)
- `catalog_sales` → `warehouse` (from)

**Inventory Relationships:**
- `inventory` → `item` (references)
- `inventory` → `warehouse` (at)
- `inventory` → `date_dim` (on)

**Customer Relationships:**
- `customer` → `customer_address` (has)
- `customer` → `customer_demographics` (has)
- `customer` → `household_demographics` (belongs_to)

**Return Relationships:**
- `store_returns` → `store_sales` (returns)
- `web_returns` → `web_sales` (returns)
- `catalog_returns` → `catalog_sales` (returns)

### Business Terms Added

5 business terms have been defined:
- `multi-channel` - Sales across multiple channels (store, catalog, web)
- `sales` - Sales transactions across all channels
- `returns` - Product returns across all channels
- `revenue` - Total sales revenue
- `inventory` - Product inventory levels at warehouses

## Semantic Registry (`semantic_registry.json`)

### Dimensions Added

6 dimensional models have been created:

1. **tpcds_customer_dimension**
   - Base table: `customer`
   - Join path: customer → customer_address → customer_demographics → household_demographics

2. **tpcds_store_sales_dimension**
   - Base table: `store_sales`
   - Join path: store_sales → customer → item → store → date_dim → promotion

3. **tpcds_web_sales_dimension**
   - Base table: `web_sales`
   - Join path: web_sales → customer → item → date_dim → web_site → warehouse

4. **tpcds_catalog_sales_dimension**
   - Base table: `catalog_sales`
   - Join path: catalog_sales → customer → item → date_dim → call_center → warehouse

5. **tpcds_inventory_dimension**
   - Base table: `inventory`
   - Join path: inventory → item → warehouse → date_dim

6. **tpcds_multi_channel_sales**
   - Base table: `customer`
   - Join path: customer → store_sales, web_sales, catalog_sales
   - Purpose: Analyze customer purchases across all channels

## Lineage (`lineage.json`)

### Edges Added

25 lineage edges have been added, matching the relationships in knowledge_base.json:

- Sales fact tables to customer, item, date_dim, and channel-specific dimensions
- Inventory to item, warehouse, and date_dim
- Customer to address and demographics
- Returns to their corresponding sales tables

These edges enable:
- Data lineage tracking
- Join path discovery
- Query optimization
- Impact analysis

## Business Rules (`rules.json`)

### Rules Added

12 business rules have been defined:

1. **tpcds_multi_channel_sales**
   - Rule: Total sales = SUM(store_sales.ss_sales_price) + SUM(catalog_sales.cs_sales_price) + SUM(web_sales.ws_sales_price)
   - Type: Aggregation

2. **tpcds_sales_date_join**
   - Rule: Sales fact tables must join with date_dim using sold_date_sk
   - Type: Join

3. **tpcds_customer_sales_join**
   - Rule: Store sales uses ss_customer_sk, web/catalog use bill_customer_sk
   - Type: Join

4. **tpcds_item_sales_join**
   - Rule: All sales join to item using item_sk columns
   - Type: Join

5. **tpcds_returns_reference_sales**
   - Rule: Returns reference sales using ticket_number/order_number and item_sk
   - Type: Relationship

6. **tpcds_inventory_snapshot**
   - Rule: Inventory joins with date_dim, item, and warehouse
   - Type: Join

7. **tpcds_promotion_optional**
   - Rule: Use LEFT JOIN for promotions as they're optional
   - Type: Join

8. **tpcds_revenue_calculation**
   - Rule: Revenue = SUM(sales_price), Net Profit = SUM(net_profit)
   - Type: Calculation

9. **tpcds_customer_demographics**
   - Rule: Customer links to demographics tables
   - Type: Join

10. **tpcds_warehouse_shipment**
    - Rule: Web and catalog sales ship from warehouses
    - Type: Join

11. **tpcds_date_dimension_usage**
    - Rule: Always join date_dim for time-based queries
    - Type: Best Practice

12. **tpcds_channel_identification**
    - Rule: Add literal 'store', 'catalog', or 'web' when combining channels
    - Type: Best Practice

## Usage Examples

### Query Generation

The metadata enables the system to:

1. **Identify relevant tables** based on natural language queries:
   - "Show me sales by customer" → Identifies `store_sales`, `web_sales`, `catalog_sales`, `customer`

2. **Suggest join paths**:
   - Customer sales query → Uses `tpcds_customer_dimension` or `tpcds_store_sales_dimension`

3. **Apply business rules**:
   - "Total revenue" → Applies `tpcds_multi_channel_sales` rule to combine all channels

4. **Validate relationships**:
   - Ensures joins use correct key columns (e.g., `ss_customer_sk` vs `ws_bill_customer_sk`)

### Example Query Generation

**Natural Language:** "Show me top 10 customers by total sales across all channels"

**Generated SQL (using metadata):**
```sql
SELECT 
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    COALESCE(SUM(ss.ss_sales_price), 0) + 
    COALESCE(SUM(cs.cs_sales_price), 0) + 
    COALESCE(SUM(ws.ws_sales_price), 0) as total_sales
FROM tpcds.tiny.customer c
LEFT JOIN tpcds.tiny.store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
LEFT JOIN tpcds.tiny.catalog_sales cs ON c.c_customer_sk = cs.cs_bill_customer_sk
LEFT JOIN tpcds.tiny.web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
ORDER BY total_sales DESC
LIMIT 10;
```

**Metadata Used:**
- `tpcds_multi_channel_sales` rule for aggregation
- `tpcds_customer_sales_join` rule for correct join columns
- `tpcds_multi_channel_sales` dimension for join path

## Benefits

1. **Accurate Query Generation**: Metadata ensures correct table and column selection
2. **Proper Joins**: Rules enforce correct join conditions across channels
3. **Multi-Channel Support**: Enables combining sales from all three channels
4. **Time-Based Analysis**: Date dimension integration for temporal queries
5. **Demographic Analysis**: Customer demographics relationships enable segmentation
6. **Return Analysis**: Return-to-sales relationships enable return analysis

## Maintenance

To add new TPC-DS tables or relationships:

1. **Add table schema** to `metadata/tables.json`
2. **Add table description** to `metadata/knowledge_base.json`
3. **Add relationships** to `metadata/knowledge_base.json` and `metadata/lineage.json`
4. **Add dimensions** to `metadata/semantic_registry.json` if needed
5. **Add rules** to `metadata/rules.json` for business logic

Use the extraction script to automatically generate schemas:
```bash
python scripts/extract_tpcds_schema.py tiny
```

## References

- [TPC-DS Setup Guide](./TPCDS_SETUP.md)
- [Trino Configuration](../config/trino/README.md)
- [TPC-DS Benchmark Specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf)
