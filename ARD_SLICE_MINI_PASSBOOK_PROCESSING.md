# ARD slice mini passbook - Processing Summary

**URL:** https://slicepay.atlassian.net/wiki/spaces/HOR/pages/2898362610/ARD+slice+mini+passbook  
**Page ID:** 2898362610  
**Space:** Horizontal-Analytics (HOR)  
**Version:** 20  
**Last Modified:** 2023-02-13T06:09:18.557Z

## What Happens When Processing This Page

### 1. Page Fetching ✓
- **Action:** Fetches page from Confluence API using REST endpoint
- **Endpoint:** `GET /rest/api/content/2898362610?expand=body.storage,space,version,metadata.labels`
- **Result:** 
  - Page title: "ARD: slice mini passbook"
  - Space: Horizontal-Analytics (HOR)
  - Parent page: ARD Repository (ID: 2883059807)
  - Full HTML content retrieved

### 2. Product Extraction ✓
- **Action:** Extracts product name from page title
- **Method:** Removes document type prefix (ARD:) and extracts product name
- **Result:** 
  - Product: `slice mini passbook`
  - Document Type: `ARD`

### 3. Knowledge Extraction ✓

#### 3.1 Table Extraction
- **Action:** Parses HTML tables from page content
- **Method:** 
  - Detects tables with headers in first row (even if using `<td>` instead of `<th>`)
  - Extracts table structure with headers and data rows
- **Result:** 
  - **1 table extracted** with 13 columns and 15 data rows
  - Table headers: EVENT NAME, EVENT TYPE, FE/BE, POD OWNER, EVENT TRIGGER, EVENT DESCRIPTION, SCREENSHOT, EVENT PROPERTY, EVENT PROPERTY VALUE, Comments, AMPLITUDE, CLEVERTAP, PRIORITISATION

#### 3.2 Event Extraction
- **Action:** Extracts ARD events from the events table
- **Method:** 
  - Identifies table with "EVENT NAME" and "EVENT TYPE" columns
  - Parses each row as an event
- **Result:** 
  - **15 events extracted**, including:
    - `mini_passbook_opened` (PAGE OPEN)
    - `mini_passbook_error_page_opened` (page_opened)
    - `back_clicked` (CTA)
    - `mini_passbook_search_clicked` (CTA)
    - `mini_passbook_search_entered` (entered)
    - `mini_passbook_search_delete_clicked` (CTA)
    - `mini_passbook_search_no_results_opened` (PAGE OPEN)
    - `mini_passbook_search_no_results_check_all_transactions_clicked` (CTA)
    - `mini_passbook_filter_clicked` (CTA)
    - `mini_passbook_filter_bottomsheet_opened` (Bottomsheet open)
    - `mini_passbook_filter_bottomsheet_filter_selected` (CTA)
    - `mini_passbook_filter_bottomsheet_apply_clicked` (CTA)
    - `mini_passbook_filter_bottomsheet_clear_all_clicked` (CTA)
    - `mini_passbook_transaction_clicked` (cta)

#### 3.3 Entity Extraction
- **Action:** Extracts entities (tables, domains, concepts) from content
- **Method:** Pattern matching for table mentions and domain concepts
- **Result:** 
  - **1 entity extracted:** `passbook` (Table mentioned in ARD document)

#### 3.4 Relationship Extraction
- **Action:** Extracts relationships between entities
- **Result:** **0 relationships** (no explicit relationship patterns found)

#### 3.5 Metrics Extraction
- **Action:** Extracts metrics from document
- **Result:** **0 metrics** (metrics section was empty in this document)

### 4. Knowledge Register Population ✓
- **Action:** Stores extracted knowledge in Knowledge Register for search
- **Result:** 
  - Reference ID created: `PROD-3649-2898362610`
  - Page indexed with:
    - Title: "ARD: slice mini passbook"
    - Product: "slice mini passbook"
    - Keywords: extracted from content
    - Segments: entities, events, tables, relationships, metrics
    - Full text: searchable content

### 5. Knowledge Base Population ✓
- **Action:** Adds extracted knowledge to Knowledge Base
- **Result:** 
  - Events added to knowledge base (15 events)
  - Tables added to knowledge base (1 table)
  - Entities indexed
  - Knowledge base file updated: `metadata/knowledge_base.json`

### 6. Product Index Update ✓
- **Action:** Links document to product in Product Index
- **Result:** 
  - Product "slice mini passbook" indexed
  - Document linked to product
  - Product index file updated: `metadata/product_index.json`

### 7. Files Created/Updated

#### Raw Data Files:
- `data/raw/confluence_page_2898362610.html` - Raw HTML content
- `data/raw/confluence_page_2898362610_metadata.json` - Page metadata (title, space, version, etc.)

#### Metadata Files (Updated):
- `metadata/knowledge_register.json` - Searchable page index
- `metadata/knowledge_base.json` - Structured knowledge (events, tables, entities)
- `metadata/product_index.json` - Product-to-document mapping

## Summary Statistics

| Category | Count |
|----------|-------|
| **Entities** | 1 |
| **Events** | 15 |
| **Tables** | 1 |
| **Relationships** | 0 |
| **Metrics** | 0 |

## What Can Be Done Next

1. **Vector Search:** Run `python src/pipeline.py --step all` to index for vector search
2. **Query:** Use `python test_query.py --question 'What is slice mini passbook?' --project 'slice mini passbook'`
3. **Search:** Search by:
   - Product name: "slice mini passbook"
   - Reference ID: "PROD-3649-2898362610"
   - Keywords: "passbook", "mini", "events", etc.
4. **API Access:** Access via Knowledge Base API or Knowledge Register API

## Event Details

The 15 events extracted represent the complete event tracking specification for the slice mini passbook feature:

- **Page Open Events:** User opens passbook, error pages, search results
- **CTA Events:** User clicks buttons (back, search, filter, apply, clear)
- **Interaction Events:** User types in search, selects filters, clicks transactions
- **Event Properties:** Each event includes properties like app_version, event_type, filter_type, etc.

All events are properly categorized and stored with their descriptions, triggers, and properties for analytics tracking.

