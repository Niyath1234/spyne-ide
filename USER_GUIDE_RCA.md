# RCA Engine - Complete User Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Step-by-Step: Performing an RCA](#step-by-step-performing-an-rca)
4. [Understanding the Results](#understanding-the-results)
5. [Advanced Usage](#advanced-usage)
6. [Troubleshooting](#troubleshooting)
7. [Best Practices](#best-practices)
8. [Hypergraph Visualization](#hypergraph-visualization)

---

## Introduction

The RCA (Root Cause Analysis) Engine is an intelligent system that automatically identifies and explains discrepancies between data systems. This guide will walk you through the complete end-to-end process of performing an RCA analysis using the web application.

### What You Can Do

- **Compare Data Across Systems**: Identify differences between two data systems (e.g., System A vs System B)
- **Find Root Causes**: Automatically discover why discrepancies exist
- **Row-Level Analysis**: Get exact rows causing differences, not just aggregate mismatches
- **Natural Language Queries**: Ask questions in plain English
- **Complete Audit Trail**: See how data was transformed through the pipeline

---

## Getting Started

### Prerequisites

1. **Server Running**: Ensure the RCA Engine server is running on `http://localhost:8080`
   ```bash
   # Start the server
   ./start_server.sh
   # Or
   cargo run --bin server
   ```

2. **UI Running**: Open the web application in your browser
   - Navigate to `http://localhost:8080` (or the configured port)
   - The UI should load with the RCA Engine interface

3. **Data Sources Ready**: Have your CSV data files ready to upload (optional if data already exists)

### Initial Setup

1. **Open the Application**
   - Launch your browser and navigate to the RCA Engine URL
   - You should see the main interface with a top bar and sidebar

2. **Verify Server Connection**
   - The UI will automatically try to connect to the backend
   - If you see an error, ensure the server is running

---

## Step-by-Step: Performing an RCA

### Step 1: Add Data Sources (If Needed)

If you haven't already added your data sources, follow these steps:

#### 1.1 Navigate to Data Sources

- Click on **"Pipelines"** or **"Data Sources"** in the sidebar (or top navigation)
- You'll see a list of existing data sources, or an empty state if none exist

#### 1.2 Add a New CSV Source

1. **Click "Add CSV Source"** button
   - This opens a dialog for uploading and configuring a new data source

2. **Upload CSV File**
   - Click **"Upload CSV File"** button
   - Select your CSV file from your computer
   - The system will automatically parse and preview the file

3. **Review CSV Preview**
   - You'll see a preview table showing:
     - Column headers
     - Sample rows (first 2 rows)
   - Verify the data looks correct

4. **Provide Column Descriptions**
   - For each column, enter a brief description
   - This helps the system understand your data better
   - Example: For a "loan_id" column, you might write "Unique identifier for each loan"

5. **Fill Table Information**
   - **Source Name**: Give your data source a meaningful name (e.g., "loans_system_a")
   - **What is this table about?**: Brief description (e.g., "Loan transactions from System A")
   - **System Name**: The system this data belongs to (e.g., "system_a", "core_banking", "scf_v1")
   - **Entity Name**: The type of entity (e.g., "loan", "payment", "transaction")

6. **Select Primary Keys**
   - Check the columns that uniquely identify each row
   - Common examples: `uuid`, `loan_id`, `transaction_id`
   - You can select multiple columns for composite keys

7. **Select Grain Columns** (Optional)
   - Grain defines the level of aggregation
   - If left empty, primary keys will be used as grain
   - Examples: `["uuid"]`, `["loan_id", "paid_date"]`

8. **Process & Register**
   - Click **"Process & Register"** button
   - The system will:
     - Upload the CSV
     - Parse and detect schema
     - Create metadata
     - Register to knowledge graph
   - Wait for processing to complete (progress bar will show)

9. **Verify Source Added**
   - After processing, you'll see the new source in the list
   - Status should show as "active" (green checkmark)

#### 1.3 Repeat for Additional Systems

- If comparing two systems, repeat the process for System B
   - Example: Upload `loans_system_b.csv` with system name "system_b"
- Ensure both systems have compatible schemas for comparison

---

### Step 2: Navigate to Reasoning Interface

1. **Open Reasoning Chat**
   - Click on **"Reasoning"** in the sidebar or top navigation
   - You'll see a chat interface similar to ChatGPT

2. **Familiarize Yourself with the Interface**
   - Input field at the bottom for your queries
   - Message area showing analysis steps and results
   - Clear button to reset the conversation

---

### Step 3: Formulate Your RCA Query

#### 3.1 Query Structure

Your query should include:
- **Systems to compare**: Which two systems are you comparing?
- **Metric of interest**: What metric is mismatching? (e.g., recovery, balance, count)
- **Filters/Constraints**: Any specific conditions? (e.g., date, loan type, status)

#### 3.2 Example Queries

**Basic Mismatch Query:**
```
Why is recovery mismatching between system_a and system_b?
```

**With Date Filter:**
```
Why is recovery mismatching between system_a and system_b for Digital loans on 2026-01-08?
```

**With Specific Metric:**
```
Why is ledger balance different between scf_v1 and scf_v2?
```

**Complex Query:**
```
Why is total outstanding mismatching between core_banking and reconciliation_system for active loans as of 2026-01-15?
```

#### 3.3 Query Best Practices

✅ **Good Queries:**
- Clear system names: "system_a and system_b"
- Specific metrics: "recovery", "ledger balance", "total outstanding"
- Include filters when relevant: "for Digital loans", "on 2026-01-08"
- Natural language: Write as you would ask a colleague

❌ **Avoid:**
- Vague queries: "What's wrong?"
- Missing system names: "Why is there a mismatch?"
- Too many filters at once (start simple, then refine)

---

### Step 4: Submit Your Query

1. **Type Your Query**
   - Enter your question in the input field at the bottom
   - You can use multi-line text (Shift+Enter for new line)

2. **Submit**
   - Click the **Send** button (orange button with arrow icon)
   - Or press **Enter** (without Shift) to submit

3. **Wait for Processing**
   - You'll see a "💭 Analyzing..." message
   - The system processes your query through multiple steps:
     - **💭 Thought**: System thinking about your query
     - **⚙️ Action**: System performing operations
     - **📊 Result**: Final analysis results

---

### Step 5: Review Analysis Steps

As the analysis progresses, you'll see real-time updates:

#### 5.1 Thought Steps (💭)
- Shows the system's reasoning process
- Example: "Analyzing query: Why is recovery mismatching..."
- Example: "Detected mismatch query. Identifying systems and metrics involved..."

#### 5.2 Action Steps (⚙️)
- Shows what the system is doing
- Example: "Querying data sources: system_a and system_b"
- Example: "Comparing data and detecting differences..."
- Example: "Analyzing root causes..."

#### 5.3 Result Step (📊)
- Contains the complete analysis results
- Includes:
  - Summary of findings
  - Root causes identified
  - Recommendations
  - Detailed mismatch data (if applicable)

---

### Step 6: Interpret the Results

The final result will contain several sections:

#### 6.1 Analysis Summary

```
Root Cause Analysis Complete

Query: Why is recovery mismatching between system_a and system_b?

Analysis Steps:
1. ✅ Identified systems: system_a and system_b
2. ✅ Detected metric: recovery
3. ✅ Found mismatch: Significant difference detected
4. ✅ Analyzed data sources
5. ✅ Identified root causes
```

#### 6.2 Root Causes Found

The system identifies specific root causes:

```
Root Causes Found:
- Data synchronization delay between systems
- Missing transactions in system_b
- Calculation method differences
```

Each root cause explains why the discrepancy exists.

#### 6.3 Recommendations

Actionable steps to resolve the issues:

```
Recommendations:
- Review data sync process
- Verify transaction completeness
- Align calculation methods
```

#### 6.4 Mismatch Details (Table)

If there are specific mismatches, you'll see a data table:

| System | Metric | Value | Status | Difference |
|--------|--------|-------|--------|------------|
| system_a | Ledger Balance | 125000.00 | Mismatch | +5000.00 |
| system_b | Ledger Balance | 120000.00 | Mismatch | -5000.00 |
| system_a | Transaction Count | 150 | Match | 0 |
| system_b | Transaction Count | 145 | Mismatch | -5 |

**Interpreting the Table:**
- **System**: Which system the data is from
- **Metric**: What metric is being compared
- **Value**: The actual value in that system
- **Status**: Whether it matches or mismatches
- **Difference**: How much it differs from the other system

#### 6.5 Download Results (Optional)

- If a data table is shown, you'll see a **"Download CSV"** button
- Click to download the mismatch details as a CSV file
- Useful for further analysis or reporting

---

### Step 7: Follow Up Queries (Optional)

You can ask follow-up questions to dive deeper:

**Examples:**
```
Show me the specific rows that are missing in system_b
```

```
What are the join conditions that failed?
```

```
Explain why row uuid_001 is different between systems
```

**How to Ask Follow-ups:**
1. Simply type your new question in the input field
2. The system maintains context from your previous query
3. Submit and review the additional analysis

---

## Understanding the Results

### Result Components Explained

#### 1. **System Identification**
- The system correctly identifies which systems you're comparing
- Verifies that both systems exist in your data sources

#### 2. **Metric Detection**
- Extracts the metric you're interested in from your query
- Maps it to the appropriate business rules and calculations

#### 3. **Mismatch Detection**
- Compares data at the row level (not just aggregates)
- Identifies:
  - **Missing rows**: Rows in one system but not the other
  - **Extra rows**: Rows in the other system but not the first
  - **Value mismatches**: Same row, different values
  - **Matches**: Rows that are identical

#### 4. **Root Cause Analysis**
- Traces data transformations:
  - **Join failures**: Rows dropped due to failed joins
  - **Filter exclusions**: Rows filtered out by conditions
  - **Calculation differences**: Different formulas or rules applied
  - **Data quality issues**: Missing values, nulls, type mismatches

#### 5. **Verification Proof**
- Mathematically verifies that row-level differences explain aggregate mismatches
- Provides confidence in the analysis

---

## Smart Query Understanding (Fail-Fast Clarification)

The RCA Engine uses an intelligent **fail-fast mechanism** that ensures queries have enough information before attempting analysis. If the system isn't confident about your query, it will ask **ONE clarifying question** covering all missing pieces.

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          QUERY SUBMISSION                                    │
│                                                                              │
│  User: "Why is the balance different?"                                       │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                       STEP 1: CONFIDENCE ASSESSMENT                          │
│                                                                              │
│  📊 Analyzing query...                                                       │
│  ✅ Detected: metric = "balance"                                             │
│  ❌ Missing: Which systems to compare?                                       │
│  ❌ Missing: Which entity grain? (loan, customer, account?)                  │
│                                                                              │
│  Confidence: 45% (below 70% threshold)                                       │
│  Decision: FAIL FAST → Ask for clarification                                 │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STEP 2: SINGLE CLARIFYING QUESTION                        │
│                                                                              │
│  🤖 "I need a bit more context. Could you specify:                          │
│      (1) which two systems to compare (e.g., system_a vs system_b),          │
│      (2) and optionally the level of detail (loan-level, customer-level)?"  │
│                                                                              │
│  Response Hints:                                                             │
│    - "systems: system_a, system_b"                                           │
│    - "grain: loan_id"                                                        │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                       STEP 3: USER PROVIDES ANSWER                           │
│                                                                              │
│  User: "Compare system_a and system_b at loan level"                         │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                        ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STEP 4: COMPILE WITH CLARIFICATION                        │
│                                                                              │
│  Original: "Why is the balance different?"                                   │
│  + Answer: "Compare system_a and system_b at loan level"                     │
│                                                                              │
│  Confidence: 95% ✅                                                          │
│  Decision: PROCEED with analysis                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### API Endpoints

#### 1. Assess Query (Fail-Fast Check)

**Endpoint:** `POST /api/reasoning/assess`

**Request:**
```json
{
  "query": "Why is the balance different?"
}
```

**Response (Needs Clarification):**
```json
{
  "status": "needs_clarification",
  "needs_clarification": true,
  "question": "I need a bit more context. Could you specify: (1) which two systems to compare (e.g., system_a vs system_b, khatabook vs tally), and (2) what level of detail you want (loan-level, customer-level)?",
  "missing_pieces": [
    {
      "field": "systems",
      "description": "Which systems to compare",
      "importance": "Required",
      "suggestions": ["system_a", "system_b", "khatabook", "tally"]
    },
    {
      "field": "grain",
      "description": "Level of analysis",
      "importance": "Helpful",
      "suggestions": ["loan_id", "customer_id", "account_id"]
    }
  ],
  "confidence": 0.45,
  "partial_understanding": {
    "task_type": "RCA",
    "systems": [],
    "metrics": ["balance"],
    "entities": [],
    "grain": [],
    "keywords": ["balance", "different"]
  },
  "response_hints": [
    "systems: system_a, system_b",
    "grain: loan_id"
  ]
}
```

**Response (Success - No Clarification Needed):**
```json
{
  "status": "success",
  "needs_clarification": false,
  "intent": {
    "task_type": "RCA",
    "systems": ["system_a", "system_b"],
    "target_metrics": ["balance"],
    "grain": ["loan_id"],
    ...
  },
  "message": "Query understood. Ready to execute."
}
```

#### 2. Submit Clarification Answer

**Endpoint:** `POST /api/reasoning/clarify`

**Request:**
```json
{
  "query": "Why is the balance different?",
  "answer": "Compare system_a and system_b at loan level"
}
```

**Response:**
```json
{
  "status": "success",
  "needs_clarification": false,
  "intent": {
    "task_type": "RCA",
    "systems": ["system_a", "system_b"],
    "target_metrics": ["balance"],
    "grain": ["loan_id"],
    ...
  },
  "message": "Query understood with clarification. Ready to execute."
}
```

### What Triggers Clarification?

| Missing Information | Importance | Example Fix |
|---------------------|------------|-------------|
| **Systems** | Required | "between system_a and system_b" |
| **Metrics** | Required | "compare TOS" or "recovery metric" |
| **Grain** | Helpful | "at loan level" or "per customer" |
| **Constraints** | Helpful | "for active loans" or "in January 2025" |

### Examples: Vague vs Clear Queries

| ❌ Vague Query | 🤖 System Asks | ✅ Clear Query |
|----------------|----------------|----------------|
| "Why is the balance different?" | "Which systems?" | "Compare balance between system_a and system_b" |
| "Check TOS" | "Which systems and what grain?" | "Compare TOS between khatabook and TB at loan level" |
| "Find mismatches" | "What metric and systems?" | "Find recovery mismatches between system_a and system_b for active loans" |

### Benefits

1. **No Wasted Computation**: Avoids running analysis on incomplete queries
2. **Better Results**: More context = more accurate analysis
3. **Single Question**: ONE consolidated question, not multiple back-and-forth
4. **Partial Understanding Shown**: You can see what the system already understood
5. **Response Hints**: Suggestions for how to answer

### Configuration

The confidence threshold is configurable:

```rust
// In IntentCompiler
let compiler = IntentCompiler::new(llm)
    .with_confidence_threshold(0.7)  // 70% confidence required
    .with_fail_fast(true);           // Enable fail-fast
```

- **Default threshold**: 70% confidence required
- **Below threshold**: Asks clarifying question
- **Above threshold**: Proceeds with compilation

---

## Advanced Usage

### Using Rules View

1. **Navigate to Rules**
   - Click **"Rules"** in the sidebar
   - View all business rules defined in your metadata

2. **Understand Rule Structure**
   - Each rule shows:
     - System it belongs to
     - Metric it calculates
     - Target entity and grain
     - Computation formula

3. **Use Rules in Queries**
   - Reference specific rules in your queries
   - Example: "Why is the system_a_recovery rule producing different results?"

### Monitoring View

1. **Navigate to Monitoring**
   - Click **"Monitoring"** in the sidebar
   - View system health and execution history

2. **Check Execution Status**
   - See recent RCA analyses
   - View performance metrics
   - Identify any errors or warnings

### Complex Queries

#### Multi-Metric Comparison
```
Compare recovery and total outstanding between system_a and system_b for active loans
```

#### Time-Based Analysis
```
Why is recovery mismatching between system_a and system_b for the last 30 days?
```

#### Filtered Analysis
```
Why is recovery different between system_a and system_b excluding writeoff loans?
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: "Cannot connect to server"

**Symptoms:**
- Error message: "Cannot connect to server. Make sure the RCA Engine server is running on http://localhost:8080"
- Data sources fail to load

**Solutions:**
1. Check if server is running:
   ```bash
   # Check if port 8080 is in use
   lsof -i :8080
   ```
2. Start the server:
   ```bash
   ./start_server.sh
   # Or
   cargo run --bin server
   ```
3. Verify server is accessible:
   - Open `http://localhost:8080/api/health` in browser
   - Should return: `{"status":"ok","service":"rca-engine-api"}`

#### Issue 2: "No data sources found"

**Symptoms:**
- Empty data sources list
- Query fails with "no tables found"

**Solutions:**
1. Add data sources (see Step 1)
2. Verify metadata directory exists:
   ```bash
   ls metadata/
   ```
3. Check if tables.json has entries:
   ```bash
   cat metadata/tables.json
   ```

#### Issue 3: Query Returns Generic Results

**Symptoms:**
- Results don't show specific mismatches
- Only generic "analysis complete" message

**Solutions:**
1. Ensure your data sources are properly configured:
   - System names match between sources
   - Primary keys are correctly set
   - Grain columns are appropriate
2. Verify your query mentions:
   - Both system names explicitly
   - The metric you're interested in
   - Any relevant filters
3. Check if data files exist:
   ```bash
   ls data/
   ls tables/
   ```

#### Issue 4: CSV Upload Fails

**Symptoms:**
- Upload button doesn't work
- File not processing

**Solutions:**
1. Verify file is valid CSV:
   - Open in Excel/text editor
   - Check for proper comma separation
   - Ensure no encoding issues
2. Check file size:
   - Very large files may take time
   - Consider sampling for testing
3. Verify required fields are filled:
   - System name
   - Entity name
   - Primary keys selected

#### Issue 5: Results Show "Use CLI for full execution"

**Symptoms:**
- Result message suggests using command line
- Limited analysis shown

**Solutions:**
1. This may indicate the query needs more complex processing
2. Try the CLI command shown in the result:
   ```bash
   cargo run --bin rca-engine run "your query" --metadata-dir ./metadata --data-dir ./data
   ```
3. Or refine your query to be more specific

---

## Best Practices

### 1. Data Source Setup

✅ **Do:**
- Use descriptive system names: `system_a`, `core_banking`, `scf_v1`
- Provide clear column descriptions
- Set appropriate primary keys
- Use consistent naming conventions across systems

❌ **Don't:**
- Use generic names: `data1`, `file1`
- Skip column descriptions
- Use non-unique columns as primary keys
- Mix naming conventions

### 2. Query Formulation

✅ **Do:**
- Be specific about systems: "system_a and system_b"
- Mention the metric: "recovery", "balance", "count"
- Include relevant filters: dates, types, statuses
- Start simple, then add complexity

❌ **Don't:**
- Use vague queries: "What's wrong?"
- Omit system names
- Over-complicate with too many filters at once
- Use technical jargon unnecessarily

### 3. Result Interpretation

✅ **Do:**
- Read the complete result, not just the summary
- Pay attention to root causes
- Review the mismatch details table
- Follow recommendations provided

❌ **Don't:**
- Ignore root cause explanations
- Skip the verification proof
- Dismiss recommendations without review
- Assume all mismatches are errors (some may be expected)

### 4. Follow-Up Analysis

✅ **Do:**
- Ask follow-up questions for clarification
- Drill down into specific rows if needed
- Verify findings with source data
- Document findings for future reference

❌ **Don't:**
- Accept results without validation
- Ignore warnings or errors
- Skip the audit trail
- Forget to download results for records

### 5. Performance Optimization

✅ **Do:**
- Use date filters to limit data scope
- Start with small date ranges for testing
- Use specific entity filters when possible
- Sample data for initial exploration

❌ **Don't:**
- Query entire datasets without filters
- Compare systems with vastly different schemas
- Ignore performance warnings
- Run multiple heavy queries simultaneously

---

## Complete Example Workflow

Here's a complete example from start to finish:

### Scenario: Compare Recovery Between Two Systems

1. **Setup (One-time)**
   - Upload `loans_system_a.csv`:
     - System: `system_a`
     - Entity: `loan`
     - Primary Key: `uuid`
   - Upload `loans_system_b.csv`:
     - System: `system_b`
     - Entity: `loan`
     - Primary Key: `uuid`

2. **Navigate to Reasoning**
   - Click "Reasoning" in sidebar

3. **Submit Query**
   - Type: `Why is recovery mismatching between system_a and system_b for Digital loans on 2026-01-08?`
   - Click Send

4. **Review Analysis**
   - Watch thought steps: "Analyzing query..."
   - Watch action steps: "Querying data sources..."
   - Review result step with:
     - Root causes
     - Mismatch details table
     - Recommendations

5. **Interpret Results**
   - See that 50 rows are missing in system_b
   - Root cause: Failed joins on order_id
   - Recommendation: Check if orders exist in system_b

6. **Follow Up** (Optional)
   - Ask: `Show me the specific UUIDs that are missing in system_b`
   - Get detailed list of affected rows

7. **Take Action**
   - Download CSV of mismatches
   - Investigate the root cause (missing orders)
   - Fix data sync issue
   - Re-run query to verify fix

---

## Hypergraph Visualization

The RCA Engine includes an **interactive hypergraph visualizer** that provides a visual representation of your data landscape, showing all tables, their relationships (joins), and metadata in an intuitive graph interface.

### What is the Hypergraph Visualizer?

The hypergraph visualizer is a powerful tool that helps you:

- **Visualize Table Relationships**: See how all your tables are connected through join relationships
- **Explore Schema Clustering**: Tables are automatically grouped and color-coded by schema/system
- **Inspect Metadata**: View row counts, columns, and other metadata for each table
- **Find Join Paths**: Discover how data flows between different tables
- **Search and Navigate**: Quickly find specific tables and explore their connections

### Accessing the Visualizer

#### Option 1: Standalone Frontend

The hypergraph visualizer has its own React-based frontend located in `hypergraph-visualizer/frontend/`:

1. **Start the RCA Engine Server** (if not already running):
   ```bash
   cd /path/to/RCA-ENGINE
   cargo run --bin server
   ```
   The server will run on `http://localhost:8080` and provide the `/api/graph` endpoint.

2. **Start the Visualizer Frontend**:
   ```bash
   cd hypergraph-visualizer/frontend
   npm install  # First time only
   npm run dev
   ```
   The frontend will start on `http://localhost:5173` (or similar).

3. **Open in Browser**: Navigate to the Vite dev server URL (e.g., `http://localhost:5173`)

#### Option 2: Integrated UI

The visualizer can also be integrated directly into the main RCA Engine UI by importing the `HypergraphVisualizer` component.

### Using the Visualizer

#### 1. Initial View

When you open the visualizer, you'll see:

- **Graph Canvas**: A dark-themed canvas showing all tables as circular nodes
- **Color-Coded Nodes**: Each schema/system has a distinct color
- **Join Edges**: Lines connecting tables that have join relationships
- **Schema Legend**: A sidebar showing the color mapping for each schema
- **Search Bar**: At the top for quick table lookup
- **Stats Summary**: Total tables, columns, and join relationships

#### 2. Interacting with the Graph

**Navigation:**
- **Zoom**: Use mouse wheel to zoom in/out
- **Pan**: Click and drag on empty space to pan the view
- **Drag Nodes**: Click and drag individual nodes to rearrange the layout

**Node Interactions:**
- **Hover**: Hover over a node to see a popup with table details
  - Table name
  - Row count
  - List of columns
- **Click**: Click a node to:
  - Highlight the node and its connections
  - Dim unrelated nodes and edges
  - Lock the info panel open
- **Click Empty Space**: Click on the canvas background to clear selection and restore full view

**Edge Interactions:**
- **Hover over Edges**: See the join condition (e.g., `table1.id = table2.id`)
- **Connected Edges**: When you select a node, its edges are highlighted in bright colors

#### 3. Searching for Tables

Use the search bar at the top to quickly find specific tables:

1. **Type** the table name (or part of it) in the search box
2. **Partial Match**: The search works with partial names (e.g., "customer" finds "customer_master_a")
3. **Auto-Focus**: The graph automatically zooms to the matching table and highlights it
4. **Clear Search**: Click the 'X' icon in the search bar to clear the search and reset the view

#### 4. Understanding the Visual Elements

**Node Colors:**
- Nodes are colored by schema/system (e.g., `system_a`, `system_b`)
- Each schema gets a unique, vibrant color from a predefined palette
- Check the **Schema Legend** panel on the right to see the color mapping

**Edge Styles:**
- **Bright Colors**: Edges use green, orange, purple, and pink for easy visibility
- **Highlighting**: When a node is selected, its edges glow and thicken
- **Bidirectional**: All joins are shown as bidirectional (undirected edges)

**Node Size:**
- Node size adjusts based on the label length for better readability
- Larger labels get slightly bigger nodes

#### 5. Reading the Info Panel

When you hover or click on a node, an info panel appears showing:

- **Table Name**: Full table name (e.g., `customer_master_a`)
- **Row Count**: Number of rows in the table (if available)
- **Columns**: A scrollable list of all columns in the table
- **System/Schema**: Implicit from the node color (see legend)

The panel:
- **Auto-positions** near the node to avoid overlapping
- **Stays visible** when you move your mouse over it
- **Locks** when you click a node (close by clicking the X or clicking empty space)

#### 6. Schema Clustering

Tables are automatically clustered by schema for better organization:

- **Visual Grouping**: Tables from the same schema/system are positioned close together
- **Color Consistency**: All tables in a schema share the same color
- **Legend Reference**: Use the schema legend to quickly identify which system each node belongs to

### Example Workflow

**Scenario**: You want to understand how `customer_master_a` connects to `customer_transactions_a`.

1. **Open the Visualizer**: Start the frontend and open it in your browser
2. **Search for "customer_master"**: Type in the search bar
3. **View Highlighted Node**: The graph zooms to `customer_master_a` and highlights it
4. **Inspect Connections**: Look at the glowing edges - you'll see connections to:
   - `customer_accounts_a`
   - `customer_transactions_a`
   - `customer_summary_a`
5. **Hover Over Edge**: Hover over the edge connecting to `customer_transactions_a` to see:
   - Join condition: `customer_master_a.customer_id = customer_transactions_a.customer_id`
6. **Click to Lock**: Click `customer_transactions_a` to see its columns and details
7. **Explore Further**: Notice it also connects to other tables, showing the full data lineage

### Visualizer Architecture

The hypergraph visualizer is built with:

- **Frontend**: React + TypeScript + Material-UI + vis-network
- **Backend**: Rust (RCA Engine server provides `/api/graph` endpoint)
- **Data Format**: JSON with nodes, edges, and stats

#### API Endpoint: `/api/graph`

The visualizer consumes data from the `/api/graph` endpoint, which returns:

```json
{
  "nodes": [
    {
      "id": "table_name",
      "label": "table_name",
      "type": "table",
      "row_count": 1000,
      "columns": ["col1", "col2", "col3"],
      "title": "table_name - entity"
    }
  ],
  "edges": [
    {
      "id": "edge_1",
      "from": "table_1",
      "to": "table_2",
      "label": "table_1.col1 = table_2.col2",
      "relationship": "one-to-many"
    }
  ],
  "stats": {
    "total_nodes": 10,
    "total_edges": 8,
    "table_count": 10,
    "column_count": 50
  }
}
```

This data is generated from your `metadata/tables.json` and `metadata/lineage.json` files.

### Customization

You can customize the visualizer by:

1. **Modifying Color Palette**: Edit the `colorPalette` array in `HypergraphVisualizer.tsx`
2. **Adjusting Layout**: Modify the `physics` settings in the vis-network options
3. **Changing Node Styles**: Update node shape, size, or fonts in the component
4. **Adding Metadata**: Extend the `/api/graph` endpoint to include more metadata

### Troubleshooting the Visualizer

**Problem: Visualizer shows "No graph data available"**

- **Solution**: 
  - Ensure `metadata/tables.json` and `metadata/lineage.json` exist
  - Check that the RCA Engine server is running on `http://localhost:8080`
  - Verify `/api/graph` endpoint returns valid JSON: `curl http://localhost:8080/api/graph`

**Problem: Edges are not showing**

- **Solution**: 
  - Verify `metadata/lineage.json` has valid edges
  - Check that edge `from` and `to` values match actual table names in `tables.json`
  - Look for console errors in the browser developer tools

**Problem: Frontend can't connect to backend**

- **Solution**:
  - Ensure RCA Engine server is running on port 8080
  - Check CORS settings if running on different ports
  - Set `VITE_API_BASE_URL` environment variable if using a custom API URL

**Problem: Graph layout looks cramped**

- **Solution**:
  - Zoom out using the mouse wheel
  - Manually drag nodes to spread them out (physics is disabled after initial layout)
  - Adjust `springLength` and `gravitationalConstant` in the physics configuration

### Benefits of the Visualizer

1. **Quick Overview**: See your entire data landscape at a glance
2. **Lineage Exploration**: Understand how tables relate to each other
3. **Debugging Aid**: Identify missing joins or isolated tables
4. **Documentation**: Visual representation serves as living documentation
5. **Onboarding**: Help new team members understand the data model quickly

---

## Summary

Performing an RCA in the RCA Engine involves:

1. ✅ **Setup**: Add your data sources (CSV files)
2. ✅ **Query**: Ask a natural language question
3. ✅ **Analyze**: Review the step-by-step analysis
4. ✅ **Interpret**: Understand root causes and mismatches
5. ✅ **Act**: Follow recommendations and fix issues
6. ✅ **Verify**: Re-run query to confirm resolution
7. ✅ **Visualize**: Use the hypergraph visualizer to explore data relationships

The system handles the complex work of:
- Parsing your natural language query
- Identifying relevant systems and metrics
- Comparing data at row level
- Tracing data transformations
- Finding root causes
- Providing actionable recommendations

You just need to ask the right question!

---

## Additional Resources

- **Project Documentation**: See `PROJECT_DOCUMENTATION.md` for technical details
- **API Documentation**: Check server endpoints in `src/bin/server.rs`
- **Metadata Structure**: Review `metadata/` directory for configuration examples

---

*Last Updated: 2026-01-18*
*Version: 1.1 - Added Hypergraph Visualization Module*

