# RCA Engine - Complete System Overview

**End-to-End Architecture, Pipelines, and Component Documentation**

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Four-Plane Architecture](#four-plane-architecture)
3. [End-to-End Query Pipeline](#end-to-end-query-pipeline)
4. [Component Breakdown](#component-breakdown)
5. [API Endpoints](#api-endpoints)
6. [Frontend Architecture](#frontend-architecture)
7. [Data Flow](#data-flow)
8. [Key Pipelines](#key-pipelines)
9. [Integration Points](#integration-points)

---

## System Architecture

### High-Level Overview

RCA Engine is a **Natural Language to SQL Query Engine** that converts user queries into optimized SQL queries using AI agents, metadata, and business rules. The system follows a **four-plane architecture** with clear separation of concerns.

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend (React/TypeScript)              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Notebook │  │ Reasoning│  │ Metadata │  │ Knowledge│   │
│  │   UI     │  │   Chat   │  │ Register │  │ Register │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼─────────────┼──────────────┼──────────────┼─────────┘
        │             │              │              │
        └─────────────┴──────────────┴──────────────┘
                          │ HTTP/REST API
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Backend (Python Flask + Rust Core)              │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Ingress Plane│  │ Planning     │  │ Execution    │      │
│  │              │  │ Plane        │  │ Plane        │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │               │
│         ▼                  ▼                  ▼               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         Agentic Orchestration System                 │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐      │   │
│  │  │ Intent │ │ Metric │ │ Table  │ │ Filter │      │   │
│  │  │ Agent  │ │ Agent  │ │ Agent  │ │ Agent  │      │   │
│  │  └────────┘ └────────┘ └────────┘ └────────┘      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         Execution Engines (Rust Core)                  │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐      │   │
│  │  │ DuckDB │ │ Trino  │ │ Polars │ │ Postgres│      │   │
│  │  └────────┘ └────────┘ └────────┘ └────────┘      │   │
│  └──────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Sources & Metadata                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │Database  │  │Metadata   │  │Knowledge │  │Business  │   │
│  │Connector │  │Registry   │  │Base      │  │Rules     │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Four-Plane Architecture

The system is organized into four distinct planes, each with a single responsibility:

### 1. Ingress Plane (`backend/planes/ingress.py`)

**Responsibility**: Request validation, authentication, rate limiting

**Components**:
- **Request Validation**: Validates incoming requests (JSON schema, required fields)
- **Authentication**: API key validation, user identification
- **Rate Limiting**: Token bucket algorithm (60 req/min, 1000 req/hour)
- **CORS Handling**: Cross-origin resource sharing configuration
- **Correlation IDs**: Generates unique request IDs for tracing

**Key Files**:
- `backend/auth/rate_limiter.py` - Rate limiting implementation
- `backend/auth/middleware.py` - Authentication middleware
- `backend/app_production.py` - Request/response middleware

**Flow**:
```
HTTP Request → CORS Check → Rate Limiter → Auth Check → Request Validation → Planning Plane
```

### 2. Planning Plane (`backend/planes/planning.py`)

**Responsibility**: Intent extraction, SQL generation, clarification

**Components**:
- **Intent Extraction** (`backend/planning/intent_extractor.py`): Extracts query intent from natural language
- **Clarification Agent** (`backend/planning/clarification_agent.py`): Detects ambiguities and asks questions
- **Multi-Step Planner** (`backend/planning/multi_step_planner.py`): Orchestrates planning stages
- **Schema Selector** (`backend/planning/schema_selector.py`): Selects relevant tables/schemas
- **Metric Resolver** (`backend/planning/metric_resolver.py`): Resolves business metrics
- **Query Builder** (`backend/planning/query_builder.py`): Builds SQL query structure
- **Validator** (`backend/planning/validator.py`): Validates query correctness

**Key Pipeline**:
```
User Query → Intent Extraction → Schema Selection → Metric Resolution → Query Building → Validation → SQL
```

**Agentic System** (`backend/agentic/orchestrator.py`):
- **Intent Agent**: Classifies query type and extracts requested metrics
- **Metric Agent**: Resolves metrics against semantic registry (CRITICAL - fails if unresolved)
- **Table Agent**: Confirms base table selection
- **Filter Agent**: Generates filters from business rules
- **Shape Agent**: Determines query presentation (aggregation, grouping)
- **Verifier Agent**: Validates query correctness
- **SQL Renderer**: Generates final SQL

### 3. Execution Plane (`backend/planes/execution.py`)

**Responsibility**: Query execution, engine selection

**Components**:
- **Engine Router** (`rust/execution/router.rs`): Routes queries to appropriate engine
- **DuckDB Engine** (`rust/execution/duckdb_engine.rs`): In-memory analytical database
- **Trino Engine** (`rust/execution/trino_engine.rs`): Distributed SQL query engine
- **Polars Engine** (`rust/execution/polars_engine.rs`): DataFrame processing engine
- **Query Firewall** (`backend/execution/query_firewall.py`): Security checks before execution
- **Sandbox** (`backend/execution/sandbox.py`): Sandboxed execution environment

**Execution Flow**:
```
SQL Query → Query Firewall → Engine Selection → Execution → Result Processing → Presentation Plane
```

### 4. Presentation Plane (`backend/planes/presentation.py`)

**Responsibility**: Result formatting, explanation generation

**Components**:
- **Result Formatter** (`backend/presentation/formatter.py`): Formats query results
- **Query Explainer** (`backend/presentation/explainer.py`): Generates query explanations
- **Reasoning Steps** (`backend/presentation/reasoning.py`): Tracks reasoning chain

**Output**:
- Formatted results (JSON, CSV, table)
- Query explanation
- Reasoning steps for transparency

---

## End-to-End Query Pipeline

### Complete Flow: Natural Language → SQL → Results

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. USER INPUT (Frontend)                                        │
│    User types: "Show me top 10 customers by revenue"            │
│    Component: NotebookCell → AIChatPanel                        │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. API REQUEST (Frontend → Backend)                             │
│    POST /api/v1/notebook/{notebook_id}/cells/{cell_id}/generate │
│    Body: { "query": "Show me top 10 customers by revenue" }     │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. INGRESS PLANE                                                │
│    ├─ CORS Check                                                │
│    ├─ Rate Limiting (Token Bucket)                             │
│    ├─ Authentication (API Key/User ID)                          │
│    ├─ Request Validation                                        │
│    └─ Correlation ID Generation                                 │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. PLANNING PLANE - Multi-Step Planning                         │
│                                                                  │
│    Step 1: Intent Extraction                                     │
│    ├─ Extract query type: "analytical"                          │
│    ├─ Extract metrics: ["revenue"]                              │
│    ├─ Extract dimensions: ["customer"]                           │
│    └─ Extract filters: ["top 10"]                               │
│                                                                  │
│    Step 2: Clarification Check                                  │
│    ├─ Detect ambiguities                                        │
│    ├─ If ambiguous → Ask clarifying questions                    │
│    └─ If clear → Proceed                                        │
│                                                                  │
│    Step 3: Schema Selection                                     │
│    ├─ Load metadata registry                                     │
│    ├─ Match tables: ["customers", "orders", "revenue"]          │
│    └─ Select base table: "customers"                            │
│                                                                  │
│    Step 4: Metric Resolution (CRITICAL)                         │
│    ├─ Query semantic registry                                    │
│    ├─ Resolve "revenue" → "SUM(order_amount)"                   │
│    └─ If unresolved → FAIL (Golden Invariant)                   │
│                                                                  │
│    Step 5: Query Building                                       │
│    ├─ Build SELECT clause                                       │
│    ├─ Build FROM clause                                         │
│    ├─ Build JOIN clauses (from metadata)                         │
│    ├─ Build WHERE clauses (from business rules)                  │
│    ├─ Build GROUP BY clause                                     │
│    └─ Build ORDER BY + LIMIT                                     │
│                                                                  │
│    Step 6: Validation                                           │
│    ├─ SQL syntax validation                                     │
│    ├─ Table/column existence check                              │
│    └─ Business rule compliance                                  │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. AGENTIC ORCHESTRATION (Alternative Path)                      │
│                                                                  │
│    AgenticSQLOrchestrator chains agents:                         │
│                                                                  │
│    Intent Agent → Metric Agent → Table Agent →                  │
│    Filter Agent → Shape Agent → Verifier Agent →                │
│    SQL Renderer                                                  │
│                                                                  │
│    Each agent:                                                   │
│    ├─ Receives context from previous agent                       │
│    ├─ Uses LLM for reasoning                                    │
│    ├─ Queries metadata/knowledge base                           │
│    └─ Produces structured output                                │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. EXECUTION PLANE                                              │
│                                                                  │
│    Query Firewall:                                               │
│    ├─ Check for dangerous operations                             │
│    ├─ Validate resource limits                                  │
│    └─ Check query complexity                                    │
│                                                                  │
│    Engine Selection:                                             │
│    ├─ Analyze query characteristics                             │
│    ├─ Check data source type                                    │
│    └─ Route to: DuckDB / Trino / Polars / PostgreSQL           │
│                                                                  │
│    Execution:                                                    │
│    ├─ Connect to database                                       │
│    ├─ Execute SQL                                               │
│    ├─ Stream results                                            │
│    └─ Handle errors gracefully                                  │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. PRESENTATION PLANE                                            │
│                                                                  │
│    Result Processing:                                            │
│    ├─ Format results (JSON/CSV/Table)                            │
│    ├─ Generate query explanation                                │
│    ├─ Extract reasoning steps                                   │
│    └─ Add metadata (execution time, row count)                  │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 8. API RESPONSE (Backend → Frontend)                            │
│    {                                                             │
│      "success": true,                                            │
│      "sql": "SELECT c.customer_id, SUM(o.amount) as revenue...", │
│      "reasoning_steps": [...],                                   │
│      "explanation": "Query retrieves top 10 customers..."       │
│    }                                                             │
└────────────────────┬────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│ 9. FRONTEND DISPLAY                                             │
│    ├─ Update NotebookCell with generated SQL                    │
│    ├─ Show reasoning steps in AIChatPanel                        │
│    └─ User can execute or modify                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Breakdown

### Backend Components

#### 1. API Layer (`backend/api/`)

**Purpose**: REST API endpoints for all functionality

**Key Endpoints**:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/health` | GET | Health check |
| `/api/v1/metrics` | GET | System metrics |
| `/api/query` | POST | Process natural language query |
| `/api/agent/run` | POST | Run agentic query |
| `/api/reasoning/assess` | POST | Assess query complexity |
| `/api/reasoning/clarify` | POST | Clarify ambiguous query |
| `/api/reasoning/query` | POST | Execute reasoning query |
| `/api/v1/notebook` | GET/POST/PUT | Notebook CRUD operations |
| `/api/v1/notebook/{id}/cells/{cell_id}/execute` | POST | Execute notebook cell |
| `/api/v1/notebook/{id}/cells/{cell_id}/generate` | POST | Generate SQL for cell |
| `/api/metadata/ingest/table` | POST | Register table metadata |
| `/api/metadata/ingest/metric` | POST | Register metric metadata |
| `/api/knowledge/register` | POST | Register business knowledge |

**Files**:
- `api/query.py` - Main query endpoints
- `api/notebook.py` - Notebook API (cell-based SQL composition)
- `api/clarification.py` - Clarification endpoints
- `api/ingestion.py` - Metadata ingestion
- `api/health.py` - Health checks
- `api/metrics.py` - Metrics endpoints

#### 2. Agentic System (`backend/agentic/`)

**Purpose**: Multi-agent system for query decomposition

**Agents**:

1. **Intent Agent** (`intent_agent.py`)
   - Classifies query type (analytical, exploratory, comparison)
   - Extracts requested metrics and dimensions
   - Identifies filters and constraints

2. **Metric Agent** (`metric_agent.py`) - **CRITICAL**
   - Resolves business metrics against semantic registry
   - **Golden Invariant**: If metric unresolved → system refuses to answer
   - Maps natural language metrics to SQL expressions

3. **Table Agent** (`table_agent.py`)
   - Selects base table(s) for query
   - Uses metadata registry and RAG for table discovery
   - Validates table existence

4. **Filter Agent** (`filter_agent.py`)
   - Generates WHERE clauses from business rules
   - Applies time-based filters (if needed)
   - Applies data quality filters

5. **Shape Agent** (`shape_agent.py`)
   - Determines query presentation shape
   - Decides aggregation level
   - Determines grouping and ordering

6. **Verifier Agent** (`verifier_agent.py`)
   - Validates SQL correctness
   - Checks against metadata
   - Ensures business rule compliance

7. **SQL Renderer** (`sql_renderer.py`)
   - Generates final SQL from agent outputs
   - Optimizes query structure
   - Handles dialect differences

**Orchestrator** (`orchestrator.py`):
- Chains agents in sequence
- Manages context passing between agents
- Handles failures and retries
- Tracks execution steps for debugging

#### 3. Planning System (`backend/planning/`)

**Purpose**: Multi-stage query planning

**Components**:

- **Multi-Step Planner** (`multi_step_planner.py`): Orchestrates planning stages
- **Intent Extractor** (`intent_extractor.py`): Extracts query intent
- **Clarification Agent** (`clarification_agent.py`): Proactive clarification
- **Schema Selector** (`schema_selector.py`): Selects relevant schemas
- **Metric Resolver** (`metric_resolver.py`): Resolves metrics
- **Query Builder** (`query_builder.py`): Builds SQL structure
- **Validator** (`validator.py`): Validates queries
- **Guardrails** (`guardrails.py`): Planning safety checks

**Planning Stages**:
1. Intent Extraction
2. Clarification (if needed)
3. Schema Selection
4. Metric Resolution
5. Query Building
6. Validation

#### 4. Execution System (`backend/execution/` + `rust/execution/`)

**Purpose**: Query execution with multiple engines

**Engines**:
- **DuckDB** (`duckdb_engine.rs`): Fast in-memory analytical queries
- **Trino** (`trino_engine.rs`): Distributed SQL queries
- **Polars** (`polars_engine.rs`): DataFrame processing
- **PostgreSQL/MySQL**: Traditional databases

**Components**:
- **Router** (`router.rs`): Routes queries to appropriate engine
- **Query Firewall** (`query_firewall.py`): Security checks
- **Sandbox** (`sandbox.py`): Sandboxed execution
- **Kill Switch** (`kill_switch.py`): Emergency query termination

#### 5. Metadata System (`backend/stores/`)

**Purpose**: Metadata storage and retrieval

**Components**:
- **Table Store** (`table_store.py`): Table metadata storage
- **DB Connection** (`db_connection.py`): Database connection management
- **Contract Store** (`contract_store.py`): Query contract storage

**Metadata Structure**:
- Tables: Name, columns, types, relationships
- Metrics: Name, SQL expression, dimensions, filters
- Dimensions: Name, description, grain
- Joins: From table, to table, join condition
- Business Rules: Filters, validations, constraints

#### 6. Knowledge System (`backend/rag/` + `components/KnowledgeBase/`)

**Purpose**: Business knowledge retrieval

**Components**:
- **RAG Retrieval** (`rag/retrieval.py`): Retrieval-augmented generation
- **Knowledge Base** (`components/KnowledgeBase/`): Vector store for knowledge
- **Hybrid Retriever** (`hybrid_knowledge_retriever.py`): Combines multiple retrieval methods

**Knowledge Types**:
- Business terms and definitions
- Domain-specific rules
- Data quality rules
- Business logic

#### 7. Observability (`backend/observability/`)

**Purpose**: Logging, metrics, tracing

**Components**:
- **Structured Logging** (`structured_logging.py`): JSON-formatted logs
- **Golden Signals** (`golden_signals.py`): Latency, errors, throughput, saturation
- **Metrics** (`metrics.py`): Prometheus metrics
- **Correlation IDs** (`correlation.py`): Request tracing

**Metrics Tracked**:
- Request latency (p50, p95, p99)
- Error rates
- Throughput (requests/second)
- LLM API calls and costs
- Query execution times
- Clarification usage

#### 8. Security (`backend/security/`)

**Purpose**: Security features

**Components**:
- **Prompt Injection Prevention** (`prompt_injection.py`): Detects and prevents prompt injection
- **Data Exfiltration Prevention** (`data_exfiltration.py`): Prevents unauthorized data access
- **Query Firewall** (`execution/query_firewall.py`): Validates queries before execution

### Frontend Components

#### 1. Main Application (`frontend/src/App.tsx`)

**Purpose**: Root application component

**Features**:
- Theme provider (dark theme with pink accents)
- View mode routing
- Sidebar navigation
- Top bar

#### 2. Notebook Interface (`frontend/src/components/TrinoNotebook.tsx`)

**Purpose**: Cell-based SQL notebook (primary interface)

**Features**:
- Multiple cells (like Jupyter)
- AI-powered SQL generation per cell
- Cell execution
- Results display
- Error handling

**Flow**:
```
User clicks "Ask AI" → AIChatPanel opens → User enters query → 
API call → SQL generated → Cell updated → User executes → Results shown
```

#### 3. AI Chat Panel (`frontend/src/components/AIChatPanel.tsx`)

**Purpose**: AI assistant for SQL generation

**Features**:
- Natural language input
- SQL generation
- Reasoning steps display
- Error handling

#### 4. Notebook Cell (`frontend/src/components/NotebookCell.tsx`)

**Purpose**: Individual notebook cell component

**Features**:
- SQL editor (CodeMirror)
- Run button
- AI button
- Status indicators (running, success, error)
- Results table
- Error display

#### 5. Reasoning Chat (`frontend/src/components/ReasoningChat.tsx`)

**Purpose**: Advanced reasoning interface

**Features**:
- Multi-step reasoning display
- Completeness tracking
- Query assessment
- Clarification handling

#### 6. Metadata Register (`frontend/src/components/MetadataRegister.tsx`)

**Purpose**: Register and manage metadata

**Features**:
- Table registration
- Metric registration
- Search and filter
- Table display

#### 7. Knowledge Register (`frontend/src/components/KnowledgeRegister.tsx`)

**Purpose**: Register business knowledge

**Features**:
- Knowledge entry creation
- Search functionality
- Tag management

#### 8. API Client (`frontend/src/api/client.ts`)

**Purpose**: Frontend API communication

**Features**:
- Axios-based HTTP client
- Request/response interceptors
- Error handling
- CORS error detection
- TypeScript types for all APIs

**API Modules**:
- `healthAPI` - Health checks
- `metricsAPI` - Metrics
- `agentAPI` - Agent queries
- `reasoningAPI` - Reasoning queries
- `notebookAPI` - Notebook operations
- `queryAPI` - Query generation
- `metadataAPI` - Metadata operations
- `knowledgeAPI` - Knowledge operations

---

## API Endpoints

### Health & Metrics

```
GET  /api/v1/health              - Basic health check
GET  /api/v1/health/detailed     - Detailed health check
GET  /api/v1/health/ready        - Readiness check
GET  /api/v1/metrics             - System metrics
```

### Query Endpoints

```
POST /api/query                  - Process natural language query
POST /api/query/batch            - Process multiple queries
POST /api/query/preview          - Preview query without execution
```

### Agent Endpoints

```
POST /api/agent/run              - Run agentic query
POST /api/agent/continue         - Continue agent session
```

### Reasoning Endpoints

```
POST /api/reasoning/assess       - Assess query complexity
POST /api/reasoning/clarify      - Clarify ambiguous query
POST /api/reasoning/query        - Execute reasoning query
```

### Notebook Endpoints

```
GET    /api/v1/notebook/{id}                    - Get notebook
POST   /api/v1/notebook                         - Create notebook
PUT    /api/v1/notebook/{id}                    - Update notebook
DELETE /api/v1/notebook/{id}                    - Delete notebook
POST   /api/v1/notebook/{id}/cells/{cell_id}/execute  - Execute cell
POST   /api/v1/notebook/{id}/cells/{cell_id}/generate - Generate SQL
```

### Metadata Endpoints

```
POST /api/metadata/ingest/table  - Register table metadata
POST /api/metadata/ingest/metric - Register metric metadata
GET  /api/metadata/tables        - List tables
GET  /api/metadata/metrics       - List metrics
GET  /api/metadata/prerequisites - Get metadata prerequisites
```

### Knowledge Endpoints

```
POST /api/knowledge/register     - Register business knowledge
GET  /api/knowledge/search       - Search knowledge base
GET  /api/knowledge/entries      - List knowledge entries
```

### Clarification Endpoints

```
POST /api/clarification/analyze  - Analyze query for ambiguities
POST /api/clarification/resolve  - Resolve clarification answers
```

---

## Frontend Architecture

### Component Hierarchy

```
App.tsx
├── TopBar.tsx
├── Sidebar.tsx
└── Content Area (View Mode Router)
    ├── TrinoNotebook.tsx (Default)
    │   ├── NotebookCell.tsx (Multiple)
    │   │   ├── CodeMirror Editor
    │   │   ├── Run Button
    │   │   ├── AI Button
    │   │   ├── Results Table
    │   │   └── Error Display
    │   └── AIChatPanel.tsx (When AI open)
    │       ├── Chat Input
    │       └── Reasoning Steps
    ├── ReasoningChat.tsx
    ├── MetadataRegister.tsx
    ├── KnowledgeRegister.tsx
    ├── Monitoring.tsx
    ├── PipelineManager.tsx
    └── RulesView.tsx
```

### State Management

**Zustand Store** (`frontend/src/store/useStore.ts`):
- `viewMode`: Current view ('notebook', 'reasoning', 'metadata-register', etc.)
- `sidebarOpen`: Sidebar visibility
- `sidebarWidth`: Sidebar width
- `reasoningSteps`: Reasoning history

### Theme System

**Theme** (`frontend/src/theme.ts`):
- Dark theme with pink accents (`#ff096c`)
- Color palette:
  - Background: `#22292f`
  - Cards: `#2a3843`
  - Borders: `#4f6172`
  - Accent: `#ff096c`

**CodeMirror Theme** (`frontend/src/components/colabTheme.ts`):
- Dark editor theme
- Pink syntax highlighting
- Rim highlights on focus

---

## Data Flow

### 1. Metadata Flow

```
User Registers Metadata
    ↓
POST /api/metadata/ingest/table
    ↓
Backend Stores in Database
    ↓
Metadata Available for Query Planning
    ↓
Used by Schema Selector, Table Agent, Metric Agent
```

### 2. Query Flow

```
User Query (Natural Language)
    ↓
Frontend: AIChatPanel → notebookAPI.generateSQL()
    ↓
Backend: /api/v1/notebook/{id}/cells/{cell_id}/generate
    ↓
Planning Plane: Multi-Step Planner or Agentic Orchestrator
    ↓
SQL Generated
    ↓
Frontend: Cell Updated with SQL
    ↓
User Clicks Run
    ↓
Frontend: notebookAPI.execute()
    ↓
Backend: /api/v1/notebook/{id}/cells/{cell_id}/execute
    ↓
Execution Plane: Engine Router → DuckDB/Trino/Polars
    ↓
Results Returned
    ↓
Frontend: Results Displayed in Cell
```

### 3. Knowledge Flow

```
User Registers Knowledge
    ↓
POST /api/knowledge/register
    ↓
Backend: Stores in Knowledge Base (Vector DB)
    ↓
RAG Retrieval: Queries Knowledge Base during Planning
    ↓
Knowledge Used in Query Building
```

### 4. Clarification Flow

```
User Query (Ambiguous)
    ↓
Planning Plane: Clarification Agent Detects Ambiguity
    ↓
Response: { needs_clarification: true, questions: [...] }
    ↓
Frontend: Displays Clarification Questions
    ↓
User Answers Questions
    ↓
POST /api/clarification/resolve
    ↓
Planning Plane: Merges Answers into Intent
    ↓
Query Planning Continues
```

---

## Key Pipelines

### Pipeline 1: Natural Language to SQL (Primary)

**Entry Point**: Notebook Cell AI Chat

**Steps**:
1. User enters natural language query in AIChatPanel
2. Frontend calls `notebookAPI.generateSQL(notebookId, cellId, { query })`
3. Backend receives request at `/api/v1/notebook/{id}/cells/{cell_id}/generate`
4. Planning Plane processes query:
   - Option A: Multi-Step Planner
     - Intent Extraction
     - Schema Selection
     - Metric Resolution
     - Query Building
   - Option B: Agentic Orchestrator
     - Intent Agent
     - Metric Agent (CRITICAL)
     - Table Agent
     - Filter Agent
     - Shape Agent
     - Verifier Agent
     - SQL Renderer
5. SQL generated and returned
6. Frontend updates NotebookCell with SQL
7. User can execute or modify

**Key Files**:
- `backend/api/notebook.py` - API endpoint
- `backend/planning/multi_step_planner.py` - Multi-step planning
- `backend/agentic/orchestrator.py` - Agentic orchestration
- `frontend/src/components/AIChatPanel.tsx` - UI component
- `frontend/src/api/client.ts` - API client

### Pipeline 2: Query Execution

**Entry Point**: Notebook Cell Run Button

**Steps**:
1. User clicks Run button in NotebookCell
2. Frontend calls `notebookAPI.execute(notebookId, { cell_id })`
3. Backend receives request at `/api/v1/notebook/{id}/cells/{cell_id}/execute`
4. Notebook compilation:
   - Extract SQL from cell
   - Resolve `%%ref` directives (cell dependencies)
   - Compile into single SQL query
5. Execution Plane:
   - Query Firewall validation
   - Engine selection (DuckDB/Trino/Polars)
   - Query execution
6. Results returned
7. Frontend displays results in cell

**Key Files**:
- `backend/api/notebook.py` - Execution endpoint
- `backend/execution/query_firewall.py` - Security checks
- `rust/execution/router.rs` - Engine routing
- `frontend/src/components/NotebookCell.tsx` - UI component

### Pipeline 3: Clarification Flow

**Entry Point**: Ambiguous Query Detection

**Steps**:
1. User query detected as ambiguous
2. Clarification Agent analyzes query
3. Questions generated
4. Response: `{ needs_clarification: true, questions: [...] }`
5. Frontend displays clarification UI
6. User answers questions
7. Answers merged into query intent
8. Query planning continues with clarified intent

**Key Files**:
- `backend/planning/clarification_agent.py` - Clarification logic
- `backend/api/clarification.py` - Clarification API
- `frontend/src/components/ReasoningChat.tsx` - Clarification UI

### Pipeline 4: Metadata Ingestion

**Entry Point**: Metadata Register UI

**Steps**:
1. User enters table/metric description
2. Frontend calls `metadataAPI.ingestTable()` or `metadataAPI.ingestMetric()`
3. Backend receives at `/api/metadata/ingest/table` or `/api/metadata/ingest/metric`
4. Metadata parsed and validated
5. Stored in database (table_store, contract_store)
6. Available for query planning

**Key Files**:
- `backend/api/ingestion.py` - Ingestion API
- `backend/stores/table_store.py` - Table storage
- `frontend/src/components/MetadataRegister.tsx` - UI component

### Pipeline 5: Knowledge Registration

**Entry Point**: Knowledge Register UI

**Steps**:
1. User creates knowledge entry
2. Frontend calls `knowledgeAPI.register()`
3. Backend receives at `/api/knowledge/register`
4. Knowledge processed and embedded
5. Stored in vector database
6. Available for RAG retrieval during query planning

**Key Files**:
- `backend/rag/retrieval.py` - RAG retrieval
- `components/KnowledgeBase/` - Knowledge base server
- `frontend/src/components/KnowledgeRegister.tsx` - UI component

---

## Integration Points

### Python ↔ Rust Integration

**Python Bindings** (`rust/python_bindings/`):
- `execution.rs` - Execution engine bindings
- `agent_decision.rs` - Agent decision bindings
- `profile.rs` - Profiling bindings

**Usage**:
```python
from rust.python_bindings import execute_query, make_agent_decision
```

### Frontend ↔ Backend Integration

**REST API**:
- All communication via HTTP/REST
- JSON request/response format
- CORS enabled for cross-origin requests
- Correlation IDs for tracing

**API Client** (`frontend/src/api/client.ts`):
- Axios-based HTTP client
- Request/response interceptors
- Error handling
- TypeScript types

### Database Integration

**Supported Databases**:
- PostgreSQL
- MySQL
- SQLite
- DuckDB (in-memory)
- Trino (distributed)
- Polars (DataFrame)

**Connection Management** (`backend/stores/db_connection.py`):
- Connection pooling
- Connection health checks
- Automatic reconnection

### LLM Integration

**LLM Providers** (`backend/interfaces/llm_provider.py`):
- OpenAI (GPT-4, GPT-3.5)
- Extensible interface for other providers

**Usage**:
- Intent extraction
- Metric resolution
- Query generation
- Clarification question generation

### Vector Database Integration

**Knowledge Base** (`components/KnowledgeBase/`):
- Vector store for embeddings
- Semantic search
- Business term resolution

---

## System Invariants

### Golden Invariants

1. **Metric Resolution Invariant**: If Metric Agent fails to resolve a metric, the system MUST refuse to answer (fail-closed)
2. **Single Responsibility**: Each compiler phase has a single responsibility and does not reinterpret earlier phases
3. **Fail Fast**: If semantic information is lost, fail fast—do not attempt recovery or inference
4. **No Heuristics**: Never add hardcoded or heuristic logic to fix planner/analyzer bugs; enforce correctness structurally

### Safety Invariants

1. **Read-Only**: System only reads data, never modifies
2. **Query Validation**: All queries validated before execution
3. **Resource Limits**: Query complexity and resource usage limited
4. **Sandboxed Execution**: Queries executed in sandboxed environment

---

## Configuration

### Environment Variables

**Backend** (`.env`):
```
RCA_HOST=0.0.0.0
RCA_PORT=8080
RCA_DB_TYPE=postgresql
RCA_DB_HOST=localhost
RCA_DB_NAME=rca_engine
RCA_DB_USER=rca_user
RCA_DB_PASSWORD=password
OPENAI_API_KEY=sk-...
RCA_LLM_MODEL=gpt-4
RCA_CORS_ORIGINS=http://localhost:5173
```

**Frontend** (`frontend/.env`):
```
VITE_API_URL=http://localhost:8080
```

### Configuration Files

- `config/config.yaml` - Main application configuration
- `config/pipeline_config.yaml` - Pipeline configuration
- `config/rag_config.yaml` - RAG configuration
- `config/prometheus.yml` - Prometheus metrics configuration

---

## Deployment

### Docker Deployment

**Docker Compose** (`docker/docker-compose.yml`):
- Backend service (Python Flask)
- Frontend service (Nginx)
- Database (PostgreSQL)
- Trino (optional)
- Prometheus (monitoring)

**Build**:
```bash
docker-compose build
docker-compose up
```

### Production Deployment

**Backend**:
- Gunicorn WSGI server
- Multiple workers
- Health checks
- Metrics endpoint

**Frontend**:
- Nginx static file server
- Production build (Vite)
- API proxy configuration

---

## Monitoring & Observability

### Metrics

**Golden Signals**:
- **Latency**: Request latency (p50, p95, p99)
- **Errors**: Error rate and types
- **Throughput**: Requests per second
- **Saturation**: Resource utilization

**Custom Metrics**:
- LLM API calls and costs
- Query execution times
- Clarification usage
- Metadata registry size

### Logging

**Structured Logging**:
- JSON format
- Correlation IDs
- Request tracing
- Error stack traces

**Log Levels**:
- ERROR: Errors and exceptions
- WARN: Warnings
- INFO: General information
- DEBUG: Debug information

### Tracing

**Correlation IDs**:
- Generated per request
- Passed through all components
- Included in logs and responses

---

## Error Handling

### Error Types

1. **Validation Errors**: Invalid input (400)
2. **Authentication Errors**: Auth failures (401)
3. **Rate Limit Errors**: Rate limit exceeded (429)
4. **Planning Errors**: Query planning failures (500)
5. **Execution Errors**: Query execution failures (500)
6. **Clarification Required**: Ambiguous query (200 with clarification flag)

### Error Recovery

1. **LLM Failures**: Retry with exponential backoff
2. **Database Failures**: Connection retry
3. **Metadata Drift**: Detect and warn
4. **Partial Results**: Return partial results if possible

---

## Security

### Security Features

1. **Rate Limiting**: Token bucket algorithm
2. **Query Firewall**: Validates queries before execution
3. **Prompt Injection Prevention**: Detects and prevents prompt injection
4. **Data Exfiltration Prevention**: Prevents unauthorized data access
5. **CORS**: Configurable CORS origins
6. **Read-Only**: System only reads data

### Security Layers

1. **Ingress Plane**: Rate limiting, authentication
2. **Planning Plane**: Query validation
3. **Execution Plane**: Query firewall, sandboxing
4. **Presentation Plane**: Result sanitization

---

## Performance Optimizations

### Backend Optimizations

1. **Connection Pooling**: Database connection reuse
2. **Caching**: Metadata and query result caching
3. **Parallel Execution**: Parallel agent execution where possible
4. **Query Optimization**: SQL query optimization

### Frontend Optimizations

1. **Code Splitting**: Lazy loading of components
2. **Memoization**: React memo for expensive components
3. **Virtual Scrolling**: For large result sets
4. **Debouncing**: API call debouncing

---

## Testing

### Test Structure

```
tests/
├── test_auth_integration.py    - Authentication tests
├── test_clarification_agent.py - Clarification tests
├── test_contract_store.py      - Contract store tests
├── test_observability.py        - Observability tests
└── test_table_store.py          - Table store tests
```

### Running Tests

```bash
python -m pytest tests/ -v
```

---

## Development Workflow

### Backend Development

1. Make changes in `backend/`
2. Run tests: `pytest tests/`
3. Start server: `python backend/app_production.py`
4. Test API endpoints

### Frontend Development

1. Make changes in `frontend/src/`
2. Start dev server: `npm run dev`
3. Hot reload enabled
4. Test in browser

### Rust Development

1. Make changes in `rust/`
2. Build: `cargo build --release`
3. Run tests: `cargo test`
4. Python bindings auto-regenerate

---

## Future Enhancements

### Planned Features

1. **Query Caching**: Cache query results
2. **Query History**: Track query history
3. **Query Sharing**: Share queries between users
4. **Advanced Visualizations**: Enhanced result visualizations
5. **Multi-User Support**: User management and permissions
6. **Query Templates**: Pre-built query templates
7. **Query Optimization**: Automatic query optimization
8. **Data Lineage**: Track data lineage

---

## Conclusion

RCA Engine is a comprehensive natural language to SQL query engine with:

- **Four-plane architecture** for clear separation of concerns
- **Multi-agent system** for intelligent query decomposition
- **Proactive clarification** for ambiguous queries
- **Multiple execution engines** for optimal performance
- **Comprehensive observability** for monitoring and debugging
- **Production-ready features** including security, rate limiting, and error handling

The system is designed to be:
- **Reliable**: Fail-fast with clear error messages
- **Secure**: Multiple security layers
- **Observable**: Comprehensive logging and metrics
- **Extensible**: Modular architecture for easy extension
- **Performant**: Optimized for speed and efficiency

---

## Quick Reference

### Key Files

**Backend**:
- `backend/app_production.py` - Main Flask application
- `backend/agentic/orchestrator.py` - Agent orchestration
- `backend/planning/multi_step_planner.py` - Multi-step planning
- `backend/api/notebook.py` - Notebook API
- `backend/api/query.py` - Query API

**Frontend**:
- `frontend/src/App.tsx` - Main app component
- `frontend/src/components/TrinoNotebook.tsx` - Notebook interface
- `frontend/src/components/AIChatPanel.tsx` - AI chat
- `frontend/src/api/client.ts` - API client

**Rust Core**:
- `rust/execution/router.rs` - Engine routing
- `rust/core/agent/rca_cursor/` - RCA cursor implementation
- `rust/python_bindings/` - Python bindings

### Key Commands

```bash
# Backend
python backend/app_production.py

# Frontend
cd frontend && npm run dev

# Rust
cargo build --release

# Docker
docker-compose up

# Tests
pytest tests/ -v
```

---

**Document Version**: 1.0  
**Last Updated**: 2026-02-01  
**Maintained By**: RCA Engine Team
