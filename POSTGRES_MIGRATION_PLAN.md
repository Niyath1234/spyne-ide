# PostgreSQL Migration Plan for RCA Engine

## Overview
This document outlines what can be migrated from file-based storage to PostgreSQL as we ramp up to production.

## Current Storage Architecture

### 1. **Metadata Storage** (JSON Files in `metadata/`)
Currently stored as JSON files:
- `tables.json` - Table definitions
- `entities.json` - Entity definitions  
- `metrics.json` - Metric definitions
- `rules.json` - Business rules
- `lineage.json` - Table relationships/joins
- `business_labels.json` - System/metric labels
- `time.json` - Time-based rules
- `identity.json` - Key mappings
- `exceptions.json` - Exception rules

**Migration Priority: HIGH**
- Benefits: Version control, concurrent access, queryability, relationships
- Schema: Normalized tables with foreign keys

### 2. **Data Storage** (CSV/Parquet Files in `data/`)
Currently stored as files:
- CSV files (e.g., `scf_v1.csv`, `scf_v2.csv`)
- Parquet files (e.g., `outstanding_daily.parquet`)

**Migration Priority: HIGH**
- Benefits: ACID transactions, concurrent queries, indexing, joins
- Schema: One table per data source, partitioned by system/entity

### 3. **Knowledge Base** (JSON File)
Currently: `metadata/knowledge_base.json`
- Terms dictionary
- Table relationships
- Business concepts

**Migration Priority: MEDIUM**
- Benefits: Full-text search, semantic queries, versioning
- Schema: Normalized tables with JSONB for flexible metadata

### 4. **Graph Traversal State** (In-Memory)
Currently: Ephemeral, lost on restart
- `TraversalState` - Current traversal progress
- Visited nodes, findings, hypotheses
- Query execution history

**Migration Priority: HIGH**
- Benefits: Resume interrupted analyses, audit trail, debugging
- Schema: State tables with JSONB for flexible state storage

### 5. **RCA Query History & Results** (Not Persisted)
Currently: Not stored anywhere
- User queries
- Analysis results
- Root cause findings
- Comparison results

**Migration Priority: HIGH**
- Benefits: Historical analysis, pattern detection, audit trail
- Schema: Query log, results, findings tables

### 6. **Clarification Sessions** (In-Memory)
Currently: Ephemeral during request
- Original queries
- Clarification questions
- User answers
- Final compiled intents

**Migration Priority: MEDIUM**
- Benefits: Learning from user feedback, improving clarification logic
- Schema: Session, questions, answers tables

### 7. **Table Upload State** (In-Memory)
Currently: Ephemeral
- Upload progress
- Schema inference results
- Validation results

**Migration Priority: LOW**
- Benefits: Resume failed uploads, audit trail
- Schema: Upload jobs table

## Recommended Migration Phases

### Phase 1: Core Metadata (Week 1-2)
**Goal**: Move all metadata from JSON to PostgreSQL

**Tables to Create**:
```sql
-- Core entities
CREATE TABLE entities (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    grain JSONB, -- Array of grain columns
    attributes JSONB, -- Array of attributes
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE tables (
    name VARCHAR PRIMARY KEY,
    entity_id VARCHAR REFERENCES entities(id),
    primary_key JSONB, -- Array of primary key columns
    time_column VARCHAR,
    system VARCHAR NOT NULL,
    path VARCHAR, -- Keep for backward compatibility during migration
    columns JSONB, -- Array of ColumnMetadata
    labels JSONB, -- Array of labels
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE metrics (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    grain JSONB,
    precision INTEGER,
    null_policy VARCHAR,
    unit VARCHAR,
    versions JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE rules (
    id VARCHAR PRIMARY KEY,
    system VARCHAR NOT NULL,
    metric_id VARCHAR REFERENCES metrics(id),
    target_entity_id VARCHAR REFERENCES entities(id),
    target_grain JSONB,
    description TEXT,
    formula TEXT,
    source_entities JSONB,
    aggregation_grain JSONB,
    filter_conditions JSONB,
    source_table VARCHAR,
    note TEXT,
    labels JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE lineage_edges (
    id SERIAL PRIMARY KEY,
    from_table VARCHAR REFERENCES tables(name),
    to_table VARCHAR REFERENCES tables(name),
    keys JSONB, -- Map of join keys
    relationship VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE business_labels (
    id SERIAL PRIMARY KEY,
    label_type VARCHAR NOT NULL, -- 'system', 'metric', 'reconciliation_type'
    label VARCHAR NOT NULL,
    aliases JSONB,
    system_id VARCHAR, -- For system labels
    metric_id VARCHAR REFERENCES metrics(id), -- For metric labels
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE time_rules (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR REFERENCES tables(name),
    rule_type VARCHAR NOT NULL, -- 'as_of' or 'lateness'
    as_of_column VARCHAR,
    default_value VARCHAR,
    max_lateness_days INTEGER,
    action VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE identity_mappings (
    id SERIAL PRIMARY KEY,
    entity_id VARCHAR REFERENCES entities(id),
    canonical_key VARCHAR,
    system VARCHAR,
    key_name VARCHAR,
    mapping_table VARCHAR,
    confidence VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE exceptions (
    id VARCHAR PRIMARY KEY,
    description TEXT,
    table_name VARCHAR REFERENCES tables(name),
    filter_condition TEXT,
    applies_to JSONB, -- Array of rule IDs or table names
    override_fields JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Migration Script**: Create a Rust migration tool that:
1. Reads existing JSON files
2. Inserts into PostgreSQL
3. Validates data integrity
4. Maintains backward compatibility during transition

### Phase 2: Data Tables (Week 2-3)
**Goal**: Move CSV/Parquet data to PostgreSQL tables

**Approach**:
1. Create PostgreSQL tables matching CSV schemas
2. Use `COPY` or bulk insert for initial load
3. Update `SqlEngine` to query PostgreSQL instead of files
4. Keep file paths in metadata for reference

**Tables**:
- One table per data source (e.g., `scf_v1_data`, `scf_v2_data`)
- Partition by system/entity if needed
- Add indexes on primary keys and time columns

**Migration Script**: 
- Use Polars to read CSV/Parquet
- Bulk insert into PostgreSQL
- Handle schema evolution

### Phase 3: Query History & Results (Week 3-4)
**Goal**: Persist all RCA queries and results

**Tables**:
```sql
CREATE TABLE rca_queries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_text TEXT NOT NULL,
    user_id VARCHAR, -- If you add user management
    status VARCHAR NOT NULL, -- 'pending', 'running', 'completed', 'failed'
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_message TEXT
);

CREATE TABLE rca_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_id UUID REFERENCES rca_queries(id),
    system_a VARCHAR NOT NULL,
    system_b VARCHAR NOT NULL,
    metric VARCHAR NOT NULL,
    as_of_date DATE,
    root_cause_found BOOLEAN,
    classifications JSONB, -- Array of root cause classifications
    comparison_stats JSONB, -- Population diff, data diff stats
    mismatch_details JSONB, -- Sample mismatch rows
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE rca_findings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    result_id UUID REFERENCES rca_results(id),
    finding_type VARCHAR NOT NULL, -- 'root_cause', 'anomaly', 'mismatch'
    description TEXT,
    severity VARCHAR, -- 'high', 'medium', 'low'
    evidence JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Phase 4: Graph Traversal State (Week 4-5)
**Goal**: Persist traversal state for resumability

**Tables**:
```sql
CREATE TABLE traversal_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_id UUID REFERENCES rca_queries(id),
    current_hypothesis TEXT,
    root_cause_found BOOLEAN DEFAULT FALSE,
    current_depth INTEGER DEFAULT 0,
    max_depth INTEGER DEFAULT 10,
    status VARCHAR NOT NULL, -- 'active', 'completed', 'paused'
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE traversal_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES traversal_sessions(id),
    node_id VARCHAR NOT NULL,
    node_type VARCHAR NOT NULL, -- 'table', 'rule', 'join', 'filter', 'metric'
    visited BOOLEAN DEFAULT FALSE,
    visit_count INTEGER DEFAULT 0,
    score FLOAT,
    probe_result JSONB, -- SqlProbeResult serialized
    metadata JSONB, -- NodeMetadata serialized
    visited_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE traversal_findings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES traversal_sessions(id),
    finding_text TEXT NOT NULL,
    confidence FLOAT,
    evidence JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE traversal_path (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES traversal_sessions(id),
    from_node_id UUID REFERENCES traversal_nodes(id),
    to_node_id UUID REFERENCES traversal_nodes(id),
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Phase 5: Knowledge Base (Week 5-6)
**Goal**: Move knowledge base to PostgreSQL with full-text search

**Tables**:
```sql
CREATE TABLE knowledge_terms (
    id SERIAL PRIMARY KEY,
    term VARCHAR NOT NULL UNIQUE,
    definition TEXT,
    category VARCHAR, -- 'business', 'technical', 'domain'
    aliases JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE knowledge_relationships (
    id SERIAL PRIMARY KEY,
    from_term_id INTEGER REFERENCES knowledge_terms(id),
    to_term_id INTEGER REFERENCES knowledge_terms(id),
    relationship_type VARCHAR, -- 'synonym', 'related', 'part_of'
    strength FLOAT, -- 0.0 to 1.0
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE knowledge_table_mappings (
    id SERIAL PRIMARY KEY,
    term_id INTEGER REFERENCES knowledge_terms(id),
    table_name VARCHAR REFERENCES tables(name),
    relevance_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Full-text search index
CREATE INDEX idx_knowledge_terms_fts ON knowledge_terms 
    USING gin(to_tsvector('english', term || ' ' || COALESCE(definition, '')));
```

### Phase 6: Clarification Sessions (Week 6-7)
**Goal**: Track clarification flow for learning

**Tables**:
```sql
CREATE TABLE clarification_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_query TEXT NOT NULL,
    status VARCHAR NOT NULL, -- 'needs_clarification', 'clarified', 'compiled'
    confidence FLOAT,
    created_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP
);

CREATE TABLE clarification_questions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES clarification_sessions(id),
    question_text TEXT NOT NULL,
    missing_pieces JSONB,
    confidence FLOAT,
    partial_understanding JSONB,
    response_hints JSONB,
    question_order INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE clarification_answers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    question_id UUID REFERENCES clarification_questions(id),
    answer_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE compiled_intents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID REFERENCES clarification_sessions(id),
    intent JSONB NOT NULL, -- IntentCompilationResult serialized
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Implementation Strategy

### 1. **Database Connection Layer**
Create a new module `src/db/mod.rs`:
```rust
// src/db/mod.rs
pub mod connection;
pub mod metadata;
pub mod data;
pub mod queries;
pub mod traversal;
pub mod knowledge;

// Use sqlx or tokio-postgres
use sqlx::PgPool;
```

### 2. **Migration Tools**
Create `src/bin/migrate_to_postgres.rs`:
- Reads JSON files
- Inserts into PostgreSQL
- Validates data
- Generates migration reports

### 3. **Backward Compatibility**
During migration:
- Keep reading from files if PostgreSQL unavailable
- Add feature flag: `--use-postgres` or env var `USE_POSTGRES=true`
- Gradually migrate endpoints

### 4. **Update Existing Code**
- `Metadata::load()` → `Metadata::load_from_db(pool)`
- `SqlEngine` → Query PostgreSQL instead of files
- `GraphTraversalAgent` → Persist state to DB
- `RcaEngine` → Save results to DB

## Benefits of PostgreSQL Migration

1. **Concurrency**: Multiple users can query simultaneously
2. **ACID Transactions**: Data consistency guarantees
3. **Query Performance**: Indexes, query optimization
4. **Scalability**: Handle larger datasets
5. **Audit Trail**: Complete history of queries and results
6. **Resumability**: Resume interrupted analyses
7. **Analytics**: Query patterns, common root causes
8. **Backup/Recovery**: Standard database backup tools
9. **Relationships**: Foreign keys ensure data integrity
10. **Full-text Search**: Better knowledge base queries

## Dependencies to Add

```toml
# Cargo.toml additions
[dependencies]
sqlx = { version = "0.7", features = ["runtime-tokio-rustls", "postgres", "chrono", "uuid"] }
tokio-postgres = "0.7"  # Alternative to sqlx
postgres-types = "0.2"  # If using tokio-postgres directly
```

## Migration Checklist

- [ ] Phase 1: Metadata tables created
- [ ] Phase 1: Migration script for metadata
- [ ] Phase 1: Update `Metadata::load()` to use DB
- [ ] Phase 2: Data tables created
- [ ] Phase 2: CSV/Parquet import script
- [ ] Phase 2: Update `SqlEngine` to query PostgreSQL
- [ ] Phase 3: Query history tables created
- [ ] Phase 3: Update `RcaEngine` to save results
- [ ] Phase 4: Traversal state tables created
- [ ] Phase 4: Update `GraphTraversalAgent` to persist state
- [ ] Phase 5: Knowledge base tables created
- [ ] Phase 5: Update knowledge base queries
- [ ] Phase 6: Clarification tables created
- [ ] Phase 6: Update clarification flow
- [ ] Testing: End-to-end tests with PostgreSQL
- [ ] Documentation: Update user guide
- [ ] Deployment: Production PostgreSQL setup

## Next Steps

1. Set up PostgreSQL database
2. Create database schema (Phase 1 tables)
3. Write migration script for metadata
4. Test with small dataset
5. Gradually migrate endpoints
6. Monitor performance
7. Complete remaining phases

