-- ============================================================================
-- RCA ENGINE DATABASE SCHEMA
-- Run this in pgAdmin Query Tool on the rca_engine database
-- ============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- PHASE 1: METADATA TABLES
-- ============================================================================

-- Entities
CREATE TABLE entities (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    grain JSONB,
    attributes JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Tables
CREATE TABLE tables (
    name VARCHAR PRIMARY KEY,
    entity_id VARCHAR REFERENCES entities(id),
    primary_key JSONB,
    time_column VARCHAR,
    system VARCHAR NOT NULL,
    path VARCHAR,
    columns JSONB,
    labels JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Metrics
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

-- Rules
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

-- Lineage Edges
CREATE TABLE lineage_edges (
    id SERIAL PRIMARY KEY,
    from_table VARCHAR REFERENCES tables(name),
    to_table VARCHAR REFERENCES tables(name),
    keys JSONB,
    relationship VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Business Labels
CREATE TABLE business_labels (
    id SERIAL PRIMARY KEY,
    label_type VARCHAR NOT NULL,
    label VARCHAR NOT NULL,
    aliases JSONB,
    system_id VARCHAR,
    metric_id VARCHAR REFERENCES metrics(id),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Time Rules
CREATE TABLE time_rules (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR REFERENCES tables(name),
    rule_type VARCHAR NOT NULL,
    as_of_column VARCHAR,
    default_value VARCHAR,
    max_lateness_days INTEGER,
    action VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Identity Mappings
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

-- Exceptions
CREATE TABLE exceptions (
    id VARCHAR PRIMARY KEY,
    description TEXT,
    table_name VARCHAR REFERENCES tables(name),
    filter_condition TEXT,
    applies_to JSONB,
    override_fields JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- PHASE 3: QUERY HISTORY & RESULTS
-- ============================================================================

-- RCA Queries
CREATE TABLE rca_queries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_text TEXT NOT NULL,
    user_id VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_message TEXT
);

-- RCA Results
CREATE TABLE rca_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID REFERENCES rca_queries(id),
    system_a VARCHAR NOT NULL,
    system_b VARCHAR NOT NULL,
    metric VARCHAR NOT NULL,
    as_of_date DATE,
    root_cause_found BOOLEAN,
    classifications JSONB,
    comparison_stats JSONB,
    mismatch_details JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- RCA Findings
CREATE TABLE rca_findings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    result_id UUID REFERENCES rca_results(id),
    finding_type VARCHAR NOT NULL,
    description TEXT,
    severity VARCHAR,
    evidence JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- PHASE 4: GRAPH TRAVERSAL STATE
-- ============================================================================

-- Traversal Sessions
CREATE TABLE traversal_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID REFERENCES rca_queries(id),
    current_hypothesis TEXT,
    root_cause_found BOOLEAN DEFAULT FALSE,
    current_depth INTEGER DEFAULT 0,
    max_depth INTEGER DEFAULT 10,
    status VARCHAR NOT NULL DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Traversal Nodes
CREATE TABLE traversal_nodes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID REFERENCES traversal_sessions(id),
    node_id VARCHAR NOT NULL,
    node_type VARCHAR NOT NULL,
    visited BOOLEAN DEFAULT FALSE,
    visit_count INTEGER DEFAULT 0,
    score FLOAT,
    probe_result JSONB,
    metadata JSONB,
    visited_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Traversal Findings
CREATE TABLE traversal_findings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID REFERENCES traversal_sessions(id),
    finding_text TEXT NOT NULL,
    confidence FLOAT,
    evidence JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Traversal Path
CREATE TABLE traversal_path (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID REFERENCES traversal_sessions(id),
    from_node_id UUID REFERENCES traversal_nodes(id),
    to_node_id UUID REFERENCES traversal_nodes(id),
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- PHASE 5: KNOWLEDGE BASE
-- ============================================================================

-- Knowledge Terms
CREATE TABLE knowledge_terms (
    id SERIAL PRIMARY KEY,
    term VARCHAR NOT NULL UNIQUE,
    definition TEXT,
    category VARCHAR,
    aliases JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Knowledge Relationships
CREATE TABLE knowledge_relationships (
    id SERIAL PRIMARY KEY,
    from_term_id INTEGER REFERENCES knowledge_terms(id),
    to_term_id INTEGER REFERENCES knowledge_terms(id),
    relationship_type VARCHAR,
    strength FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Knowledge Table Mappings
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

-- ============================================================================
-- PHASE 6: CLARIFICATION SESSIONS
-- ============================================================================

-- Clarification Sessions
CREATE TABLE clarification_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    original_query TEXT NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'needs_clarification',
    confidence FLOAT,
    created_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP
);

-- Clarification Questions
CREATE TABLE clarification_questions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID REFERENCES clarification_sessions(id),
    question_text TEXT NOT NULL,
    missing_pieces JSONB,
    confidence FLOAT,
    partial_understanding JSONB,
    response_hints JSONB,
    question_order INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Clarification Answers
CREATE TABLE clarification_answers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    question_id UUID REFERENCES clarification_questions(id),
    answer_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Compiled Intents
CREATE TABLE compiled_intents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID REFERENCES clarification_sessions(id),
    intent JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Indexes on foreign keys
CREATE INDEX idx_tables_entity_id ON tables(entity_id);
CREATE INDEX idx_tables_system ON tables(system);
CREATE INDEX idx_rules_metric_id ON rules(metric_id);
CREATE INDEX idx_rules_system_metric ON rules(system, metric_id);
CREATE INDEX idx_lineage_from_table ON lineage_edges(from_table);
CREATE INDEX idx_lineage_to_table ON lineage_edges(to_table);
CREATE INDEX idx_rca_results_query_id ON rca_results(query_id);
CREATE INDEX idx_traversal_nodes_session_id ON traversal_nodes(session_id);
CREATE INDEX idx_traversal_findings_session_id ON traversal_findings(session_id);

-- Indexes on commonly queried columns
CREATE INDEX idx_rca_queries_status ON rca_queries(status);
CREATE INDEX idx_rca_queries_created_at ON rca_queries(created_at);
CREATE INDEX idx_traversal_sessions_status ON traversal_sessions(status);

-- ============================================================================
-- VERIFICATION QUERIES (Run these after schema creation)
-- ============================================================================

-- Check all tables exist
-- SELECT table_name 
-- FROM information_schema.tables 
-- WHERE table_schema = 'public' 
-- ORDER BY table_name;

-- Check extensions
-- SELECT * FROM pg_extension;

-- Count tables (should be 20+)
-- SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'public';

