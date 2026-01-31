-- ============================================================================
-- ENTERPRISE SAFETY SCHEMA
-- Implements table states, roles, versioning, and safety features
-- Based on EXECUTION_PLAN.md
-- ============================================================================

-- ============================================================================
-- TABLE STATES & VERSIONING
-- ============================================================================

-- Table States Enum
CREATE TYPE table_state AS ENUM ('READ_ONLY', 'SHADOW', 'ACTIVE', 'DEPRECATED');

-- System Modes Enum
CREATE TYPE system_mode AS ENUM ('READ_ONLY', 'INGESTION_READY');

-- Roles Enum
CREATE TYPE user_role AS ENUM ('VIEWER', 'ANALYST', 'ENGINEER', 'ADMIN');

-- Enhanced Tables with State and Versioning
ALTER TABLE tables ADD COLUMN IF NOT EXISTS state table_state DEFAULT 'READ_ONLY';
ALTER TABLE tables ADD COLUMN IF NOT EXISTS version VARCHAR DEFAULT 'v1';
ALTER TABLE tables ADD COLUMN IF NOT EXISTS owner VARCHAR;
ALTER TABLE tables ADD COLUMN IF NOT EXISTS supersedes VARCHAR;
ALTER TABLE tables ADD COLUMN IF NOT EXISTS deprecated_at TIMESTAMP;
ALTER TABLE tables ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
ALTER TABLE tables ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();

-- Table State History (for audit trail)
CREATE TABLE IF NOT EXISTS table_state_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR NOT NULL REFERENCES tables(name),
    from_state table_state,
    to_state table_state NOT NULL,
    changed_by VARCHAR NOT NULL,
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table Versions (for tracking version changes)
CREATE TABLE IF NOT EXISTS table_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR NOT NULL REFERENCES tables(name),
    version VARCHAR NOT NULL,
    state table_state NOT NULL,
    schema_snapshot JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    deprecated_at TIMESTAMP,
    UNIQUE(table_name, version)
);

-- ============================================================================
-- USER ROLES & PERMISSIONS
-- ============================================================================

-- Users Table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR NOT NULL UNIQUE,
    role user_role NOT NULL DEFAULT 'VIEWER',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Role Permissions (explicit permission matrix)
CREATE TABLE IF NOT EXISTS role_permissions (
    role user_role PRIMARY KEY,
    can_query BOOLEAN NOT NULL DEFAULT FALSE,
    can_create_contracts BOOLEAN NOT NULL DEFAULT FALSE,
    can_ingest BOOLEAN NOT NULL DEFAULT FALSE,
    can_promote BOOLEAN NOT NULL DEFAULT FALSE,
    can_deprecate BOOLEAN NOT NULL DEFAULT FALSE,
    can_view_shadow BOOLEAN NOT NULL DEFAULT FALSE
);

-- Insert default role permissions
INSERT INTO role_permissions (role, can_query, can_create_contracts, can_ingest, can_promote, can_deprecate, can_view_shadow)
VALUES 
    ('VIEWER', TRUE, FALSE, FALSE, FALSE, FALSE, FALSE),
    ('ANALYST', TRUE, FALSE, FALSE, FALSE, FALSE, FALSE),
    ('ENGINEER', TRUE, TRUE, TRUE, FALSE, FALSE, TRUE),
    ('ADMIN', TRUE, TRUE, TRUE, TRUE, TRUE, TRUE)
ON CONFLICT (role) DO NOTHING;

-- ============================================================================
-- INGESTION SEMANTICS & CONTRACTS
-- ============================================================================

-- Contracts Table (with ingestion semantics)
CREATE TABLE IF NOT EXISTS contracts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id VARCHAR NOT NULL UNIQUE,
    endpoint VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL REFERENCES tables(name),
    ingestion_semantics JSONB NOT NULL, -- Required: mode, idempotency_key, etc.
    version VARCHAR NOT NULL DEFAULT 'v1',
    state table_state NOT NULL DEFAULT 'SHADOW',
    owner VARCHAR,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Ingestion Semantics Validation
-- ingestion_semantics must contain:
--   - mode: 'append' | 'upsert'
--   - idempotency_key: array of column names
--   - event_time_column: column name
--   - processing_time_column: column name
--   - dedupe_window: interval string (e.g., '24h')
--   - conflict_resolution: 'latest_wins' | 'error'

-- Ingestion History
CREATE TABLE IF NOT EXISTS ingestion_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id VARCHAR NOT NULL REFERENCES contracts(contract_id),
    rows_ingested INTEGER NOT NULL,
    rows_duplicated INTEGER DEFAULT 0,
    ingestion_time TIMESTAMP NOT NULL,
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    status VARCHAR NOT NULL DEFAULT 'success', -- 'success', 'error', 'partial'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- JOIN CANDIDATES & ACCEPTED JOINS
-- ============================================================================

-- Join Candidates (suggestions, not active)
CREATE TABLE IF NOT EXISTS join_candidates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    candidate_id VARCHAR NOT NULL UNIQUE,
    table1 VARCHAR NOT NULL REFERENCES tables(name),
    table2 VARCHAR NOT NULL REFERENCES tables(name),
    join_type VARCHAR NOT NULL, -- 'left', 'inner', 'right', 'full'
    condition TEXT NOT NULL,
    confidence FLOAT NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    reason TEXT,
    assumptions JSONB,
    risk_level VARCHAR NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    validation_stats JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP
);

-- Accepted Joins (explicitly accepted by admin)
CREATE TABLE IF NOT EXISTS accepted_joins (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    join_id VARCHAR NOT NULL UNIQUE,
    candidate_id VARCHAR REFERENCES join_candidates(candidate_id),
    table1 VARCHAR NOT NULL REFERENCES tables(name),
    table2 VARCHAR NOT NULL REFERENCES tables(name),
    join_type VARCHAR NOT NULL,
    condition TEXT NOT NULL,
    version VARCHAR NOT NULL DEFAULT 'v1',
    state table_state NOT NULL DEFAULT 'ACTIVE',
    accepted_by VARCHAR NOT NULL,
    rationale TEXT,
    validation_stats JSONB,
    supersedes VARCHAR,
    created_at TIMESTAMP DEFAULT NOW(),
    deprecated_at TIMESTAMP
);

-- Join Usage Tracking
CREATE TABLE IF NOT EXISTS join_usage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    join_id VARCHAR NOT NULL REFERENCES accepted_joins(join_id),
    query_id UUID,
    used_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- METADATA DRIFT DETECTION
-- ============================================================================

-- Drift Detection Results
CREATE TABLE IF NOT EXISTS drift_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    contract_id VARCHAR NOT NULL REFERENCES contracts(contract_id),
    from_version VARCHAR NOT NULL,
    to_version VARCHAR NOT NULL,
    severity VARCHAR NOT NULL CHECK (severity IN ('COMPATIBLE', 'WARNING', 'BREAKING')),
    changes JSONB NOT NULL,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP,
    resolved_by VARCHAR
);

-- Drift Changes (detailed change tracking)
CREATE TABLE IF NOT EXISTS drift_changes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    drift_report_id UUID NOT NULL REFERENCES drift_reports(id),
    change_type VARCHAR NOT NULL, -- 'ADD', 'REMOVE', 'RENAME', 'TYPE_CHANGE'
    column_name VARCHAR,
    old_column_name VARCHAR,
    new_column_name VARCHAR,
    old_type VARCHAR,
    new_type VARCHAR,
    severity VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- QUERY PREVIEW & EXPLAINABILITY
-- ============================================================================

-- Query Previews (mandatory preview before execution)
CREATE TABLE IF NOT EXISTS query_previews (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_text TEXT NOT NULL,
    sql TEXT NOT NULL,
    tables_used JSONB NOT NULL,
    join_versions JSONB,
    filters JSONB,
    aggregations JSONB,
    assumptions JSONB,
    confidence FLOAT CHECK (confidence >= 0 AND confidence <= 1),
    estimated_rows INTEGER,
    estimated_execution_time_ms INTEGER,
    requires_confirmation BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP
);

-- Query Executions (with explanation)
CREATE TABLE IF NOT EXISTS query_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    preview_id UUID REFERENCES query_previews(id),
    query_text TEXT NOT NULL,
    sql TEXT NOT NULL,
    explanation JSONB NOT NULL, -- plain_english, tables_used, joins, filters, etc.
    result_count INTEGER,
    execution_time_ms INTEGER,
    status VARCHAR NOT NULL DEFAULT 'success', -- 'success', 'error', 'cancelled'
    error_message TEXT,
    executed_by VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Query Corrections (user feedback loop)
CREATE TABLE IF NOT EXISTS query_corrections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_execution_id UUID NOT NULL REFERENCES query_executions(id),
    correction_type VARCHAR NOT NULL, -- 'JOIN_TYPE', 'FILTER', 'METRIC', etc.
    current_value JSONB,
    requested_value JSONB,
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- SYSTEM MODE CONFIGURATION
-- ============================================================================

-- System Configuration
CREATE TABLE IF NOT EXISTS system_config (
    key VARCHAR PRIMARY KEY,
    value JSONB NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert default system mode
INSERT INTO system_config (key, value)
VALUES ('system_mode', '"READ_ONLY"')
ON CONFLICT (key) DO NOTHING;

-- ============================================================================
-- OBSERVABILITY & METRICS
-- ============================================================================

-- Metrics Collection (Prometheus-style)
CREATE TABLE IF NOT EXISTS spyne_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    labels JSONB,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- Common metrics:
-- - spyne_ingestion_lag_seconds{contract_id}
-- - spyne_ingestion_rows_total{contract_id, status}
-- - spyne_join_usage_total{join_id}
-- - spyne_drift_detected_total{contract_id, severity}
-- - spyne_query_latency_seconds{query_type}
-- - spyne_query_preview_viewed_total
-- - spyne_query_guardrail_triggered_total{type}

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Table state indexes
CREATE INDEX IF NOT EXISTS idx_tables_state ON tables(state);
CREATE INDEX IF NOT EXISTS idx_tables_version ON tables(version);
CREATE INDEX IF NOT EXISTS idx_tables_state_version ON tables(state, version);

-- Contract indexes
CREATE INDEX IF NOT EXISTS idx_contracts_table_name ON contracts(table_name);
CREATE INDEX IF NOT EXISTS idx_contracts_state ON contracts(state);

-- Join indexes
CREATE INDEX IF NOT EXISTS idx_join_candidates_tables ON join_candidates(table1, table2);
CREATE INDEX IF NOT EXISTS idx_accepted_joins_state ON accepted_joins(state);
CREATE INDEX IF NOT EXISTS idx_accepted_joins_tables ON accepted_joins(table1, table2);

-- Query indexes
CREATE INDEX IF NOT EXISTS idx_query_previews_created_at ON query_previews(created_at);
CREATE INDEX IF NOT EXISTS idx_query_executions_executed_by ON query_executions(executed_by);
CREATE INDEX IF NOT EXISTS idx_query_executions_created_at ON query_executions(created_at);

-- Metrics indexes
CREATE INDEX IF NOT EXISTS idx_spyne_metrics_name_time ON spyne_metrics(metric_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_spyne_metrics_timestamp ON spyne_metrics(timestamp);

-- ============================================================================
-- CONSTRAINTS & VALIDATION
-- ============================================================================

-- Ensure only one ACTIVE version per table
CREATE UNIQUE INDEX IF NOT EXISTS idx_tables_active_unique 
ON tables(name) 
WHERE state = 'ACTIVE';

-- Ensure contracts have valid ingestion semantics
-- (enforced via application logic, but we can add a check constraint)
ALTER TABLE contracts ADD CONSTRAINT check_ingestion_semantics 
CHECK (
    ingestion_semantics ? 'mode' AND
    ingestion_semantics ? 'idempotency_key' AND
    ingestion_semantics ? 'event_time_column' AND
    ingestion_semantics ? 'processing_time_column' AND
    ingestion_semantics ? 'dedupe_window' AND
    ingestion_semantics ? 'conflict_resolution'
);

