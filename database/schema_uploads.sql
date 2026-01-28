-- Additional tables for file upload and dataset management
-- Run this after schema.sql

-- Upload jobs tracking
CREATE TABLE IF NOT EXISTS upload_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    file_name TEXT NOT NULL,
    file_type TEXT NOT NULL, -- 'csv', 'excel', 'parquet'
    file_size_bytes BIGINT,
    sheet_name TEXT, -- for Excel files
    upload_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    error_message TEXT,
    rows_count INTEGER,
    columns_count INTEGER,
    inferred_schema JSONB, -- column names and types
    uploaded_by TEXT,
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_upload_jobs_status ON upload_jobs(status);
CREATE INDEX idx_upload_jobs_uploaded_at ON upload_jobs(uploaded_at DESC);
CREATE INDEX idx_upload_jobs_file_name ON upload_jobs(file_name);

-- Dataset versions (track changes to tables/datasets over time)
CREATE TABLE IF NOT EXISTS dataset_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT NOT NULL,
    version_number INTEGER NOT NULL,
    upload_job_id UUID REFERENCES upload_jobs(id) ON DELETE SET NULL,
    file_path TEXT NOT NULL,
    schema_snapshot JSONB, -- columns at this version
    row_count INTEGER,
    checksum TEXT, -- file hash for integrity
    is_active BOOLEAN DEFAULT TRUE, -- current active version
    notes TEXT,
    created_by TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, version_number)
);

CREATE INDEX idx_dataset_versions_table ON dataset_versions(table_name);
CREATE INDEX idx_dataset_versions_active ON dataset_versions(table_name, is_active) WHERE is_active = TRUE;
CREATE INDEX idx_dataset_versions_created_at ON dataset_versions(created_at DESC);

-- Column metadata (detailed column info for uploaded datasets)
CREATE TABLE IF NOT EXISTS dataset_columns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_version_id UUID REFERENCES dataset_versions(id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    column_position INTEGER,
    data_type TEXT, -- 'string', 'integer', 'float', 'boolean', 'date', 'datetime'
    nullable BOOLEAN DEFAULT TRUE,
    unique_values_count INTEGER,
    null_count INTEGER,
    sample_values JSONB, -- array of sample values
    min_value TEXT,
    max_value TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_dataset_columns_version ON dataset_columns(dataset_version_id);
CREATE INDEX idx_dataset_columns_table ON dataset_columns(table_name);

-- Data quality checks (track validation results)
CREATE TABLE IF NOT EXISTS data_quality_checks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    upload_job_id UUID REFERENCES upload_jobs(id) ON DELETE CASCADE,
    check_type TEXT NOT NULL, -- 'duplicate_rows', 'null_check', 'type_check', 'range_check'
    column_name TEXT,
    status TEXT NOT NULL, -- 'passed', 'warning', 'failed'
    message TEXT,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_quality_checks_job ON data_quality_checks(upload_job_id);
CREATE INDEX idx_quality_checks_status ON data_quality_checks(status);

-- Add columns to existing tables table for upload tracking
ALTER TABLE tables 
ADD COLUMN IF NOT EXISTS current_version_id UUID,
ADD COLUMN IF NOT EXISTS last_uploaded_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS upload_enabled BOOLEAN DEFAULT TRUE;

COMMENT ON TABLE upload_jobs IS 'Tracks all file uploads (CSV, Excel, Parquet)';
COMMENT ON TABLE dataset_versions IS 'Version history for datasets - enables time-travel queries';
COMMENT ON TABLE dataset_columns IS 'Detailed column metadata with statistics';
COMMENT ON TABLE data_quality_checks IS 'Data quality validation results';

