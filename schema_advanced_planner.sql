-- Advanced Query Planner Metadata
-- Goes beyond standard SQL planner stats for speed + insights

-- ============================================
-- 1. COLUMN SEMANTICS (Business Context)
-- ============================================
CREATE TABLE IF NOT EXISTS column_semantics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    
    -- Semantic type (helps understand meaning)
    semantic_type TEXT,  -- 'customer_id', 'amount', 'date', 'category', 'email', 'phone', etc.
    business_name TEXT,  -- Human-readable name: 'Customer ID', 'Disbursement Amount'
    business_description TEXT,
    
    -- Data sensitivity (for security/privacy)
    sensitivity_level TEXT,  -- 'public', 'internal', 'confidential', 'restricted', 'pii'
    contains_pii BOOLEAN DEFAULT FALSE,
    
    -- Format patterns (for validation & optimization)
    format_pattern TEXT,  -- Regex or format: 'YYYY-MM-DD', '^[0-9]{10}$', etc.
    example_values TEXT[],  -- Non-sensitive examples
    
    -- Units and precision
    unit TEXT,  -- 'rupees', 'days', 'percentage', 'count'
    precision_decimal INTEGER,  -- For financial calculations
    
    UNIQUE(table_name, column_name)
);

CREATE INDEX idx_column_semantics_table ON column_semantics(table_name);
CREATE INDEX idx_column_semantics_type ON column_semantics(semantic_type);

-- ============================================
-- 2. DATA QUALITY METADATA (Freshness, Completeness)
-- ============================================
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column_name TEXT,  -- NULL means table-level metric
    
    -- Completeness
    completeness_score FLOAT,  -- 0.0 to 1.0 (% of non-null, valid values)
    null_percentage FLOAT,
    empty_string_percentage FLOAT,  -- For text columns
    
    -- Validity
    validity_score FLOAT,  -- % of values matching expected format
    invalid_value_count INTEGER,
    out_of_range_count INTEGER,
    
    -- Uniqueness
    uniqueness_score FLOAT,  -- For columns that should be unique
    duplicate_count INTEGER,
    
    -- Freshness
    latest_value_timestamp TIMESTAMP,  -- Most recent data point
    data_lag_hours FLOAT,  -- How stale is the data?
    update_frequency TEXT,  -- 'daily', 'hourly', 'real-time'
    
    -- Consistency
    consistency_score FLOAT,  -- Cross-column validation score
    referential_integrity_violations INTEGER,
    
    measured_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, column_name)
);

CREATE INDEX idx_quality_metrics_table ON data_quality_metrics(table_name);
CREATE INDEX idx_quality_metrics_freshness ON data_quality_metrics(latest_value_timestamp DESC);

-- ============================================
-- 3. USAGE PATTERNS (Query Optimization Hints)
-- ============================================
CREATE TABLE IF NOT EXISTS column_usage_patterns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    
    -- Access frequency
    select_frequency INTEGER DEFAULT 0,  -- How often selected
    filter_frequency INTEGER DEFAULT 0,  -- How often in WHERE clause
    join_frequency INTEGER DEFAULT 0,  -- How often in JOINs
    group_by_frequency INTEGER DEFAULT 0,  -- How often in GROUP BY
    order_by_frequency INTEGER DEFAULT 0,  -- How often in ORDER BY
    
    -- Common patterns
    common_filters JSONB,  -- [{"filter": "amount > 100000", "count": 45}, ...]
    common_join_columns TEXT[],  -- Columns commonly joined with
    typical_aggregations TEXT[],  -- ['SUM', 'AVG', 'COUNT']
    
    -- Performance hints
    hot_column BOOLEAN DEFAULT FALSE,  -- Frequently accessed → cache candidate
    cold_column BOOLEAN DEFAULT FALSE,  -- Rarely accessed → deprioritize
    
    -- Time-based patterns
    access_pattern_by_hour JSONB,  -- {"09": 45, "10": 78, ...} queries per hour
    peak_access_hours INTEGER[],  -- [9, 10, 14, 15]
    
    last_accessed TIMESTAMP WITH TIME ZONE,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(table_name, column_name)
);

CREATE INDEX idx_usage_patterns_hot ON column_usage_patterns(table_name) WHERE hot_column = TRUE;

-- ============================================
-- 4. JOIN SELECTIVITY (Advanced Relationship Metadata)
-- ============================================
CREATE TABLE IF NOT EXISTS join_statistics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    left_table TEXT REFERENCES tables(name),
    left_column TEXT,
    right_table TEXT REFERENCES tables(name),
    right_column TEXT,
    
    -- Cardinality estimates
    join_type TEXT,  -- '1:1', '1:many', 'many:1', 'many:many'
    left_cardinality BIGINT,  -- Distinct values in left
    right_cardinality BIGINT,  -- Distinct values in right
    
    -- Selectivity
    avg_matches_per_key FLOAT,  -- Average rows matched per join key
    match_percentage FLOAT,  -- % of left rows that find a match
    
    -- Join performance
    estimated_join_cost FLOAT,
    recommended_join_algorithm TEXT,  -- 'hash', 'nested_loop', 'merge'
    
    -- Relationship strength
    relationship_strength FLOAT,  -- 0.0 to 1.0 (how "tight" is the relationship)
    referential_integrity BOOLEAN,  -- Is FK constraint enforced?
    
    -- Time variance
    join_stability FLOAT,  -- Does selectivity change over time?
    last_measured TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(left_table, left_column, right_table, right_column)
);

CREATE INDEX idx_join_stats_left ON join_statistics(left_table);
CREATE INDEX idx_join_stats_right ON join_statistics(right_table);

-- ============================================
-- 5. VALUE DISTRIBUTION (Advanced Histograms)
-- ============================================
CREATE TABLE IF NOT EXISTS column_value_distribution (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    
    -- Histogram buckets (for numeric/date columns)
    histogram_bounds JSONB,  -- [0, 100, 500, 1000, 5000, max]
    histogram_frequencies JSONB,  -- [234, 189, 89, 28, 4] rows per bucket
    
    -- Most common values (MCV)
    most_common_values JSONB,  -- [{"value": "bangalore", "frequency": 0.234}, ...]
    most_common_count INTEGER DEFAULT 10,
    
    -- Rare values
    rare_values_count INTEGER,  -- Values appearing < 0.1%
    singleton_count INTEGER,  -- Values appearing exactly once
    
    -- Distribution shape
    distribution_type TEXT,  -- 'uniform', 'normal', 'skewed_left', 'skewed_right', 'bimodal'
    skewness FLOAT,  -- Statistical skewness
    kurtosis FLOAT,  -- Statistical kurtosis
    
    -- Outliers
    outlier_lower_bound FLOAT,
    outlier_upper_bound FLOAT,
    outlier_count INTEGER,
    
    last_analyzed TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, column_name)
);

CREATE INDEX idx_value_distribution_table ON column_value_distribution(table_name);

-- ============================================
-- 6. TIME-SERIES METADATA (For temporal queries)
-- ============================================
CREATE TABLE IF NOT EXISTS time_series_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    time_column TEXT NOT NULL,
    
    -- Temporal coverage
    earliest_timestamp TIMESTAMP WITH TIME ZONE,
    latest_timestamp TIMESTAMP WITH TIME ZONE,
    time_span_days FLOAT,
    
    -- Granularity
    detected_granularity TEXT,  -- 'second', 'minute', 'hour', 'day', 'week', 'month'
    has_gaps BOOLEAN,  -- Are there missing time periods?
    gap_count INTEGER,
    largest_gap_hours FLOAT,
    
    -- Patterns
    seasonality_detected BOOLEAN,
    seasonality_period TEXT,  -- 'daily', 'weekly', 'monthly', 'yearly'
    trend TEXT,  -- 'increasing', 'decreasing', 'stable', 'volatile'
    
    -- Data arrival patterns
    avg_rows_per_day FLOAT,
    peak_day_rows INTEGER,
    data_arrival_pattern TEXT,  -- 'batch', 'streaming', 'irregular'
    
    -- Useful for partitioning
    recommended_partition_strategy TEXT,  -- 'daily', 'weekly', 'monthly'
    
    last_analyzed TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, time_column)
);

CREATE INDEX idx_time_series_table ON time_series_metadata(table_name);

-- ============================================
-- 7. CORRELATION MATRIX (Cross-column insights)
-- ============================================
CREATE TABLE IF NOT EXISTS column_correlations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column1 TEXT NOT NULL,
    column2 TEXT NOT NULL,
    
    -- Statistical correlation
    correlation_coefficient FLOAT,  -- -1.0 to 1.0 (Pearson)
    correlation_strength TEXT,  -- 'strong', 'moderate', 'weak', 'none'
    
    -- Functional dependency
    is_functionally_dependent BOOLEAN,  -- col1 determines col2?
    dependency_strength FLOAT,  -- 0.0 to 1.0
    
    -- Co-occurrence
    always_non_null_together BOOLEAN,  -- If col1 has value, col2 always does too
    mutual_exclusivity BOOLEAN,  -- Only one can be non-null
    
    -- Query optimization hints
    redundant_for_queries BOOLEAN,  -- Can skip one if other is selected
    denormalization_candidate BOOLEAN,  -- Should be stored together
    
    last_measured TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, column1, column2)
);

CREATE INDEX idx_correlations_table ON column_correlations(table_name);
CREATE INDEX idx_correlations_strong ON column_correlations(correlation_coefficient) 
    WHERE ABS(correlation_coefficient) > 0.7;

-- ============================================
-- 8. ANOMALY DETECTION METADATA
-- ============================================
CREATE TABLE IF NOT EXISTS anomaly_patterns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column_name TEXT,
    
    -- Baseline statistics
    baseline_mean FLOAT,
    baseline_stddev FLOAT,
    baseline_median FLOAT,
    
    -- Anomaly thresholds
    anomaly_threshold_lower FLOAT,
    anomaly_threshold_upper FLOAT,
    
    -- Recent anomalies
    recent_anomaly_count INTEGER,
    last_anomaly_detected TIMESTAMP WITH TIME ZONE,
    anomaly_severity TEXT,  -- 'low', 'medium', 'high', 'critical'
    
    -- Alert configuration
    alert_on_anomaly BOOLEAN DEFAULT FALSE,
    alert_threshold INTEGER,  -- Alert after N consecutive anomalies
    
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, column_name)
);

CREATE INDEX idx_anomalies_table ON anomaly_patterns(table_name);

-- ============================================
-- 9. QUERY COST ESTIMATES (ML-based predictions)
-- ============================================
CREATE TABLE IF NOT EXISTS query_cost_estimates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    
    -- Scan costs
    full_table_scan_cost FLOAT,  -- Estimated cost to scan entire table
    index_scan_cost FLOAT,  -- Average cost for index scan
    
    -- Join costs
    hash_join_cost_factor FLOAT,
    nested_loop_cost_factor FLOAT,
    merge_join_cost_factor FLOAT,
    
    -- Memory estimates
    typical_memory_usage_mb FLOAT,
    peak_memory_usage_mb FLOAT,
    
    -- I/O estimates
    avg_io_operations INTEGER,
    cache_hit_ratio FLOAT,  -- 0.0 to 1.0
    
    -- Execution time (historical)
    avg_query_time_ms FLOAT,
    p50_query_time_ms FLOAT,
    p95_query_time_ms FLOAT,
    p99_query_time_ms FLOAT,
    
    last_measured TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_cost_estimates_table ON query_cost_estimates(table_name);

-- ============================================
-- 10. CACHING HINTS (For query result caching)
-- ============================================
CREATE TABLE IF NOT EXISTS cache_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column_name TEXT,
    
    -- Cacheability
    cache_worthy BOOLEAN DEFAULT FALSE,
    cache_hit_rate FLOAT,  -- Historical cache effectiveness
    
    -- Invalidation strategy
    cache_ttl_seconds INTEGER,  -- Time-to-live
    invalidation_trigger TEXT,  -- 'time', 'update', 'manual'
    
    -- Access patterns
    read_write_ratio FLOAT,  -- High ratio = good cache candidate
    temporal_locality BOOLEAN,  -- Recently accessed = likely to be accessed again
    
    -- Cost/benefit
    cache_size_estimate_mb FLOAT,
    cache_benefit_score FLOAT,  -- 0.0 to 1.0
    
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, column_name)
);

CREATE INDEX idx_cache_metadata_worthy ON cache_metadata(table_name) WHERE cache_worthy = TRUE;

-- ============================================
-- 11. BUSINESS RULES AS METADATA (For validation)
-- ============================================
CREATE TABLE IF NOT EXISTS column_business_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    
    -- Value constraints
    rule_type TEXT,  -- 'range', 'enum', 'regex', 'custom'
    rule_expression TEXT,  -- SQL expression or description
    
    -- Range rules
    min_allowed_value FLOAT,
    max_allowed_value FLOAT,
    
    -- Enum rules
    allowed_values TEXT[],
    
    -- Cross-column rules
    dependent_columns TEXT[],  -- Other columns this depends on
    
    -- Business context
    rule_rationale TEXT,  -- Why this rule exists
    rule_priority TEXT,  -- 'critical', 'important', 'nice_to_have'
    
    -- Validation
    validation_enabled BOOLEAN DEFAULT TRUE,
    violation_action TEXT,  -- 'reject', 'warn', 'log'
    current_violations INTEGER DEFAULT 0,
    
    created_by TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, column_name, rule_type)
);

CREATE INDEX idx_business_rules_table ON column_business_rules(table_name);

-- ============================================
-- 12. DATA LINEAGE & PROVENANCE (Advanced)
-- ============================================
CREATE TABLE IF NOT EXISTS column_lineage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    target_table TEXT REFERENCES tables(name),
    target_column TEXT,
    
    -- Source tracking
    source_table TEXT REFERENCES tables(name),
    source_column TEXT,
    
    -- Transformation
    transformation_type TEXT,  -- 'direct', 'aggregation', 'calculation', 'lookup'
    transformation_logic TEXT,  -- SQL expression or description
    
    -- Data flow
    hop_count INTEGER,  -- How many transformations from original source?
    data_delay_hours FLOAT,  -- Typical lag from source to target
    
    -- Impact analysis
    downstream_dependencies TEXT[],  -- What depends on this column?
    blast_radius INTEGER,  -- How many objects affected if this changes?
    
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(target_table, target_column, source_table, source_column)
);

CREATE INDEX idx_column_lineage_target ON column_lineage(target_table, target_column);
CREATE INDEX idx_column_lineage_source ON column_lineage(source_table, source_column);

-- ============================================
-- 13. PARTITION METADATA (For large tables)
-- ============================================
CREATE TABLE IF NOT EXISTS partition_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name TEXT REFERENCES tables(name) ON DELETE CASCADE,
    
    -- Partitioning strategy
    is_partitioned BOOLEAN DEFAULT FALSE,
    partition_column TEXT,
    partition_strategy TEXT,  -- 'range', 'list', 'hash'
    partition_granularity TEXT,  -- 'daily', 'weekly', 'monthly'
    
    -- Partition details
    partition_count INTEGER,
    avg_partition_size_mb FLOAT,
    largest_partition_size_mb FLOAT,
    
    -- Pruning effectiveness
    typical_partitions_scanned INTEGER,  -- For typical queries
    pruning_effectiveness FLOAT,  -- 0.0 to 1.0 (higher = better pruning)
    
    -- Recommendations
    should_repartition BOOLEAN DEFAULT FALSE,
    recommended_partition_strategy TEXT,
    
    last_analyzed TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_partition_metadata_table ON partition_metadata(table_name);

-- ============================================
-- 14. MATERIALIZED VIEW CANDIDATES
-- ============================================
CREATE TABLE IF NOT EXISTS materialized_view_candidates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    base_tables TEXT[],  -- Source tables
    
    -- Query pattern
    common_aggregation TEXT,  -- 'SUM(amount) GROUP BY branch'
    common_filters TEXT[],
    common_joins TEXT[],
    
    -- Benefit estimate
    query_frequency INTEGER,  -- How often this pattern is queried
    avg_query_cost FLOAT,
    estimated_materialization_cost FLOAT,
    cost_benefit_ratio FLOAT,  -- Higher = better candidate
    
    -- Maintenance
    recommended_refresh_strategy TEXT,  -- 'real-time', 'hourly', 'daily'
    estimated_view_size_mb FLOAT,
    
    -- Priority
    priority_score FLOAT,  -- 0.0 to 1.0
    implemented BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================
-- 15. QUERY TEMPLATES (Common patterns)
-- ============================================
CREATE TABLE IF NOT EXISTS query_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    template_name TEXT UNIQUE,
    template_category TEXT,  -- 'aggregation', 'join', 'filter', 'time_series'
    
    -- Template pattern
    sql_pattern TEXT,  -- Parameterized SQL
    description TEXT,
    
    -- Usage
    usage_count INTEGER DEFAULT 0,
    avg_execution_time_ms FLOAT,
    
    -- Optimization hints
    recommended_indexes TEXT[],
    recommended_rewrites TEXT,  -- Faster alternative queries
    
    -- Tables involved
    involved_tables TEXT[],
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_query_templates_category ON query_templates(template_category);

-- ============================================
-- SUMMARY VIEW: Complete Table Profile
-- ============================================
CREATE OR REPLACE VIEW table_complete_profile AS
SELECT 
    t.name AS table_name,
    t.system,
    t.entity_id,
    
    -- Basic stats
    ts.row_count,
    ts.table_size_bytes,
    ts.avg_row_length,
    
    -- Quality
    AVG(dqm.completeness_score) AS avg_completeness,
    AVG(dqm.validity_score) AS avg_validity,
    MAX(dqm.latest_value_timestamp) AS freshest_data,
    
    -- Usage
    SUM(cup.select_frequency) AS total_selects,
    SUM(cup.filter_frequency) AS total_filters,
    COUNT(CASE WHEN cup.hot_column THEN 1 END) AS hot_columns_count,
    
    -- Indexes
    COUNT(DISTINCT ti.index_name) AS index_count,
    
    -- Time series
    tsm.earliest_timestamp,
    tsm.latest_timestamp,
    tsm.seasonality_detected,
    
    -- Partitioning
    pm.is_partitioned,
    pm.partition_count
    
FROM tables t
LEFT JOIN table_statistics ts ON t.name = ts.table_name
LEFT JOIN data_quality_metrics dqm ON t.name = dqm.table_name
LEFT JOIN column_usage_patterns cup ON t.name = cup.table_name
LEFT JOIN table_indexes ti ON t.name = ti.table_name
LEFT JOIN time_series_metadata tsm ON t.name = tsm.table_name
LEFT JOIN partition_metadata pm ON t.name = pm.table_name
GROUP BY t.name, t.system, t.entity_id, ts.row_count, ts.table_size_bytes, 
         ts.avg_row_length, tsm.earliest_timestamp, tsm.latest_timestamp, 
         tsm.seasonality_detected, pm.is_partitioned, pm.partition_count;

COMMENT ON VIEW table_complete_profile IS 'Complete metadata profile for all tables - one-stop view for query planner and insights';

-- Grant permissions (adjust as needed)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO rca_user;

