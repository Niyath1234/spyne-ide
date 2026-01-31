//! RCA Cursor Flow Orchestrator
//! 
//! Main orchestration engine that runs the complete RCA pipeline:
//! 1. Normalize metrics
//! 2. Materialize rows
//! 3. Canonicalize
//! 4. Diff rows
//! 5. Trace lineage
//! 6. Attribute root causes
//! 7. Build narratives

use crate::core::models::{CanonicalEntity, CanonicalEntityRegistry};
use crate::core::metrics::{MetricDefinition, MetricNormalizer};
use crate::core::engine::{
    RowMaterializationEngine, CanonicalMapper,
    RowDiffEngine, AggregateReconciliationEngine,
};
use crate::core::lineage::{
    JoinTracer, FilterTracer, RuleTracer,
    JoinTraceCollection, FilterTraceCollection, RuleTraceCollection,
};
use crate::core::rca::{
    AttributionEngine, NarrativeBuilder, RowNarrative,
    RCAConfig, RCAMode, ModeSelector,
    DimensionAggregator, DimensionAggregationResult,
    result_formatter::{ResultFormatter, FormattedDisplayResult},
};
use crate::core::performance::{Sampler, SamplingStrategy as PerfSamplingStrategy, HashDiffEngine};
use crate::core::trust::{EvidenceStore, EvidenceRecord, ExecutionInputs, ExecutionOutputs, OutputSummary};
use crate::core::llm::{LlmStrategyEngine, MetricStrategy, DrilldownStrategy};
use crate::error::{RcaError, Result};
use crate::metadata::{Metadata, Rule};
use crate::llm::LlmClient;
use polars::prelude::*;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use chrono::Utc;

/// Complete RCA result
#[derive(Debug, Clone)]
pub struct RcaCursorResult {
    /// Row-level diff results
    pub row_diff: crate::core::engine::row_diff::RowDiffResult,
    
    /// Aggregation reconciliation
    pub reconciliation: crate::core::engine::aggregate_reconcile::AggregateReconciliation,
    
    /// Root cause explanations
    pub explanations: Vec<crate::core::rca::attribution::RowExplanation>,
    
    /// Human-readable narratives
    pub narratives: Vec<RowNarrative>,
    
    /// Summary statistics
    pub summary: RcaSummary,
    
    /// Dimension aggregation (for Fast mode)
    pub dimension_aggregation: Option<DimensionAggregationResult>,
    
    /// Execution mode used
    pub mode: RCAMode,
    
    /// LLM-formatted display result (optional, generated when LLM is available)
    pub formatted_display: Option<crate::core::rca::result_formatter::FormattedDisplayResult>,
}

/// Summary of RCA results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RcaSummary {
    /// Total rows analyzed
    pub total_rows: usize,
    
    /// Rows only in left
    pub missing_left_count: usize,
    
    /// Rows only in right
    pub missing_right_count: usize,
    
    /// Value mismatches
    pub mismatch_count: usize,
    
    /// Aggregate mismatch
    pub aggregate_mismatch: f64,
    
    /// Reconciliation passes
    pub reconciliation_passes: bool,
}

/// RCA Cursor orchestrator
/// 
/// Orchestrates the complete RCA pipeline from metrics to narratives
pub struct RcaCursor {
    metadata: Metadata,
    data_dir: PathBuf,
    llm_client: Option<LlmClient>,
    entity_registry: CanonicalEntityRegistry,
    strategy_engine: Option<LlmStrategyEngine>,
    evidence_store: Option<EvidenceStore>,
}

impl RcaCursor {
    /// Create a new RCA cursor
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Self {
        Self {
            metadata,
            data_dir,
            llm_client: None,
            entity_registry: crate::core::models::create_default_registry(),
            strategy_engine: None,
            evidence_store: None,
        }
    }
    
    /// Set evidence store for Forensic mode
    pub fn with_evidence_store(mut self, evidence_dir: PathBuf) -> Self {
        self.evidence_store = Some(EvidenceStore::new(evidence_dir));
        self
    }
    
    /// Set LLM client for enhanced narratives and strategy
    pub fn with_llm(mut self, llm: LlmClient) -> Self {
        self.llm_client = Some(llm.clone());
        self.strategy_engine = Some(LlmStrategyEngine::new(llm, self.metadata.clone()));
        self
    }
    
    /// Select optimal metric strategy using LLM
    /// 
    /// Given a problem description, uses LLM to select the best metric
    /// and rule combination for investigation.
    pub async fn select_metric_strategy(
        &self,
        problem_description: &str,
        system_a: &str,
        system_b: &str,
    ) -> Result<MetricStrategy> {
        // Get available rules for both systems
        let available_rules: Vec<&Rule> = self.metadata.rules
            .iter()
            .filter(|r| r.system == system_a || r.system == system_b)
            .collect();
        
        if let Some(ref strategy_engine) = self.strategy_engine {
            strategy_engine.select_metric_strategy(
                problem_description,
                system_a,
                system_b,
                &available_rules,
            ).await
        } else {
            Err(RcaError::Execution("LLM strategy engine not available. Call with_llm() first.".to_string()))
        }
    }
    
    /// Generate drilldown strategy using LLM
    /// 
    /// Given RCA results, suggests how to drill down to find root causes.
    pub async fn generate_drilldown_strategy(
        &self,
        problem_description: &str,
        metric: &str,
        rca_summary: &RcaSummary,
        available_columns: &[String],
    ) -> Result<DrilldownStrategy> {
        if let Some(ref strategy_engine) = self.strategy_engine {
            let strategy_rca_summary = crate::core::llm::RcaSummary {
                total_rows: rca_summary.total_rows,
                missing_left_count: rca_summary.missing_left_count,
                missing_right_count: rca_summary.missing_right_count,
                mismatch_count: rca_summary.mismatch_count,
                aggregate_mismatch: rca_summary.aggregate_mismatch,
            };
            
            strategy_engine.generate_drilldown_strategy(
                problem_description,
                metric,
                &strategy_rca_summary,
                available_columns,
            ).await
        } else {
            Err(RcaError::Execution("LLM strategy engine not available. Call with_llm() first.".to_string()))
        }
    }
    
    /// Run complete RCA pipeline
    /// 
    /// This is the main entry point that orchestrates all phases:
    /// 1. Normalize metrics from rules
    /// 2. Materialize rows from both pipelines (with sampling if Fast mode)
    /// 3. Canonicalize to common format
    /// 4. Diff rows (hash-based for Fast, deterministic for Deep/Forensic)
    /// 5. Trace lineage (conditional based on mode)
    /// 6. Attribute root causes
    /// 7. Build narratives
    /// 8. Store evidence (if Forensic mode)
    pub async fn run_rca(
        &self,
        metric: &str,
        left_rule: &Rule,
        right_rule: &Rule,
        value_columns: &[String],
        reported_mismatch: f64,
        config: RCAConfig,
    ) -> Result<RcaCursorResult> {
        // Phase 1: Normalize metrics
        let metric_def_left = MetricNormalizer::normalize_from_rule(left_rule)?;
        let metric_def_right = MetricNormalizer::normalize_from_rule(right_rule)?;
        
        // Phase 2: Materialize rows (with sampling if Fast mode)
        let materializer = RowMaterializationEngine::new(self.metadata.clone(), self.data_dir.clone());
        let mut left_rows = materializer.materialize_rows(&metric_def_left, "left").await?;
        let mut right_rows = materializer.materialize_rows(&metric_def_right, "right").await?;
        
        // Apply sampling if configured (Fast mode)
        if config.should_sample() {
            if let Some(ref sampling_config) = config.sampling {
                let sampler = Sampler;
                let sampling_strategy = self.convert_sampling_strategy(sampling_config)?;
                left_rows = sampler.sample(left_rows, &sampling_strategy)?;
                right_rows = sampler.sample(right_rows, &sampling_strategy)?;
            }
        }
        
        // Phase 3: Get canonical entity
        let entity = self.entity_registry
            .get_by_system_metric(&left_rule.system, metric)
            .or_else(|| self.entity_registry.get_by_name("payment_event"))
            .ok_or_else(|| RcaError::Execution("No canonical entity found".to_string()))?;
        
        // Phase 4: Canonicalize
        let mapper = CanonicalMapper::new();
        let left_mapping = mapper.infer_mapping(&left_rows, entity)?;
        let right_mapping = mapper.infer_mapping(&right_rows, entity)?;
        
        let left_canonical = mapper.canonicalize(left_rows, entity, &left_mapping)?;
        let right_canonical = mapper.canonicalize(right_rows, entity, &right_mapping)?;
        
        // Phase 5: Diff rows (hash-based for Fast, deterministic for Deep/Forensic)
        let row_diff = if config.use_hash_diff() {
            // Fast mode: Use hash-based diff
            let hash_diff_engine = HashDiffEngine::new();
            let hash_result = hash_diff_engine.hash_diff(
                left_canonical.clone(),
                right_canonical.clone(),
                &entity.keys,
                value_columns,
            )?;
            
            // Convert hash diff result to row diff result format
            // For Fast mode, we only have keys, not full rows
            // Create minimal dataframes with keys only
            let missing_left = self.filter_by_keys(&left_canonical, &entity.keys, &hash_result.missing_left_keys)?;
            let missing_right = self.filter_by_keys(&right_canonical, &entity.keys, &hash_result.missing_right_keys)?;
            let value_mismatch = self.filter_by_keys(&left_canonical, &entity.keys, &hash_result.mismatch_keys)?;
            let matches = self.filter_by_keys(&left_canonical, &entity.keys, &hash_result.match_keys)?;
            
            crate::core::engine::row_diff::RowDiffResult {
                missing_left,
                missing_right,
                value_mismatch,
                matches,
                summary: crate::core::engine::row_diff::DiffSummary {
                    missing_left_count: hash_result.summary.missing_left_count,
                    missing_right_count: hash_result.summary.missing_right_count,
                    mismatch_count: hash_result.summary.mismatch_count,
                    match_count: hash_result.summary.match_count,
                    total_value_diff: 0.0, // Not calculated in hash diff
                },
            }
        } else {
            // Deep/Forensic mode: Use deterministic diff
            let diff_engine = RowDiffEngine::new(2); // precision = 2 decimal places
            diff_engine.diff_rows(
                left_canonical.clone(),
                right_canonical.clone(),
                &entity.keys,
                value_columns,
            )?
        };
        
        // Phase 6: Reconcile aggregates
        let reconcile_engine = AggregateReconciliationEngine::new(2);
        let reconciliation = reconcile_engine.reconcile_aggregates(
            &row_diff,
            value_columns,
            reported_mismatch,
        )?;
        
        // Phase 7: Trace lineage (conditional based on mode)
        let (join_traces_left, filter_traces_left, rule_traces_left) = 
            if config.should_trace_joins() || config.should_trace_filters() || config.should_trace_rules() {
                // In a full implementation, these would be populated during materialization
                // For now, create empty collections
                (JoinTraceCollection::new(), FilterTraceCollection::new(), RuleTraceCollection::new())
            } else {
                (JoinTraceCollection::new(), FilterTraceCollection::new(), RuleTraceCollection::new())
            };
        
        let (join_traces_right, filter_traces_right, rule_traces_right) = 
            if config.should_trace_joins() || config.should_trace_filters() || config.should_trace_rules() {
                (JoinTraceCollection::new(), FilterTraceCollection::new(), RuleTraceCollection::new())
            } else {
                (JoinTraceCollection::new(), FilterTraceCollection::new(), RuleTraceCollection::new())
            };
        
        // Phase 8: Attribute root causes
        let attribution_engine = AttributionEngine;
        let explanations = attribution_engine.explain_all_differences(
            &row_diff,
            &join_traces_left,
            &filter_traces_left,
            &rule_traces_left,
            &join_traces_right,
            &filter_traces_right,
            &rule_traces_right,
        );
        
        // Phase 9: Build narratives
        let narrative_builder = if let Some(ref llm) = self.llm_client {
            NarrativeBuilder::new().with_llm(llm.clone())
        } else {
            NarrativeBuilder::new()
        };
        
        let mut narratives = Vec::new();
        for explanation in &explanations {
            let narrative = narrative_builder.build_narrative(explanation).await?;
            narratives.push(narrative);
        }
        
        // Build summary
        let summary = RcaSummary {
            total_rows: row_diff.missing_left.height() + 
                        row_diff.missing_right.height() + 
                        row_diff.value_mismatch.height() + 
                        row_diff.matches.height(),
            missing_left_count: row_diff.summary.missing_left_count,
            missing_right_count: row_diff.summary.missing_right_count,
            mismatch_count: row_diff.summary.mismatch_count,
            aggregate_mismatch: reported_mismatch,
            reconciliation_passes: reconciliation.passes,
        };
        
        // Phase 9: Dimension aggregation for Fast mode
        let dimension_aggregation = if matches!(config.mode, RCAMode::Fast) {
            // Try to infer dimension columns from canonical entity
            let dimension_columns: Vec<String> = entity.attributes.iter()
                .filter(|attr| {
                    // Common dimension patterns
                    let attr_lower = attr.to_lowercase();
                    attr_lower.contains("date") || 
                    attr_lower.contains("bucket") || 
                    attr_lower.contains("product") || 
                    attr_lower.contains("system") ||
                    attr_lower.contains("category") ||
                    attr_lower.contains("type")
                })
                .cloned()
                .collect();
            
            if !dimension_columns.is_empty() {
                let aggregator = DimensionAggregator;
                match aggregator.aggregate_by_dimension(
                    &row_diff.missing_left,
                    &row_diff.missing_right,
                    &row_diff.value_mismatch,
                    value_columns,
                    &dimension_columns,
                ) {
                    Ok(result) => Some(result),
                    Err(e) => {
                        tracing::warn!("Failed to aggregate dimensions: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        
        // Phase 10: Store evidence if Forensic mode
        if config.store_evidence {
            if let Some(ref evidence_store) = self.evidence_store {
                let execution_id = evidence_store.generate_execution_id();
                let evidence_record = EvidenceRecord {
                    execution_id: execution_id.clone(),
                    timestamp: Utc::now(),
                    problem_description: format!("RCA for {} between {} and {}", 
                        metric, left_rule.system, right_rule.system),
                    inputs: ExecutionInputs {
                        system_a: left_rule.system.clone(),
                        system_b: right_rule.system.clone(),
                        metric: metric.to_string(),
                        rule_ids: vec![left_rule.id.clone(), right_rule.id.clone()],
                        value_columns: value_columns.to_vec(),
                        reported_mismatch,
                        parameters: std::collections::HashMap::new(),
                    },
                    outputs: ExecutionOutputs {
                        summary: OutputSummary {
                            total_rows: summary.total_rows,
                            missing_left_count: summary.missing_left_count,
                            missing_right_count: summary.missing_right_count,
                            mismatch_count: summary.mismatch_count,
                            aggregate_mismatch: summary.aggregate_mismatch,
                        },
                        reconciliation_passes: summary.reconciliation_passes,
                        root_cause_count: explanations.len(),
                        output_files: vec![],
                    },
                    intermediates: std::collections::HashMap::new(),
                    metadata: std::collections::HashMap::new(),
                };
                evidence_store.store(&evidence_record)?;
            }
        }
        
        Ok(RcaCursorResult {
            row_diff,
            reconciliation,
            explanations,
            narratives,
            summary,
            dimension_aggregation,
            mode: config.mode,
            formatted_display: None, // Will be populated by caller if needed
        })
    }
    
    /// Convert RCA sampling config to performance sampling strategy
    fn convert_sampling_strategy(
        &self,
        config: &crate::core::rca::SamplingConfig,
    ) -> Result<PerfSamplingStrategy> {
        match config.strategy {
            crate::core::rca::RCASamplingStrategy::Random => {
                Ok(PerfSamplingStrategy::Random { 
                    sample_size: config.sample_size.unwrap_or(10000) 
                })
            }
            crate::core::rca::RCASamplingStrategy::TopN => {
                let order_by = config.order_by.clone()
                    .ok_or_else(|| RcaError::Execution("order_by required for TopN sampling".to_string()))?;
                Ok(PerfSamplingStrategy::TopN {
                    order_by,
                    n: config.top_n.unwrap_or(1000),
                    descending: true,
                })
            }
            crate::core::rca::RCASamplingStrategy::Stratified { ref column } => {
                Ok(PerfSamplingStrategy::Stratified {
                    stratify_column: column.clone(),
                    sample_size_per_stratum: config.sample_size.unwrap_or(1000),
                })
            }
        }
    }
    
    /// Filter dataframe by keys
    fn filter_by_keys(
        &self,
        df: &DataFrame,
        keys: &[String],
        key_list: &[Vec<String>],
    ) -> Result<DataFrame> {
        if key_list.is_empty() {
            return Ok(df.head(Some(0)));
        }
        
        // Build filter condition: (key1 == val1 AND key2 == val2) OR ...
        let mut conditions = Vec::new();
        
        for key_vec in key_list {
            let mut key_conditions = Vec::new();
            for (idx, key_col) in keys.iter().enumerate() {
                if let Some(key_val) = key_vec.get(idx) {
                    key_conditions.push(col(key_col).eq(lit(key_val.clone())));
                }
            }
            
            if !key_conditions.is_empty() {
                let combined = key_conditions
                    .into_iter()
                    .reduce(|acc, cond| acc.and(cond))
                    .unwrap();
                conditions.push(combined);
            }
        }
        
        if conditions.is_empty() {
            return Ok(df.head(Some(0)));
        }
        
        let filter_expr = conditions
            .into_iter()
            .reduce(|acc, cond| acc.or(cond))
            .unwrap();
        
        let filtered = df
            .clone()
            .lazy()
            .filter(filter_expr)
            .collect()?;
        
        Ok(filtered)
    }
    
    /// Run RCA with simplified interface
    /// 
    /// Takes rule IDs instead of Rule objects
    pub async fn run_rca_by_rule_ids(
        &self,
        metric: &str,
        left_rule_id: &str,
        right_rule_id: &str,
        value_columns: &[String],
        reported_mismatch: f64,
        config: RCAConfig,
    ) -> Result<RcaCursorResult> {
        let left_rule = self.metadata
            .get_rule(left_rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", left_rule_id)))?;
        
        let right_rule = self.metadata
            .get_rule(right_rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", right_rule_id)))?;
        
        self.run_rca(metric, left_rule, right_rule, value_columns, reported_mismatch, config).await
    }
    
    /// Run RCA with automatic mode selection from query
    pub async fn run_rca_with_query(
        &self,
        query: &str,
        metric: &str,
        left_rule_id: &str,
        right_rule_id: &str,
        value_columns: &[String],
        reported_mismatch: f64,
    ) -> Result<RcaCursorResult> {
        let mode = ModeSelector::select_from_query(query);
        let config = match mode {
            RCAMode::Fast => RCAConfig::fast(),
            RCAMode::Deep => RCAConfig::deep(),
            RCAMode::Forensic => RCAConfig::forensic(),
        };
        
        self.run_rca_by_rule_ids(metric, left_rule_id, right_rule_id, value_columns, reported_mismatch, config).await
    }
    
    /// Run progressive RCA: Start with Fast mode, escalate if needed
    /// 
    /// This implements the progressive RCA flow:
    /// 1. Run Fast mode first
    /// 2. Check if escalation is needed based on confidence/threshold
    /// 3. Escalate to Deep mode if needed
    /// 4. Escalate to Forensic mode if user requests or explanation quality is low
    pub async fn run_progressive_rca(
        &self,
        metric: &str,
        left_rule_id: &str,
        right_rule_id: &str,
        value_columns: &[String],
        reported_mismatch: f64,
        initial_query: Option<&str>,
    ) -> Result<RcaCursorResult> {
        // Start with Fast mode
        let mut config = if let Some(query) = initial_query {
            let mode = ModeSelector::select_from_query(query);
            match mode {
                RCAMode::Fast => RCAConfig::fast(),
                RCAMode::Deep => RCAConfig::deep(),
                RCAMode::Forensic => RCAConfig::forensic(),
            }
        } else {
            RCAConfig::fast()
        };
        
        // Run initial analysis
        let mut result = self.run_rca_by_rule_ids(
            metric, left_rule_id, right_rule_id, value_columns, reported_mismatch, config.clone()
        ).await?;
        
        // Check if escalation is needed (only if we started in Fast mode)
        if matches!(config.mode, RCAMode::Fast) {
            // Calculate confidence (simplified - in practice would use LLM or more sophisticated metrics)
            let confidence = if result.summary.total_rows > 0 {
                let match_ratio = result.row_diff.matches.height() as f64 / result.summary.total_rows as f64;
                match_ratio
            } else {
                0.0
            };
            
            // Check if we should escalate
            if let Some(escalated_mode) = ModeSelector::should_escalate(
                confidence,
                result.summary.aggregate_mismatch.abs(),
                &config,
            ) {
                // Escalate to Deep mode
                config = RCAConfig::deep();
                result = self.run_rca_by_rule_ids(
                    metric, left_rule_id, right_rule_id, value_columns, reported_mismatch, config.clone()
                ).await?;
            }
        }
        
        Ok(result)
    }
}

impl RcaCursorResult {
    /// Format the result using LLM to decide what to display
    /// 
    /// This method uses the ResultFormatter to intelligently decide
    /// what information is most important to display based on the
    /// original query and available data.
    pub async fn format_display(
        &mut self,
        llm_client: Option<&LlmClient>,
        original_query: Option<&str>,
    ) -> Result<()> {
        if let Some(llm) = llm_client {
            let formatter = ResultFormatter::new().with_llm(llm.clone());
            match formatter.format_result(self, original_query).await {
                Ok(formatted) => {
                    self.formatted_display = Some(formatted);
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!("Failed to format display with LLM: {}", e);
                    // Continue without formatted display
                    Ok(())
                }
            }
        } else {
            // No LLM available, use template formatting
            let formatter = ResultFormatter::new();
            match formatter.format_result(self, original_query).await {
                Ok(formatted) => {
                    self.formatted_display = Some(formatted);
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!("Failed to format display: {}", e);
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Note: Tests would require mock metadata and data files
}

