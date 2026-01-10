use crate::ambiguity::{AmbiguityResolver, ResolvedInterpretation};
use crate::diff::{ComparisonResult, DiffEngine};
use crate::drilldown::{DivergencePoint, DrilldownEngine};
use crate::error::{RcaError, Result};
use crate::graph::Hypergraph;
use crate::identity::IdentityResolver;
use crate::llm::{LlmClient, QueryInterpretation};
use crate::metadata::Metadata;
use crate::rule_compiler::{RuleCompiler, RuleExecutor};
use crate::time::TimeResolver;
use chrono::NaiveDate;
use polars::prelude::*;
use std::path::PathBuf;

pub struct RcaEngine {
    metadata: Metadata,
    llm: LlmClient,
    data_dir: PathBuf,
}

impl RcaEngine {
    pub fn new(metadata: Metadata, llm: LlmClient, data_dir: PathBuf) -> Self {
        Self {
            metadata,
            llm,
            data_dir,
        }
    }
    
    pub async fn run(&self, query: &str) -> Result<RcaResult> {
        // Step 1: LLM interprets query
        let interpretation = self.llm.interpret_query(
            query,
            &self.metadata.business_labels,
            &self.metadata.metrics,
        ).await?;
        
        // Step 2: Resolve ambiguities (max 3 questions)
        let ambiguity_resolver = AmbiguityResolver::new(self.metadata.clone());
        let resolved = ambiguity_resolver.resolve(&interpretation)?;
        
        // Step 3: Resolve rules and subgraph
        let graph = Hypergraph::new(self.metadata.clone());
        let subgraph = graph.get_reconciliation_subgraph(
            &resolved.system_a,
            &resolved.system_b,
            &resolved.metric,
        )?;
        
        // Step 4: Get rules (use resolved rule IDs or first available)
        let rule_a_id = resolved.rule_a
            .unwrap_or_else(|| subgraph.rules_a[0].clone());
        let rule_b_id = resolved.rule_b
            .unwrap_or_else(|| subgraph.rules_b[0].clone());
        
        // Step 5: Get metric metadata
        let metric = self.metadata
            .get_metric(&resolved.metric)
            .ok_or_else(|| RcaError::Execution(format!("Metric not found: {}", resolved.metric)))?;
        
        // Step 6: Parse as-of date
        let as_of_date = resolved.as_of_date
            .and_then(|d| NaiveDate::parse_from_str(&d, "%Y-%m-%d").ok());
        
        // Step 7: Execute both pipelines
        let compiler = RuleCompiler::new(self.metadata.clone(), self.data_dir.clone());
        let executor = RuleExecutor::new(compiler);
        
        let df_a = executor.execute(&rule_a_id, as_of_date).await?;
        let df_b = executor.execute(&rule_b_id, as_of_date).await?;
        
        // Step 8: Grain normalization
        let identity_resolver = IdentityResolver::new(self.metadata.clone(), self.data_dir.clone());
        let grain_a = graph.get_rule_grain(&rule_a_id)?;
        let grain_b = graph.get_rule_grain(&rule_b_id)?;
        
        // Normalize to common grain
        let common_grain = if grain_a == grain_b {
            grain_a
        } else {
            // Find common grain (simplified - would use identity resolution)
            metric.grain.clone()
        };
        
        let df_a_normalized = identity_resolver.normalize_keys(df_a, &subgraph.tables_a[0], &common_grain).await?;
        let df_b_normalized = identity_resolver.normalize_keys(df_b, &subgraph.tables_b[0], &common_grain).await?;
        
        // Step 9: Apply time logic
        let time_resolver = TimeResolver::new(self.metadata.clone());
        let temporal_misalignment = time_resolver.detect_temporal_misalignment(
            &df_a_normalized,
            &df_b_normalized,
            &subgraph.tables_a[0],
            &subgraph.tables_b[0],
        )?;
        
        // Step 10: Compare results
        let diff_engine = DiffEngine;
        let comparison = diff_engine.compare(
            df_a_normalized.clone(),
            df_b_normalized.clone(),
            &common_grain,
            &resolved.metric,
            metric.precision,
        )?;
        
        // Step 11: Classify mismatches
        let classifications = self.classify_mismatches(&comparison, temporal_misalignment.as_ref())?;
        
        // Step 12: Drill-down for mismatched keys
        let mismatched_keys: Vec<Vec<String>> = if comparison.data_diff.mismatches > 0 {
            let mut keys = Vec::new();
            let mismatch_df = &comparison.data_diff.mismatch_details;
            for row_idx in 0..mismatch_df.height().min(100) { // Limit to 100 for performance
                let mut key = Vec::new();
                for col_name in &common_grain {
                    if let Ok(col_val) = mismatch_df.column(col_name) {
                        let val_str = match col_val.dtype() {
                            DataType::Utf8 => {
                                col_val.str().unwrap().get(row_idx).unwrap_or("").to_string()
                            }
                            DataType::Int64 => {
                                col_val.i64().unwrap().get(row_idx).unwrap_or(0).to_string()
                            }
                            DataType::Float64 => {
                                col_val.f64().unwrap().get(row_idx).unwrap_or(0.0).to_string()
                            }
                            _ => format!("{:?}", col_val.get(row_idx)),
                        };
                        key.push(val_str);
                    }
                }
                if !key.is_empty() {
                    keys.push(key);
                }
            }
            keys
        } else {
            Vec::new()
        };
        
        let divergence = if !mismatched_keys.is_empty() {
            let drilldown = DrilldownEngine::new(executor);
            Some(drilldown.find_divergence(&rule_a_id, &rule_b_id, &mismatched_keys, as_of_date).await?)
        } else {
            None
        };
        
        // Step 13: Generate explanation
        let result = RcaResult {
            query: query.to_string(),
            system_a: resolved.system_a.clone(),
            system_b: resolved.system_b.clone(),
            metric: resolved.metric.clone(),
            as_of_date,
            comparison,
            classifications,
            divergence,
            temporal_misalignment,
        };
        
        Ok(result)
    }
    
    fn classify_mismatches(
        &self,
        comparison: &ComparisonResult,
        temporal_misalignment: Option<&crate::time::TemporalMisalignment>,
    ) -> Result<Vec<RootCauseClassification>> {
        let mut classifications = Vec::new();
        
        // Population mismatch
        if !comparison.population_diff.missing_in_b.is_empty() {
            classifications.push(RootCauseClassification {
                root_cause: "Population Mismatch".to_string(),
                subtype: "Missing Entities".to_string(),
                description: format!("{} entities missing in system B", comparison.population_diff.missing_in_b.len()),
                count: comparison.population_diff.missing_in_b.len(),
            });
        }
        
        if !comparison.population_diff.extra_in_b.is_empty() {
            classifications.push(RootCauseClassification {
                root_cause: "Population Mismatch".to_string(),
                subtype: "Extra Entities".to_string(),
                description: format!("{} extra entities in system B", comparison.population_diff.extra_in_b.len()),
                count: comparison.population_diff.extra_in_b.len(),
            });
        }
        
        // Data mismatch
        if comparison.data_diff.mismatches > 0 {
            classifications.push(RootCauseClassification {
                root_cause: "Data Mismatch".to_string(),
                subtype: "Value Difference".to_string(),
                description: format!("{} entities have different metric values", comparison.data_diff.mismatches),
                count: comparison.data_diff.mismatches,
            });
        }
        
        // Time misalignment
        if temporal_misalignment.is_some() {
            classifications.push(RootCauseClassification {
                root_cause: "Data Mismatch".to_string(),
                subtype: "Time Misalignment".to_string(),
                description: "Temporal misalignment detected between systems".to_string(),
                count: 1,
            });
        }
        
        // Logic mismatch (inferred from divergence)
        // This would be set during drilldown
        
        Ok(classifications)
    }
}

#[derive(Debug, Clone)]
pub struct RcaResult {
    pub query: String,
    pub system_a: String,
    pub system_b: String,
    pub metric: String,
    pub as_of_date: Option<NaiveDate>,
    pub comparison: ComparisonResult,
    pub classifications: Vec<RootCauseClassification>,
    pub divergence: Option<DivergencePoint>,
    pub temporal_misalignment: Option<crate::time::TemporalMisalignment>,
}

impl std::fmt::Display for RcaResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "RCA Result for: {}", self.query)?;
        writeln!(f, "System A: {} | System B: {} | Metric: {}", 
            self.system_a, self.system_b, self.metric)?;
        
        if let Some(date) = self.as_of_date {
            writeln!(f, "As-of Date: {}", date)?;
        }
        
        writeln!(f, "\n=== Classifications ===")?;
        for classification in &self.classifications {
            writeln!(f, "- {} ({})", classification.root_cause, classification.subtype)?;
            writeln!(f, "  {}", classification.description)?;
        }
        
        writeln!(f, "\n=== Population Diff ===")?;
        writeln!(f, "Missing in B: {}", self.comparison.population_diff.missing_in_b.len())?;
        writeln!(f, "Extra in B: {}", self.comparison.population_diff.extra_in_b.len())?;
        writeln!(f, "Common: {}", self.comparison.population_diff.common_count)?;
        
        writeln!(f, "\n=== Data Diff ===")?;
        writeln!(f, "Matches: {}", self.comparison.data_diff.matches)?;
        writeln!(f, "Mismatches: {}", self.comparison.data_diff.mismatches)?;
        
        if let Some(divergence) = &self.divergence {
            writeln!(f, "\n=== Divergence Point ===")?;
            writeln!(f, "Step: {} | Type: {}", divergence.step_index, divergence.divergence_type)?;
        }
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RootCauseClassification {
    pub root_cause: String,
    pub subtype: String,
    pub description: String,
    pub count: usize,
}

