//! Deterministic Replay
//! 
//! Provides deterministic replay of RCA executions for reproducibility.
//! Uses stored evidence to recreate exact execution conditions.

use crate::error::{RcaError, Result};
use crate::core::trust::evidence::{EvidenceStore, EvidenceRecord};
use crate::core::agent::RcaCursor;
use crate::metadata::Metadata;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Configuration for replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Whether to verify outputs match original
    pub verify_outputs: bool,
    
    /// Tolerance for numeric comparisons
    pub tolerance: f64,
    
    /// Whether to save replay results
    pub save_results: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            verify_outputs: true,
            tolerance: 0.01,
            save_results: false,
        }
    }
}

/// Replay engine
pub struct ReplayEngine {
    evidence_store: EvidenceStore,
    metadata: Metadata,
    data_dir: PathBuf,
}

impl ReplayEngine {
    /// Create a new replay engine
    pub fn new(
        evidence_store: EvidenceStore,
        metadata: Metadata,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            evidence_store,
            metadata,
            data_dir,
        }
    }
    
    /// Replay an RCA execution from evidence
    /// 
    /// Loads evidence record and re-executes RCA with same parameters.
    pub async fn replay(
        &self,
        execution_id: &str,
        config: &ReplayConfig,
    ) -> Result<ReplayResult> {
        // Load evidence
        let evidence = self.evidence_store.load(execution_id)?;
        
        // Get rules from evidence
        let left_rule_id = evidence.inputs.rule_ids.get(0)
            .ok_or_else(|| RcaError::Execution("Missing left rule ID in evidence".to_string()))?;
        let right_rule_id = evidence.inputs.rule_ids.get(1)
            .ok_or_else(|| RcaError::Execution("Missing right rule ID in evidence".to_string()))?;
        
        let left_rule = self.metadata.get_rule(left_rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", left_rule_id)))?;
        let right_rule = self.metadata.get_rule(right_rule_id)
            .ok_or_else(|| RcaError::Execution(format!("Rule not found: {}", right_rule_id)))?;
        
        // Re-execute RCA using old cursor implementation
        use crate::core::agent::rca_cursor::cursor_old::RcaCursor as OldRcaCursor;
        let old_cursor = OldRcaCursor::new(self.metadata.clone(), self.data_dir.clone());
        let replay_result = old_cursor.run_rca(
            &evidence.inputs.metric,
            left_rule,
            right_rule,
            &evidence.inputs.value_columns,
            evidence.inputs.reported_mismatch,
            crate::core::rca::mode::RCAConfig::default(),
        ).await?;
        
        // Verify outputs if requested
        let verification = if config.verify_outputs {
            Some(self.verify_outputs(&evidence, &replay_result, config.tolerance)?)
        } else {
            None
        };
        
        Ok(ReplayResult {
            execution_id: execution_id.to_string(),
            replay_successful: true,
            verification,
            replay_summary: ReplaySummary {
                total_rows: replay_result.summary.total_rows,
                missing_left_count: replay_result.summary.missing_left_count,
                missing_right_count: replay_result.summary.missing_right_count,
                mismatch_count: replay_result.summary.mismatch_count,
                aggregate_mismatch: replay_result.summary.aggregate_mismatch,
            },
        })
    }
    
    /// Verify that replay outputs match original
    fn verify_outputs(
        &self,
        original: &EvidenceRecord,
        replay: &crate::core::agent::RcaCursorResult,
        tolerance: f64,
    ) -> Result<ReplayVerification> {
        let mut matches = true;
        let mut differences = Vec::new();
        
        // Compare summary statistics
        let orig_summary = &original.outputs.summary;
        let replay_summary = &replay.summary;
        
        if (orig_summary.total_rows as i64 - replay_summary.total_rows as i64).abs() > 0 {
            matches = false;
            differences.push(format!(
                "Total rows: original={}, replay={}",
                orig_summary.total_rows, replay_summary.total_rows
            ));
        }
        
        if (orig_summary.missing_left_count as i64 - replay_summary.missing_left_count as i64).abs() > 0 {
            matches = false;
            differences.push(format!(
                "Missing left: original={}, replay={}",
                orig_summary.missing_left_count, replay_summary.missing_left_count
            ));
        }
        
        if (orig_summary.missing_right_count as i64 - replay_summary.missing_right_count as i64).abs() > 0 {
            matches = false;
            differences.push(format!(
                "Missing right: original={}, replay={}",
                orig_summary.missing_right_count, replay_summary.missing_right_count
            ));
        }
        
        if (orig_summary.mismatch_count as i64 - replay_summary.mismatch_count as i64).abs() > 0 {
            matches = false;
            differences.push(format!(
                "Mismatch count: original={}, replay={}",
                orig_summary.mismatch_count, replay_summary.mismatch_count
            ));
        }
        
        if (orig_summary.aggregate_mismatch - replay_summary.aggregate_mismatch).abs() > tolerance {
            matches = false;
            differences.push(format!(
                "Aggregate mismatch: original={:.2}, replay={:.2}, diff={:.2}",
                orig_summary.aggregate_mismatch,
                replay_summary.aggregate_mismatch,
                (orig_summary.aggregate_mismatch - replay_summary.aggregate_mismatch).abs()
            ));
        }
        
        // Compare reconciliation
        if original.outputs.reconciliation_passes != replay.reconciliation.passes {
            matches = false;
            differences.push(format!(
                "Reconciliation: original={}, replay={}",
                original.outputs.reconciliation_passes, replay.reconciliation.passes
            ));
        }
        
        Ok(ReplayVerification {
            matches,
            differences,
        })
    }
}

/// Replay result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub execution_id: String,
    pub replay_successful: bool,
    pub verification: Option<ReplayVerification>,
    pub replay_summary: ReplaySummary,
}

/// Replay summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySummary {
    pub total_rows: usize,
    pub missing_left_count: usize,
    pub missing_right_count: usize,
    pub mismatch_count: usize,
    pub aggregate_mismatch: f64,
}

/// Replay verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayVerification {
    pub matches: bool,
    pub differences: Vec<String>,
}

