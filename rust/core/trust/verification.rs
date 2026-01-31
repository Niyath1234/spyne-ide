//! Verification Engine
//! 
//! Provides verification capabilities for RCA results:
//! - Aggregate proof verification
//! - Consistency checks
//! - Integrity validation

use crate::error::{RcaError, Result};
use crate::core::engine::aggregate_reconcile::AggregateReconciliation;
use crate::core::agent::RcaCursorResult;
use serde::{Deserialize, Serialize};

/// Verification result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    /// Whether verification passes
    pub passes: bool,
    
    /// Verification checks performed
    pub checks: Vec<VerificationCheck>,
    
    /// Overall message
    pub message: String,
}

/// Individual verification check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationCheck {
    /// Check name
    pub name: String,
    
    /// Whether check passes
    pub passes: bool,
    
    /// Check message
    pub message: String,
    
    /// Check details
    pub details: Option<serde_json::Value>,
}

/// Verification engine
pub struct VerificationEngine {
    tolerance: f64,
}

impl VerificationEngine {
    /// Create a new verification engine
    pub fn new(tolerance: f64) -> Self {
        Self { tolerance }
    }
    
    /// Verify RCA result
    /// 
    /// Performs comprehensive verification:
    /// - Aggregate reconciliation proof
    /// - Consistency checks
    /// - Integrity validation
    pub fn verify(&self, rca_result: &RcaCursorResult) -> Result<VerificationResult> {
        let mut checks = Vec::new();
        
        // Check 1: Aggregate reconciliation
        let reconciliation_check = self.verify_reconciliation(&rca_result.reconciliation)?;
        checks.push(reconciliation_check);
        
        // Check 2: Consistency between diff and summary
        let consistency_check = self.verify_consistency(rca_result)?;
        checks.push(consistency_check);
        
        // Check 3: Integrity of row counts
        let integrity_check = self.verify_integrity(rca_result)?;
        checks.push(integrity_check);
        
        // Determine overall result
        let all_pass = checks.iter().all(|c| c.passes);
        let message = if all_pass {
            "All verification checks passed".to_string()
        } else {
            format!(
                "{} of {} checks failed",
                checks.iter().filter(|c| !c.passes).count(),
                checks.len()
            )
        };
        
        Ok(VerificationResult {
            passes: all_pass,
            checks,
            message,
        })
    }
    
    /// Verify aggregate reconciliation
    fn verify_reconciliation(
        &self,
        reconciliation: &AggregateReconciliation,
    ) -> Result<VerificationCheck> {
        let passes = reconciliation.passes;
        let message = if passes {
            format!(
                "Reconciliation verified: reported ({:.2}) matches calculated ({:.2})",
                reconciliation.reported_mismatch,
                reconciliation.calculated_mismatch
            )
        } else {
            format!(
                "Reconciliation failed: reported ({:.2}) does not match calculated ({:.2}), error: {:.2}",
                reconciliation.reported_mismatch,
                reconciliation.calculated_mismatch,
                reconciliation.reconciliation_error
            )
        };
        
        Ok(VerificationCheck {
            name: "Aggregate Reconciliation".to_string(),
            passes,
            message,
            details: Some(serde_json::json!({
                "reported_mismatch": reconciliation.reported_mismatch,
                "calculated_mismatch": reconciliation.calculated_mismatch,
                "reconciliation_error": reconciliation.reconciliation_error,
                "breakdown": reconciliation.breakdown,
            })),
        })
    }
    
    /// Verify consistency between diff results and summary
    fn verify_consistency(&self, rca_result: &RcaCursorResult) -> Result<VerificationCheck> {
        let diff = &rca_result.row_diff;
        let summary = &rca_result.summary;
        
        let mut issues = Vec::new();
        
        // Check missing_left count
        if diff.summary.missing_left_count != summary.missing_left_count {
            issues.push(format!(
                "Missing left count mismatch: diff={}, summary={}",
                diff.summary.missing_left_count,
                summary.missing_left_count
            ));
        }
        
        // Check missing_right count
        if diff.summary.missing_right_count != summary.missing_right_count {
            issues.push(format!(
                "Missing right count mismatch: diff={}, summary={}",
                diff.summary.missing_right_count,
                summary.missing_right_count
            ));
        }
        
        // Check mismatch count
        if diff.summary.mismatch_count != summary.mismatch_count {
            issues.push(format!(
                "Mismatch count mismatch: diff={}, summary={}",
                diff.summary.mismatch_count,
                summary.mismatch_count
            ));
        }
        
        let passes = issues.is_empty();
        let message = if passes {
            "Consistency check passed: diff results match summary".to_string()
        } else {
            format!("Consistency check failed: {}", issues.join("; "))
        };
        
        Ok(VerificationCheck {
            name: "Consistency Check".to_string(),
            passes,
            message,
            details: if issues.is_empty() {
                None
            } else {
                Some(serde_json::json!({ "issues": issues }))
            },
        })
    }
    
    /// Verify integrity of row counts
    fn verify_integrity(&self, rca_result: &RcaCursorResult) -> Result<VerificationCheck> {
        let diff = &rca_result.row_diff;
        
        // Calculate total from components
        let calculated_total = diff.missing_left.height() +
            diff.missing_right.height() +
            diff.value_mismatch.height() +
            diff.matches.height();
        
        // Compare with summary total
        let summary_total = rca_result.summary.total_rows;
        
        let passes = calculated_total == summary_total;
        let message = if passes {
            format!(
                "Integrity check passed: total rows ({}) matches sum of components",
                summary_total
            )
        } else {
            format!(
                "Integrity check failed: calculated total ({}) does not match summary ({})",
                calculated_total, summary_total
            )
        };
        
        Ok(VerificationCheck {
            name: "Integrity Check".to_string(),
            passes,
            message,
            details: Some(serde_json::json!({
                "calculated_total": calculated_total,
                "summary_total": summary_total,
                "missing_left": diff.missing_left.height(),
                "missing_right": diff.missing_right.height(),
                "value_mismatch": diff.value_mismatch.height(),
                "matches": diff.matches.height(),
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_verification_engine() {
        let engine = VerificationEngine::new(0.01);
        // Note: Would need mock RcaCursorResult for full test
        assert_eq!(engine.tolerance, 0.01);
    }
}

