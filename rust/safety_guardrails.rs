//! Safety & Cost Guardrails
//! 
//! Estimates rows scanned, join explosion risk, memory footprint before execution.
//! Prevents dangerous operations unless --force is used.

use crate::error::{RcaError, Result};
use crate::execution_planner::ExecutionPlan;
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Safety assessment result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyAssessment {
    pub safe: bool,
    pub estimated_rows_scanned: usize,
    pub estimated_join_explosion_risk: f64, // 0.0 to 1.0
    pub estimated_memory_mb: usize,
    pub warnings: Vec<String>,
    pub requires_force: bool,
    pub reasons: Vec<String>,
}

/// Safety & Cost Guardrails
pub struct SafetyGuardrails {
    metadata: Metadata,
    max_rows_scanned: usize,
    max_memory_mb: usize,
    max_join_explosion_risk: f64,
}

impl SafetyGuardrails {
    pub fn new(metadata: Metadata) -> Self {
        Self {
            metadata,
            max_rows_scanned: 100_000_000, // 100M rows
            max_memory_mb: 10_240, // 10GB
            max_join_explosion_risk: 0.5, // 50% risk threshold
        }
    }

    /// Assess safety of execution plan
    pub fn assess(&self, plan: &ExecutionPlan) -> Result<SafetyAssessment> {
        info!("Assessing safety of execution plan");
        
        let mut estimated_rows = 0;
        let mut warnings = Vec::new();
        let mut reasons = Vec::new();
        
        // Analyze each node
        for (idx, node) in plan.nodes.iter().enumerate() {
            match node {
                crate::execution_planner::ExecutionNode::Load { table, filters, .. } => {
                    let table_rows = self.estimate_table_rows(table)?;
                    let filtered_rows = self.estimate_filtered_rows(table_rows, filters.len());
                    estimated_rows += filtered_rows;
                    
                    if table_rows > 10_000_000 {
                        warnings.push(format!("Large table detected: {} (~{} rows)", table, table_rows));
                    }
                    
                    reasons.push(format!("Load {}: ~{} rows", table, filtered_rows));
                }
                crate::execution_planner::ExecutionNode::Join { keys, .. } => {
                    // Estimate join explosion risk
                    let explosion_risk = self.estimate_join_explosion(keys)?;
                    if explosion_risk > self.max_join_explosion_risk {
                        warnings.push(format!("High join explosion risk: {:.2}%", explosion_risk * 100.0));
                    }
                    
                    reasons.push(format!("Join with explosion risk: {:.2}%", explosion_risk * 100.0));
                }
                crate::execution_planner::ExecutionNode::Aggregate { group_by, .. } => {
                    // Aggregation reduces rows
                    let reduction_factor = 0.1; // Assume 10x reduction
                    estimated_rows = (estimated_rows as f64 * reduction_factor) as usize;
                    reasons.push(format!("Aggregation by {:?} reduces rows", group_by));
                }
                _ => {}
            }
        }
        
        // Check for cartesian joins
        let has_cartesian = plan.nodes.iter().any(|n| {
            if let crate::execution_planner::ExecutionNode::Join { keys, .. } = n {
                keys.is_empty()
            } else {
                false
            }
        });
        
        if has_cartesian {
            warnings.push("Cartesian join detected - no join keys".to_string());
            return Ok(SafetyAssessment {
                safe: false,
                estimated_rows_scanned: estimated_rows,
                estimated_join_explosion_risk: 1.0,
                estimated_memory_mb: self.estimate_memory(estimated_rows),
                warnings,
                requires_force: true,
                reasons: vec!["Cartesian join requires --force flag".to_string()],
            });
        }
        
        // Estimate join explosion risk
        let join_explosion_risk = self.estimate_overall_join_explosion(plan)?;
        
        // Estimate memory
        let memory_mb = self.estimate_memory(estimated_rows);
        
        // Determine if safe
        let safe = estimated_rows <= self.max_rows_scanned &&
                   memory_mb <= self.max_memory_mb &&
                   join_explosion_risk <= self.max_join_explosion_risk &&
                   !has_cartesian;
        
        let requires_force = !safe && (
            estimated_rows > self.max_rows_scanned ||
            memory_mb > self.max_memory_mb ||
            join_explosion_risk > self.max_join_explosion_risk
        );
        
        Ok(SafetyAssessment {
            safe,
            estimated_rows_scanned: estimated_rows,
            estimated_join_explosion_risk: join_explosion_risk,
            estimated_memory_mb: memory_mb,
            warnings,
            requires_force,
            reasons,
        })
    }

    fn estimate_table_rows(&self, table_name: &str) -> Result<usize> {
        // Try to get row count from metadata or estimate
        // For now, use a default estimate based on table name patterns
        let table = self.metadata.get_table(table_name)
            .ok_or_else(|| RcaError::Metadata(format!("Table not found: {}", table_name)))?;
        
        // Check if table has row count metadata (would be added in production)
        // For now, estimate based on table type
        if table.name.contains("summary") || table.name.contains("aggregate") {
            Ok(100_000) // Summary tables are smaller
        } else if table.name.contains("transaction") || table.name.contains("detail") {
            Ok(10_000_000) // Transaction tables are larger
        } else {
            Ok(1_000_000) // Default estimate
        }
    }

    fn estimate_filtered_rows(&self, total_rows: usize, filter_count: usize) -> usize {
        // Assume each filter reduces rows by 50%
        let reduction_factor = 0.5_f64.powi(filter_count as i32);
        (total_rows as f64 * reduction_factor) as usize
    }

    fn estimate_join_explosion(&self, keys: &[String]) -> Result<f64> {
        if keys.is_empty() {
            return Ok(1.0); // Cartesian join = 100% explosion risk
        }
        
        // Estimate based on key cardinality
        // Assume keys have reasonable cardinality (would check metadata in production)
        if keys.len() == 1 {
            Ok(0.1) // Single key join = low risk
        } else {
            Ok(0.3) // Multi-key join = medium risk
        }
    }

    fn estimate_overall_join_explosion(&self, plan: &ExecutionPlan) -> Result<f64> {
        let mut max_risk = 0.0;
        
        for node in &plan.nodes {
            if let crate::execution_planner::ExecutionNode::Join { keys, .. } = node {
                let risk = self.estimate_join_explosion(keys)?;
                if risk > max_risk {
                    max_risk = risk;
                }
            }
        }
        
        Ok(max_risk)
    }

    fn estimate_memory(&self, rows: usize) -> usize {
        // Estimate: ~100 bytes per row (conservative)
        let bytes = rows * 100;
        bytes / (1024 * 1024) // Convert to MB
    }
}

