//! Task Validation
//! 
//! Validates RcaTask semantics before execution.

use crate::metadata::Metadata;
use crate::core::agent::rca_cursor::grain_resolver::GrainResolver;
use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// RCA Task specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RcaTask {
    /// Metric name
    pub metric: String,
    /// System A name
    pub system_a: String,
    /// System B name
    pub system_b: String,
    /// Target grain (business grain, e.g. "loan", "customer")
    pub grain: String,
    /// Filter conditions
    pub filters: Vec<Filter>,
    /// Time window (optional)
    pub time_window: Option<TimeRange>,
    /// Execution mode
    pub mode: ExecutionMode,
}

/// Filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    /// Column name
    pub column: String,
    /// Filter operator (eq, ne, gt, lt, etc.)
    pub operator: String,
    /// Filter value
    pub value: serde_json::Value,
}

/// Time range filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start time
    pub start: String,
    /// End time
    pub end: String,
    /// Time column name
    pub time_column: String,
}

/// Execution mode
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Fast mode: sampling, shallow attribution, early stop
    Fast,
    /// Deep mode: full scan, full join, deep attribution
    Deep,
    /// Forensic mode: row-level lineage, full trace
    Forensic,
}

/// Validated task (output of validation)
#[derive(Debug, Clone)]
pub struct ValidatedTask {
    /// Original task
    pub task: RcaTask,
    /// Base entity for system A
    pub base_entity_a: String,
    /// Base entity for system B
    pub base_entity_b: String,
    /// Grain plan for system A
    pub grain_plan_a: Option<crate::core::agent::rca_cursor::grain_resolver::GrainPlan>,
    /// Grain plan for system B
    pub grain_plan_b: Option<crate::core::agent::rca_cursor::grain_resolver::GrainPlan>,
}

/// Task validator
pub struct TaskValidator {
    metadata: Metadata,
    grain_resolver: GrainResolver,
}

impl TaskValidator {
    /// Create a new task validator
    pub fn new(metadata: Metadata) -> Result<Self> {
        let grain_resolver = GrainResolver::new(metadata.clone())?;
        Ok(Self {
            metadata,
            grain_resolver,
        })
    }

    /// Validate an RcaTask
    /// 
    /// Performs semantic validation:
    /// - Metric exists in metadata
    /// - Metric defined for both systems
    /// - Grain exists as entity
    /// - Grain reachable from both systems
    /// - Filters reference valid columns
    /// - Time column exists if time filter given
    pub fn validate(&self, task: RcaTask) -> Result<ValidatedTask> {
        // 1. Validate metric exists
        let metric = self.metadata
            .metrics_by_id
            .get(&task.metric)
            .ok_or_else(|| RcaError::Execution(format!(
                "Metric '{}' not found in metadata",
                task.metric
            )))?;

        // 2. Validate metric defined for both systems
        let rules_a: Vec<_> = self.metadata
            .rules
            .iter()
            .filter(|r| r.system == task.system_a && r.metric == task.metric)
            .collect();
        
        if rules_a.is_empty() {
            return Err(RcaError::Execution(format!(
                "Metric '{}' not defined for system '{}'",
                task.metric, task.system_a
            )));
        }

        let rules_b: Vec<_> = self.metadata
            .rules
            .iter()
            .filter(|r| r.system == task.system_b && r.metric == task.metric)
            .collect();
        
        if rules_b.is_empty() {
            return Err(RcaError::Execution(format!(
                "Metric '{}' not defined for system '{}'",
                task.metric, task.system_b
            )));
        }

        // 3. Validate grain exists as entity
        let grain_entity = self.metadata
            .entities_by_id
            .get(&task.grain)
            .ok_or_else(|| RcaError::Execution(format!(
                "Grain '{}' not found as entity in metadata",
                task.grain
            )))?;

        // 4. Get base entities from rules
        let base_entity_a = rules_a[0].target_entity.clone();
        let base_entity_b = rules_b[0].target_entity.clone();

        // 5. Validate grain reachable from both systems
        // Always create grain plans to ensure grain_key is correctly identified
        let grain_plan_a = if base_entity_a != task.grain {
            Some(self.grain_resolver.resolve_grain(&base_entity_a, &task.grain)?)
        } else {
            // Base entity equals grain - create direct grain plan
            Some(self.grain_resolver.resolve_grain(&task.grain, &task.grain)?)
        };

        let grain_plan_b = if base_entity_b != task.grain {
            Some(self.grain_resolver.resolve_grain(&base_entity_b, &task.grain)?)
        } else {
            // Base entity equals grain - create direct grain plan
            Some(self.grain_resolver.resolve_grain(&task.grain, &task.grain)?)
        };

        // 6. Validate filters reference valid columns
        // Get tables for both systems to validate columns
        let tables_a: Vec<_> = self.metadata
            .tables
            .iter()
            .filter(|t| t.system == task.system_a)
            .collect();
        
        let tables_b: Vec<_> = self.metadata
            .tables
            .iter()
            .filter(|t| t.system == task.system_b)
            .collect();

        for filter in &task.filters {
            // Validate filter operator
            let valid_operators = ["=", "!=", "<>", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"];
            if !valid_operators.contains(&filter.operator.as_str()) {
                return Err(RcaError::Execution(format!(
                    "Invalid filter operator '{}' for column '{}'. Valid operators: {}",
                    filter.operator, filter.column, valid_operators.join(", ")
                )));
            }

            // Validate filter value based on operator
            match filter.operator.as_str() {
                "IS NULL" | "IS NOT NULL" => {
                    // These operators don't need values
                }
                "IN" => {
                    // IN requires values array
                    match &filter.value {
                        serde_json::Value::Array(_) => {
                            // Good
                        }
                        _ => {
                            return Err(RcaError::Execution(format!(
                                "Filter operator 'IN' for column '{}' requires an array value",
                                filter.column
                            )));
                        }
                    }
                }
                _ => {
                    // Other operators require a value
                    if matches!(filter.value, serde_json::Value::Null) {
                        return Err(RcaError::Execution(format!(
                            "Filter operator '{}' for column '{}' requires a value",
                            filter.operator, filter.column
                        )));
                    }
                }
            }

            // Check if column exists in any table for system A or B
            let exists_a = tables_a.iter().any(|t| {
                t.columns.as_ref().map_or(false, |cols| {
                    cols.iter().any(|c| c.name == filter.column)
                })
            });
            
            let exists_b = tables_b.iter().any(|t| {
                t.columns.as_ref().map_or(false, |cols| {
                    cols.iter().any(|c| c.name == filter.column)
                })
            });

            if !exists_a && !exists_b {
                // Provide helpful hint: list available columns
                let available_cols_a: Vec<String> = tables_a.iter()
                    .flat_map(|t| t.columns.as_ref().map(|cols| cols.iter().map(|c| c.name.clone()).collect::<Vec<_>>()).unwrap_or_default())
                    .collect();
                let available_cols_b: Vec<String> = tables_b.iter()
                    .flat_map(|t| t.columns.as_ref().map(|cols| cols.iter().map(|c| c.name.clone()).collect::<Vec<_>>()).unwrap_or_default())
                    .collect();
                
                let hint = if !available_cols_a.is_empty() || !available_cols_b.is_empty() {
                    format!(
                        " Available columns in system '{}': {}. Available columns in system '{}': {}.",
                        task.system_a,
                        available_cols_a.iter().take(10).cloned().collect::<Vec<_>>().join(", "),
                        task.system_b,
                        available_cols_b.iter().take(10).cloned().collect::<Vec<_>>().join(", ")
                    )
                } else {
                    String::new()
                };
                
                return Err(RcaError::Execution(format!(
                    "Filter column '{}' not found in any table for systems '{}' or '{}'.{}",
                    filter.column, task.system_a, task.system_b, hint
                )));
            }
        }

        // 7. Validate time column exists if time filter given
        if let Some(ref time_range) = task.time_window {
            // Validate time range format
            if time_range.start.is_empty() || time_range.end.is_empty() {
                return Err(RcaError::Execution(format!(
                    "Time window start and end must not be empty"
                )));
            }

            // Try to parse time strings (basic validation)
            // In a full implementation, we'd parse and validate the actual datetime format
            if time_range.start >= time_range.end {
                return Err(RcaError::Execution(format!(
                    "Time window start '{}' must be before end '{}'",
                    time_range.start, time_range.end
                )));
            }

            let time_col_exists_a = tables_a.iter().any(|t| {
                t.time_column.as_ref().map_or(false, |tc| tc == &time_range.time_column) ||
                t.columns.as_ref().map_or(false, |cols| {
                    cols.iter().any(|c| c.name == time_range.time_column)
                })
            });
            
            let time_col_exists_b = tables_b.iter().any(|t| {
                t.time_column.as_ref().map_or(false, |tc| tc == &time_range.time_column) ||
                t.columns.as_ref().map_or(false, |cols| {
                    cols.iter().any(|c| c.name == time_range.time_column)
                })
            });

            if !time_col_exists_a && !time_col_exists_b {
                // Provide helpful hint: list time columns
                let time_cols_a: Vec<String> = tables_a.iter()
                    .filter_map(|t| t.time_column.clone())
                    .collect();
                let time_cols_b: Vec<String> = tables_b.iter()
                    .filter_map(|t| t.time_column.clone())
                    .collect();
                
                let hint = if !time_cols_a.is_empty() || !time_cols_b.is_empty() {
                    format!(
                        " Available time columns in system '{}': {}. Available time columns in system '{}': {}.",
                        task.system_a,
                        time_cols_a.join(", "),
                        task.system_b,
                        time_cols_b.join(", ")
                    )
                } else {
                    String::new()
                };
                
                return Err(RcaError::Execution(format!(
                    "Time column '{}' not found in any table for systems '{}' or '{}'.{}",
                    time_range.time_column, task.system_a, task.system_b, hint
                )));
            }
        }

        Ok(ValidatedTask {
            task,
            base_entity_a,
            base_entity_b,
            grain_plan_a,
            grain_plan_b,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_validation() {
        // Test would require mock metadata
    }
}

