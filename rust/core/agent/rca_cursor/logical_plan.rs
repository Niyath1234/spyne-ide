//! Logical Plan Construction for RcaCursor
//! 
//! Builds grain-normalized logical plans for both systems (A and B) based on
//! validated tasks. The plan follows the pattern:
//! Scan(base_table) → Join(join_path) → Apply(filters) → Apply(time_filter) → Aggregate(metric) GROUP BY grain_key

use crate::core::agent::rca_cursor::{ValidatedTask, GrainPlan};
use crate::core::engine::logical_plan::{LogicalPlan, FilterExpr, JoinType, AggregationExpr};
use crate::metadata::{Metadata, Rule};
use crate::error::{RcaError, Result};
use std::collections::HashMap;
use regex::Regex;

/// Logical plan builder for RCA tasks
pub struct LogicalPlanBuilder {
    metadata: Metadata,
}

impl LogicalPlanBuilder {
    /// Create a new logical plan builder
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }

    /// Build logical plans for both systems
    /// 
    /// Returns a tuple of (plan_a, plan_b) where each plan is grain-normalized
    /// and produces one row per grain_key.
    pub fn build_plans(&self, validated_task: &ValidatedTask) -> Result<(LogicalPlan, LogicalPlan)> {
        let plan_a = self.build_plan_for_system(
            &validated_task.task.system_a,
            &validated_task.base_entity_a,
            validated_task.grain_plan_a.as_ref(),
            &validated_task.task,
        )?;

        let plan_b = self.build_plan_for_system(
            &validated_task.task.system_b,
            &validated_task.base_entity_b,
            validated_task.grain_plan_b.as_ref(),
            &validated_task.task,
        )?;

        Ok((plan_a, plan_b))
    }

    /// Build logical plan for a single system
    fn build_plan_for_system(
        &self,
        system: &str,
        base_entity: &str,
        grain_plan: Option<&GrainPlan>,
        task: &crate::core::agent::rca_cursor::RcaTask,
    ) -> Result<LogicalPlan> {
        // 1. Get the rule for this system and metric
        let rule = self.get_rule(system, &task.metric)?;

        // 2. Get base table for the base entity
        let base_table = self.get_base_table(system, base_entity)?;

        // 3. Build scan node
        let mut plan = LogicalPlan::Scan {
            table: base_table.name.clone(),
            filters: Vec::new(), // Filters will be pushed down later
            projection: None,
            cost_estimate: self.estimate_table_cost(&base_table),
        };

        // 4. Apply joins if grain plan exists
        if let Some(grain_plan) = grain_plan {
            plan = self.apply_join_path(plan, grain_plan, system)?;
        }

        // 5. Apply filters from task
        plan = self.apply_task_filters(plan, &task.filters)?;

        // 6. Apply time filter if present
        if let Some(ref time_range) = task.time_window {
            plan = self.apply_time_filter(plan, time_range)?;
        }

        // 7. Apply filters from rule computation definition
        if let Some(ref filter_conditions) = rule.computation.filter_conditions {
            plan = self.apply_rule_filters(plan, filter_conditions)?;
        }

        // 8. Build aggregation node
        let grain_key = if let Some(grain_plan) = grain_plan {
            grain_plan.grain_key.clone()
        } else {
            // If no grain plan, use base entity's primary key
            base_table.primary_key.first()
                .ok_or_else(|| RcaError::Execution(format!(
                    "No primary key found for base table '{}'",
                    base_table.name
                )))?
                .clone()
        };

        plan = self.build_aggregation(plan, &rule, &grain_key)?;

        Ok(plan)
    }

    /// Get rule for a system and metric
    fn get_rule(&self, system: &str, metric: &str) -> Result<&Rule> {
        self.metadata
            .rules
            .iter()
            .find(|r| r.system == system && r.metric == metric)
            .ok_or_else(|| RcaError::Execution(format!(
                "Rule not found for system '{}' and metric '{}'",
                system, metric
            )))
    }

    /// Get base table for an entity in a system
    fn get_base_table(&self, system: &str, entity: &str) -> Result<&crate::metadata::Table> {
        self.metadata
            .tables
            .iter()
            .find(|t| t.system == system && t.entity == entity)
            .ok_or_else(|| RcaError::Execution(format!(
                "Base table not found for system '{}' and entity '{}'",
                system, entity
            )))
    }

    /// Apply join path from grain plan
    fn apply_join_path(
        &self,
        plan: LogicalPlan,
        grain_plan: &GrainPlan,
        system: &str,
    ) -> Result<LogicalPlan> {
        let mut current_plan = plan;

        for join_step in &grain_plan.join_path {
            // Get the table for the target entity
            let target_table = self.metadata
                .tables
                .iter()
                .find(|t| t.system == system && t.entity == join_step.to_entity)
                .ok_or_else(|| RcaError::Execution(format!(
                    "Table not found for system '{}' and entity '{}'",
                    system, join_step.to_entity
                )))?;

            // Build scan for the right side of the join
            let right_plan = LogicalPlan::Scan {
                table: target_table.name.clone(),
                filters: Vec::new(),
                projection: None,
                cost_estimate: self.estimate_table_cost(target_table),
            };

            // Extract join keys
            let join_keys: Vec<String> = join_step.join_keys.keys().cloned().collect();
            
            // Determine join type from join_step
            let join_type = match join_step.join_type.as_str() {
                "left" => JoinType::Left,
                "right" => JoinType::Right,
                "outer" => JoinType::Outer,
                _ => JoinType::Inner,
            };

            // Estimate join selectivity based on key cardinality
            // If we have metadata about key uniqueness, use it
            // For now, use a conservative estimate
            let join_selectivity = match self.estimate_join_selectivity(&join_step.to_entity, system) {
                Ok(sel) => sel,
                Err(_) => 0.1, // Default on error
            };

            // Estimate join cost
            let left_cost = self.estimate_plan_cost(&current_plan)?;
            let right_cost = self.estimate_table_cost(target_table)
                .ok_or_else(|| RcaError::Execution(format!(
                    "Failed to estimate cost for table '{}'",
                    target_table.name
                )))?;
            
            let join_rows_scanned = ((left_cost.rows_scanned as f64 * right_cost.rows_scanned as f64) * join_selectivity) as usize;
            let join_cost = left_cost.cost + right_cost.cost + (join_rows_scanned as f64 * 0.001);
            let join_memory_mb = left_cost.memory_mb + right_cost.memory_mb + (join_rows_scanned as f64 * 100.0 / (1024.0 * 1024.0));
            let join_time_ms = left_cost.time_ms + right_cost.time_ms + (join_rows_scanned as f64 / 1000.0);

            // Build join node
            current_plan = LogicalPlan::Join {
                left: Box::new(current_plan),
                right: Box::new(right_plan),
                join_type,
                keys: join_keys,
                selectivity_estimate: Some(join_selectivity),
                cost_estimate: Some(crate::core::engine::logical_plan::CostEstimate {
                    rows_scanned: join_rows_scanned,
                    selectivity: join_selectivity,
                    cost: join_cost,
                    memory_mb: join_memory_mb,
                    time_ms: join_time_ms,
                }),
            };
        }

        Ok(current_plan)
    }

    /// Apply filters from task
    fn apply_task_filters(
        &self,
        plan: LogicalPlan,
        filters: &[crate::core::agent::rca_cursor::Filter],
    ) -> Result<LogicalPlan> {
        let mut current_plan = plan;

        for filter in filters {
            let filter_expr = FilterExpr {
                column: filter.column.clone(),
                operator: filter.operator.clone(),
                value: Some(filter.value.clone()),
                values: None,
            };

            current_plan = LogicalPlan::Filter {
                expr: filter_expr,
                input: Box::new(current_plan),
                selectivity_estimate: Some(0.5), // Default estimate
                cost_estimate: None,
            };
        }

        Ok(current_plan)
    }

    /// Apply time filter
    fn apply_time_filter(
        &self,
        plan: LogicalPlan,
        time_range: &crate::core::agent::rca_cursor::TimeRange,
    ) -> Result<LogicalPlan> {
        // Create a filter expression for time range
        // This is a simplified version - in practice, we'd need to parse the time range
        // and create appropriate filter expressions
        
        // For now, create a placeholder filter
        // In a real implementation, we'd parse start/end and create range filters
        let filter_expr = FilterExpr {
            column: time_range.time_column.clone(),
            operator: ">=".to_string(),
            value: Some(serde_json::Value::String(time_range.start.clone())),
            values: None,
        };

        Ok(LogicalPlan::Filter {
            expr: filter_expr,
            input: Box::new(plan),
            selectivity_estimate: Some(0.3), // Time filters typically have good selectivity
            cost_estimate: None,
        })
    }

    /// Apply filters from rule computation definition
    fn apply_rule_filters(
        &self,
        plan: LogicalPlan,
        filter_conditions: &HashMap<String, String>,
    ) -> Result<LogicalPlan> {
        let mut current_plan = plan;

        for (column, condition) in filter_conditions {
            // Parse condition string (simplified - in practice would need proper parsing)
            // For now, assume it's a simple equality condition
            let filter_expr = FilterExpr {
                column: column.clone(),
                operator: "=".to_string(),
                value: Some(serde_json::Value::String(condition.clone())),
                values: None,
            };

            current_plan = LogicalPlan::Filter {
                expr: filter_expr,
                input: Box::new(current_plan),
                selectivity_estimate: Some(0.5),
                cost_estimate: None,
            };
        }

        Ok(current_plan)
    }

    /// Build aggregation node
    fn build_aggregation(
        &self,
        plan: LogicalPlan,
        rule: &Rule,
        grain_key: &str,
    ) -> Result<LogicalPlan> {
        // Parse aggregation expression from rule formula
        let mut aggregations = HashMap::new();
        
        // Extract the actual column name from the formula
        // Formula can be:
        // 1. Simple column name: "paid_amount" -> Sum(paid_amount)
        // 2. Aggregation function: "sum(paid_amount)" -> Sum(paid_amount)
        // 3. Complex expression: "col1 + col2" -> Custom aggregation
        
        let formula = rule.computation.formula.as_str();
        let metric_column = self.extract_column_from_formula(formula);
        
        // Determine aggregation type based on formula
        let agg_expr = if formula.to_lowercase().starts_with("sum(") {
            // Formula is already sum(...), extract column
            AggregationExpr::Sum(metric_column)
        } else if formula.to_lowercase().starts_with("count(") {
            AggregationExpr::Count(metric_column)
        } else if formula.to_lowercase().starts_with("avg(") || formula.to_lowercase().starts_with("mean(") {
            AggregationExpr::Avg(metric_column)
        } else if formula.to_lowercase().starts_with("min(") {
            AggregationExpr::Min(metric_column)
        } else if formula.to_lowercase().starts_with("max(") {
            AggregationExpr::Max(metric_column)
        } else {
            // Simple column name or expression - default to Sum
            // For simple column names, sum is the most common aggregation
            AggregationExpr::Sum(metric_column)
        };
        
        aggregations.insert(
            rule.metric.clone(),
            agg_expr,
        );

        Ok(LogicalPlan::Aggregate {
            group_by: vec![grain_key.to_string()],
            aggregations,
            input: Box::new(plan),
            cost_estimate: Some(crate::core::engine::logical_plan::CostEstimate {
                rows_scanned: 1000, // Estimated after aggregation
                selectivity: 1.0,
                cost: 500.0,
                memory_mb: 50.0,
                time_ms: 50.0,
            }),
        })
    }
    
    /// Extract column name from formula
    /// 
    /// Handles:
    /// - Simple column: "paid_amount" -> "paid_amount"
    /// - Aggregation: "sum(paid_amount)" -> "paid_amount"
    /// - Expression: "col1 + col2" -> uses first column or creates computed column
    fn extract_column_from_formula(&self, formula: &str) -> String {
        // Try to extract from aggregation function first
        let patterns = vec![
            (r"sum\((\w+)\)", 1),
            (r"count\((\w+|\*)\)", 1),
            (r"avg\((\w+)\)", 1),
            (r"mean\((\w+)\)", 1),
            (r"min\((\w+)\)", 1),
            (r"max\((\w+)\)", 1),
        ];
        
        for (pattern, group_idx) in patterns {
            if let Ok(re) = regex::Regex::new(pattern) {
                if let Some(cap) = re.captures(formula) {
                    if let Some(col_match) = cap.get(group_idx) {
                        let col_name = col_match.as_str();
                        // Handle count(*) case
                        if col_name == "*" {
                            return "*".to_string();
                        }
                        return col_name.to_string();
                    }
                }
            }
        }
        
        // If no aggregation function found, treat as simple column name
        // Remove whitespace and take the first word/identifier
        let trimmed = formula.trim();
        if trimmed.is_empty() {
            return "value".to_string(); // Fallback
        }
        
        // Extract first identifier (column name)
        // Simple heuristic: take first word that looks like an identifier
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if let Some(first_part) = parts.first() {
            // Remove any operators or parentheses
            let cleaned = first_part.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            if !cleaned.is_empty() {
                return cleaned.to_string();
            }
        }
        
        // Last resort: use the whole formula (might be an expression)
        trimmed.to_string()
    }

    /// Estimate table cost
    fn estimate_table_cost(&self, table: &crate::metadata::Table) -> Option<crate::core::engine::logical_plan::CostEstimate> {
        // Estimate row count (metadata doesn't have row_count field, use default)
        let row_count = 10000; // Default estimate
        
        // Estimate column count
        let column_count = table.columns.as_ref()
            .map(|cols| cols.len())
            .unwrap_or(10);
        
        // Estimate average column width (bytes per cell)
        let avg_column_width = 100; // Rough estimate
        
        // Estimate selectivity (default to 1.0 for scan without filters)
        let selectivity = 1.0;
        
        // Estimate rows scanned
        let rows_scanned = row_count;
        
        // Estimate cost based on row count and column count
        let cost = (rows_scanned as f64 * column_count as f64 * avg_column_width as f64) * 0.001;
        
        // Estimate memory usage (rows * columns * avg_bytes_per_cell)
        let memory_mb = (rows_scanned * column_count * avg_column_width) as f64 / (1024.0 * 1024.0);
        
        // Estimate time (rough: 1000 rows/ms)
        let time_ms = rows_scanned as f64 / 1000.0;
        
        Some(crate::core::engine::logical_plan::CostEstimate {
            rows_scanned,
            selectivity,
            cost,
            memory_mb,
            time_ms,
        })
    }

    /// Estimate join selectivity based on entity metadata
    fn estimate_join_selectivity(&self, entity: &str, system: &str) -> Result<f64> {
        // Try to find entity metadata to estimate cardinality
        // For now, use a conservative estimate based on entity type
        // In practice, we'd use actual statistics from metadata
        
        // Default selectivity: assume joins reduce rows significantly
        // One-to-many joins typically have selectivity around 0.1-0.5
        // Many-to-many joins have lower selectivity
        Ok(0.1) // Conservative default
    }

    /// Estimate cost of a logical plan
    fn estimate_plan_cost(&self, plan: &LogicalPlan) -> Result<crate::core::engine::logical_plan::CostEstimate> {
        match plan {
            LogicalPlan::Scan { cost_estimate, .. } => {
                Ok(cost_estimate.clone().unwrap_or_else(|| crate::core::engine::logical_plan::CostEstimate {
                    rows_scanned: 10000,
                    selectivity: 1.0,
                    cost: 1000.0,
                    memory_mb: 100.0,
                    time_ms: 100.0,
                }))
            }
            LogicalPlan::Join { cost_estimate, .. } => {
                Ok(cost_estimate.clone().unwrap_or_else(|| crate::core::engine::logical_plan::CostEstimate {
                    rows_scanned: 10000,
                    selectivity: 0.1,
                    cost: 1000.0,
                    memory_mb: 100.0,
                    time_ms: 100.0,
                }))
            }
            LogicalPlan::Filter { input, cost_estimate, .. } => {
                let input_cost = self.estimate_plan_cost(input)?;
                Ok(cost_estimate.clone().unwrap_or_else(|| crate::core::engine::logical_plan::CostEstimate {
                    rows_scanned: (input_cost.rows_scanned as f64 * 0.5) as usize,
                    selectivity: 0.5,
                    cost: input_cost.cost * 0.1,
                    memory_mb: input_cost.memory_mb * 0.5,
                    time_ms: input_cost.time_ms * 0.1,
                }))
            }
            LogicalPlan::Aggregate { input, cost_estimate, .. } => {
                let input_cost = self.estimate_plan_cost(input)?;
                Ok(cost_estimate.clone().unwrap_or_else(|| crate::core::engine::logical_plan::CostEstimate {
                    rows_scanned: (input_cost.rows_scanned as f64 * 0.1) as usize, // Aggregation reduces rows
                    selectivity: 1.0,
                    cost: input_cost.cost * 0.5,
                    memory_mb: input_cost.memory_mb * 0.1,
                    time_ms: input_cost.time_ms * 0.2,
                }))
            }
            LogicalPlan::Project { input, .. } => {
                // Projection is cheap, just pass through input cost
                self.estimate_plan_cost(input)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logical_plan_building() {
        // Test would require mock metadata and validated task
    }
}

