//! Deterministic Join Planning
//! 
//! This module implements the core principle:
//! **LLMs decide intent, compilers decide join mechanics.**
//! 
//! The join planner takes:
//! - Dimension usage (Filter vs Select) from LLM
//! - Join metadata (cardinality, optionality, fan-out safety) from schema
//! 
//! And deterministically produces:
//! - Join type (INNER vs LEFT)
//! - Fan-out protection strategy (if needed)
//! - Join plan with explainability

use crate::error::{RcaError, Result};
use crate::intent::semantic_intent::{DimensionIntent, DimensionUsage};
use crate::semantic::join_graph::{Cardinality, JoinEdge, JoinType};

/// Join plan - the compiler's deterministic decision
#[derive(Debug, Clone)]
pub struct JoinPlan {
    /// The join edge with determined join type
    pub join: JoinEdge,
    /// Fan-out protection strategy (if needed)
    pub fan_out_protection: Option<FanOutProtection>,
    /// Explanation of why this join type was chosen
    pub explanation: String,
}

/// Fan-out protection strategies
#[derive(Debug, Clone)]
pub enum FanOutProtection {
    /// Pre-aggregate the right side before joining
    /// This is the safest and most performant strategy
    PreAggregate {
        /// SQL for the pre-aggregated subquery
        subquery: String,
        /// Group by columns for pre-aggregation
        group_by: Vec<String>,
    },
    /// Use DISTINCT on the metric (fallback, less performant)
    DistinctMetric {
        /// Column/expression to apply DISTINCT to
        metric_expr: String,
    },
    /// Hard fail - cannot safely join without user clarification
    HardFail {
        /// Reason for failure
        reason: String,
    },
}

/// Join planner - deterministic join type selection
pub struct JoinPlanner;

impl JoinPlanner {
    /// Determine join type and protection based on dimension usage and join metadata
    /// 
    /// This is the core deterministic logic:
    /// 1. Filter intent → INNER JOIN (user wants to restrict rows)
    /// 2. Select intent + optional → LEFT JOIN (user wants augmentation)
    /// 3. Select intent + mandatory → INNER JOIN (must exist)
    /// 4. Both intent → INNER JOIN (used for filtering)
    /// 
    /// Fan-out protection is applied if:
    /// - Cardinality is OneToMany or ManyToMany
    /// - Metric is additive (SUM, COUNT)
    pub fn plan_join(
        dimension_intent: &DimensionIntent,
        join_edge: &JoinEdge,
        metric_is_additive: bool,
    ) -> Result<JoinPlan> {
        // Step 1: Determine join type based on usage + optionality
        let join_type = Self::determine_join_type(dimension_intent.usage, join_edge.optional);
        
        // Step 2: Check for fan-out risk
        let fan_out_protection = if join_edge.can_fan_out() && metric_is_additive {
            Some(Self::determine_fan_out_protection(join_edge, metric_is_additive)?)
        } else {
            None
        };
        
        // Step 3: Build explanation
        let explanation = Self::build_explanation(
            dimension_intent,
            join_edge,
            join_type,
            &fan_out_protection,
        );
        
        // Step 4: Create join edge with determined type
        let mut join = join_edge.clone();
        join.join_type = join_type;
        
        Ok(JoinPlan {
            join,
            fan_out_protection,
            explanation,
        })
    }
    
    /// Determine join type based on dimension usage and optionality
    /// 
    /// This is the core deterministic rule:
    /// - Filter → INNER (user wants to restrict)
    /// - Select + optional → LEFT (user wants augmentation, optional is OK)
    /// - Select + mandatory → INNER (must exist)
    /// - Both → INNER (used for filtering)
    fn determine_join_type(usage: DimensionUsage, optional: bool) -> JoinType {
        match usage {
            DimensionUsage::Filter => {
                // User explicitly wants to restrict rows
                JoinType::Inner
            }
            DimensionUsage::Select => {
                // Augmentation only
                if optional {
                    JoinType::Left
                } else {
                    JoinType::Inner
                }
            }
            DimensionUsage::Both => {
                // Used for both filtering and selection → filtering takes precedence
                JoinType::Inner
            }
        }
    }
    
    /// Determine fan-out protection strategy
    /// 
    /// Priority:
    /// 1. Pre-aggregation (best performance, safest)
    /// 2. DISTINCT (fallback)
    /// 3. Hard fail (if protection unclear)
    fn determine_fan_out_protection(
        join_edge: &JoinEdge,
        metric_is_additive: bool,
    ) -> Result<FanOutProtection> {
        // Strategy A: Pre-aggregation (preferred)
        // This works by aggregating the right side before joining
        if metric_is_additive {
            // For additive metrics, we can pre-aggregate the right side
            // Extract the right table's key from the join condition
            let group_by_col = Self::extract_right_key(&join_edge.on)?;
            
            Ok(FanOutProtection::PreAggregate {
                subquery: format!(
                    "SELECT {} FROM {} GROUP BY {}",
                    group_by_col, join_edge.to_table, group_by_col
                ),
                group_by: vec![group_by_col],
            })
        } else {
            // For non-additive metrics, DISTINCT is safer
            Ok(FanOutProtection::DistinctMetric {
                metric_expr: "metric".to_string(), // Will be replaced with actual metric
            })
        }
    }
    
    /// Extract the right table's key from join condition
    /// 
    /// Example: "orders.customer_id = customers.id" → "customers.id"
    fn extract_right_key(join_condition: &str) -> Result<String> {
        // Simple extraction - assumes format "left.col = right.col"
        // In production, you'd want more robust parsing
        if let Some((_, right_part)) = join_condition.split_once('=') {
            Ok(right_part.trim().to_string())
        } else {
            Err(RcaError::Execution(format!(
                "Cannot extract right key from join condition: {}",
                join_condition
            )))
        }
    }
    
    /// Build human-readable explanation of join decision
    fn build_explanation(
        dimension_intent: &DimensionIntent,
        join_edge: &JoinEdge,
        join_type: JoinType,
        fan_out_protection: &Option<FanOutProtection>,
    ) -> String {
        let mut parts = Vec::new();
        
        // Explain join type choice
        match dimension_intent.usage {
            DimensionUsage::Filter => {
                parts.push(format!(
                    "Dimension '{}' is used for filtering → INNER JOIN (restrict rows)",
                    dimension_intent.name
                ));
            }
            DimensionUsage::Select => {
                if join_edge.optional {
                    parts.push(format!(
                        "Dimension '{}' is used for augmentation and relationship is optional → LEFT JOIN (preserve all left rows)",
                        dimension_intent.name
                    ));
                } else {
                    parts.push(format!(
                        "Dimension '{}' is used for augmentation but relationship is mandatory → INNER JOIN (must exist)",
                        dimension_intent.name
                    ));
                }
            }
            DimensionUsage::Both => {
                parts.push(format!(
                    "Dimension '{}' is used for both filtering and selection → INNER JOIN (filtering takes precedence)",
                    dimension_intent.name
                ));
            }
        }
        
        // Explain fan-out protection
        if let Some(protection) = fan_out_protection {
            match protection {
                FanOutProtection::PreAggregate { .. } => {
                    parts.push(format!(
                        "Fan-out protection: Pre-aggregating {} before join (cardinality: {:?})",
                        join_edge.to_table,
                        join_edge.cardinality
                    ));
                }
                FanOutProtection::DistinctMetric { .. } => {
                    parts.push(format!(
                        "Fan-out protection: Using DISTINCT on metric (cardinality: {:?})",
                        join_edge.cardinality
                    ));
                }
                FanOutProtection::HardFail { reason } => {
                    parts.push(format!(
                        "Fan-out protection: Cannot safely join - {}",
                        reason
                    ));
                }
            }
        }
        
        parts.join(". ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_filter_intent_inner_join() {
        let intent = DimensionIntent::filter("customer_category".to_string());
        let join = JoinEdge::with_metadata(
            "orders".to_string(),
            "customers".to_string(),
            "orders.customer_id = customers.id".to_string(),
            Cardinality::ManyToOne,
            true, // optional
        );
        
        let plan = JoinPlanner::plan_join(&intent, &join, true).unwrap();
        assert_eq!(plan.join.join_type, JoinType::Inner);
        assert!(plan.explanation.contains("filtering"));
    }
    
    #[test]
    fn test_select_intent_optional_left_join() {
        let intent = DimensionIntent::select("region".to_string());
        let join = JoinEdge::with_metadata(
            "orders".to_string(),
            "regions".to_string(),
            "orders.region_id = regions.id".to_string(),
            Cardinality::ManyToOne,
            true, // optional
        );
        
        let plan = JoinPlanner::plan_join(&intent, &join, true).unwrap();
        assert_eq!(plan.join.join_type, JoinType::Left);
        assert!(plan.explanation.contains("augmentation"));
    }
    
    #[test]
    fn test_select_intent_mandatory_inner_join() {
        let intent = DimensionIntent::select("customer".to_string());
        let join = JoinEdge::with_metadata(
            "orders".to_string(),
            "customers".to_string(),
            "orders.customer_id = customers.id".to_string(),
            Cardinality::ManyToOne,
            false, // mandatory
        );
        
        let plan = JoinPlanner::plan_join(&intent, &join, true).unwrap();
        assert_eq!(plan.join.join_type, JoinType::Inner);
        assert!(plan.explanation.contains("mandatory"));
    }
    
    #[test]
    fn test_fan_out_protection() {
        let intent = DimensionIntent::select("order_items".to_string());
        let join = JoinEdge::with_metadata(
            "orders".to_string(),
            "order_items".to_string(),
            "orders.id = order_items.order_id".to_string(),
            Cardinality::OneToMany, // Fan-out risk!
            true,
        );
        
        let plan = JoinPlanner::plan_join(&intent, &join, true).unwrap();
        assert!(plan.fan_out_protection.is_some());
        assert!(plan.explanation.contains("Fan-out protection"));
    }
}





