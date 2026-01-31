//! RcaCursor Orchestrator
//! 
//! Main orchestrator that coordinates all phases of RCA execution:
//! 1. Validation
//! 2. Grain Resolution
//! 3. Logical Plan Construction
//! 4. Execution Planning
//! 5. Execution
//! 6. Grain-Level Diff
//! 7. Attribution
//! 8. Confidence Calculation

use crate::core::agent::rca_cursor::{
    validator::{TaskValidator, ValidatedTask, RcaTask},
    grain_resolver::GrainResolver,
    logical_plan::LogicalPlanBuilder,
    planner::{ExecutionPlanner, ExecutionPlan},
    executor::{ExecutionEngine, ExecutionResult},
    diff::{GrainDiffEngine, GrainDiffResult},
    attribution::GrainAttributionEngine,
    confidence::ConfidenceModel,
};
use crate::core::rca::result_v2::RCAResult;
use crate::core::observability::{TraceCollector, GLOBAL_TRACE_STORE};
use crate::core::performance::AsyncParallelExecutor;
use crate::metadata::Metadata;
use crate::error::{RcaError, Result};
use std::path::PathBuf;

/// Main RcaCursor orchestrator
pub struct RcaCursor {
    metadata: Metadata,
    data_dir: PathBuf,
    validator: TaskValidator,
    grain_resolver: GrainResolver,
    plan_builder: LogicalPlanBuilder,
    execution_planner: ExecutionPlanner,
    execution_engine: ExecutionEngine,
    diff_engine: GrainDiffEngine,
    attribution_engine: GrainAttributionEngine,
    confidence_model: ConfidenceModel,
}

impl RcaCursor {
    /// Create a new RcaCursor
    pub fn new(metadata: Metadata, data_dir: PathBuf) -> Result<Self> {
        let validator = TaskValidator::new(metadata.clone())?;
        let grain_resolver = GrainResolver::new(metadata.clone())?;
        let plan_builder = LogicalPlanBuilder::new(metadata.clone());
        let execution_planner = ExecutionPlanner::new(metadata.clone());
        let execution_engine = ExecutionEngine::new(metadata.clone(), data_dir.clone());
        let diff_engine = GrainDiffEngine::new(100); // Default top_k = 100
        let attribution_engine = GrainAttributionEngine::new(10); // Default max_contributors = 10
        let confidence_model = ConfidenceModel::new();

        Ok(Self {
            metadata,
            data_dir,
            validator,
            grain_resolver,
            plan_builder,
            execution_planner,
            execution_engine,
            diff_engine,
            attribution_engine,
            confidence_model,
        })
    }

    /// Execute an RCA task
    /// 
    /// Orchestrates the full RCA pipeline:
    /// 1. Validate task
    /// 2. Build logical plans
    /// 3. Create execution plans
    /// 4. Execute both systems
    /// 5. Compute grain-level diff
    /// 6. Compute attributions
    /// 7. Calculate confidence
    /// 8. Build RCAResult
    pub async fn execute(&self, task: RcaTask) -> Result<RCAResult> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let mut trace_collector = TraceCollector::new(request_id.clone());

        // Phase 1: Validation
        trace_collector.start_phase("validation");
        let validated_task = self.validator.validate(task.clone())?;
        trace_collector.record_node_execution(
            "validation".to_string(),
            "Validation".to_string(),
            None,
            true,
            None,
        );
        trace_collector.end_phase("validation");

        // Phase 2: Logical Plan Construction
        trace_collector.start_phase("logical_plan_construction");
        let (logical_plan_a, logical_plan_b) = self.plan_builder.build_plans(&validated_task)?;
        trace_collector.record_node_execution(
            "logical_plan_a".to_string(),
            "LogicalPlan".to_string(),
            None,
            true,
            None,
        );
        trace_collector.record_node_execution(
            "logical_plan_b".to_string(),
            "LogicalPlan".to_string(),
            None,
            true,
            None,
        );
        trace_collector.end_phase("logical_plan_construction");

        // Phase 3: Execution Planning
        trace_collector.start_phase("execution_planning");
        let (execution_plan_a, execution_plan_b) = self.execution_planner.plan_execution(
            &validated_task,
            &logical_plan_a,
            &logical_plan_b,
        )?;
        trace_collector.record_node_execution(
            "execution_plan_a".to_string(),
            "ExecutionPlan".to_string(),
            None,
            true,
            None,
        );
        trace_collector.record_node_execution(
            "execution_plan_b".to_string(),
            "ExecutionPlan".to_string(),
            None,
            true,
            None,
        );
        trace_collector.end_phase("execution_planning");

        // Phase 4: Execution
        trace_collector.start_phase("execution");
        // Determine grain_key from grain plans
        let grain_key = if let Some(ref grain_plan_a) = validated_task.grain_plan_a {
            grain_plan_a.grain_key.clone()
        } else if let Some(ref grain_plan_b) = validated_task.grain_plan_b {
            grain_plan_b.grain_key.clone()
        } else {
            // Fallback: use base entity's primary key
            format!("{}_id", validated_task.task.grain.to_lowercase())
        };

        // Set grain resolution path
        if let Some(ref grain_plan_a) = validated_task.grain_plan_a {
            let path: Vec<String> = grain_plan_a.join_path.iter()
                .map(|js| js.to_entity.clone())
                .collect();
            trace_collector.set_grain_resolution_path(path);
        }

        // Execute both systems in parallel using tokio::join!
        let (result_a, result_b) = tokio::join!(
            self.execution_engine.execute(&execution_plan_a, &grain_key),
            self.execution_engine.execute(&execution_plan_b, &grain_key),
        );
        
        let result_a = result_a?;
        let result_b = result_b?;
        
        trace_collector.record_node_execution(
            "execution_system_a".to_string(),
            "Execution".to_string(),
            Some(result_a.row_count),
            true,
            None,
        );
        trace_collector.record_row_count("system_a", result_a.row_count);
        if let Some(sel) = result_a.metadata.filter_selectivity {
            trace_collector.record_filter_selectivity("system_a", sel);
        }

        trace_collector.record_node_execution(
            "execution_system_b".to_string(),
            "Execution".to_string(),
            Some(result_b.row_count),
            true,
            None,
        );
        trace_collector.record_row_count("system_b", result_b.row_count);
        if let Some(sel) = result_b.metadata.filter_selectivity {
            trace_collector.record_filter_selectivity("system_b", sel);
        }
        trace_collector.end_phase("execution");

        // Phase 5: Grain-Level Diff
        trace_collector.start_phase("grain_diff");
        let metric_column = task.metric.clone();
        let diff_result = self.diff_engine.compute_diff(&result_a, &result_b, &metric_column)?;
        trace_collector.record_node_execution(
            "grain_diff".to_string(),
            "GrainDiff".to_string(),
            Some(diff_result.differences.len()),
            true,
            None,
        );
        trace_collector.record_row_count("diff_result", diff_result.differences.len());
        trace_collector.end_phase("grain_diff");

        // Phase 6: Attribution
        trace_collector.start_phase("attribution");
        let attributions = self.attribution_engine.compute_attributions(
            &diff_result,
            &result_a,
            &result_b,
            &metric_column,
        )?;
        trace_collector.record_node_execution(
            "attribution".to_string(),
            "Attribution".to_string(),
            Some(attributions.len()),
            true,
            None,
        );
        trace_collector.end_phase("attribution");

        // Phase 7: Confidence Calculation
        trace_collector.start_phase("confidence");
        let confidence = self.confidence_model.compute_from_metadata(
            &result_a.metadata,
            &result_b.metadata,
            1,
            None,
        )?;
        trace_collector.record_confidence(confidence);
        trace_collector.end_phase("confidence");

        // Phase 8: Build RCAResult
        let total_grain_units = diff_result.total_grain_units_a.max(diff_result.total_grain_units_b);
        let total_delta: f64 = diff_result.differences.iter().map(|d| d.delta).sum();
        
        let summary = crate::core::rca::result_v2::RCASummary {
            total_grain_units,
            missing_left_count: diff_result.missing_left_count,
            missing_right_count: diff_result.missing_right_count,
            mismatch_count: diff_result.mismatch_count,
            aggregate_difference: total_delta,
            top_k: diff_result.top_k,
        };

        let lineage_graph = crate::core::rca::result_v2::LineageGraph {
            nodes: Vec::new(),
            edges: Vec::new(),
        };

        let execution_trace = trace_collector.build();
        
        // Store trace in global trace store for debugging
        GLOBAL_TRACE_STORE.store(execution_trace.clone());

        let rca_result = RCAResult::new(
            validated_task.task.grain.clone(),
            grain_key.clone(),
            summary,
        )
        .with_differences(diff_result.differences)
        .with_attributions(attributions)
        .with_confidence(confidence)
        .with_lineage_graph(lineage_graph)
        .with_execution_trace(execution_trace);

        Ok(rca_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rca_cursor_creation() {
        // Test would require mock metadata
    }
}

