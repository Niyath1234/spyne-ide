//! Trace Collector
//! 
//! Collects execution traces during RCA execution for debugging and observability.

use crate::core::observability::execution_trace::{ExecutionTrace, NodeExecution};
use std::collections::HashMap;
use std::time::Instant;

/// Trace collector that builds execution traces during execution
pub struct TraceCollector {
    trace: ExecutionTrace,
    start_time: Instant,
    phase_start_times: HashMap<String, Instant>,
}

impl TraceCollector {
    /// Create a new trace collector
    pub fn new(request_id: String) -> Self {
        Self {
            trace: ExecutionTrace::new(request_id),
            start_time: Instant::now(),
            phase_start_times: HashMap::new(),
        }
    }

    /// Start tracking a phase
    pub fn start_phase(&mut self, phase_name: &str) {
        self.phase_start_times.insert(phase_name.to_string(), Instant::now());
    }

    /// End tracking a phase
    pub fn end_phase(&mut self, phase_name: &str) {
        if let Some(start_time) = self.phase_start_times.remove(phase_name) {
            let duration = start_time.elapsed();
            self.trace.timings.insert(phase_name.to_string(), duration);
        }
    }

    /// Record a node execution
    pub fn record_node_execution(
        &mut self,
        node_id: String,
        node_type: String,
        rows_processed: Option<usize>,
        success: bool,
        error: Option<String>,
    ) {
        let start_time = self.start_time.elapsed();
        let duration = if let Some(phase_start) = self.phase_start_times.values().next() {
            Some(phase_start.elapsed())
        } else {
            None
        };

        let node_exec = NodeExecution {
            node_id,
            node_type,
            start_time: Some(start_time),
            end_time: duration.map(|d| start_time + d),
            duration,
            rows_processed,
            success,
            error,
        };

        self.trace.nodes_executed.push(node_exec);
    }

    /// Record row count at a stage
    pub fn record_row_count(&mut self, stage: &str, count: usize) {
        self.trace.row_counts.insert(stage.to_string(), count);
    }

    /// Record filter selectivity
    pub fn record_filter_selectivity(&mut self, filter_name: &str, selectivity: f64) {
        self.trace.filter_selectivity.insert(filter_name.to_string(), selectivity);
    }

    /// Record confidence value
    pub fn record_confidence(&mut self, confidence: f64) {
        self.trace.confidence_progression.push(confidence);
    }

    /// Set grain resolution path
    pub fn set_grain_resolution_path(&mut self, path: Vec<String>) {
        self.trace.grain_resolution_path = Some(path);
    }

    /// Set logical plans
    pub fn set_logical_plans(&mut self, plans: Vec<serde_json::Value>) {
        self.trace.logical_plans = Some(plans);
    }

    /// Build the final trace
    pub fn build(self) -> ExecutionTrace {
        self.trace
    }

    /// Get mutable reference to trace (for advanced usage)
    pub fn trace_mut(&mut self) -> &mut ExecutionTrace {
        &mut self.trace
    }
}

