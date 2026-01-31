//! Query Guards
//! 
//! Safety limits and cost guards for queries.

use crate::error::{RcaError, Result};
use crate::intent::SemanticSqlIntent;
use crate::semantic::metric::MetricDefinition;
use crate::semantic::registry::SemanticRegistry;
use std::time::Duration;

/// Query guards configuration
pub struct QueryGuards {
    pub max_dimensions: u32,
    pub max_time_range_days: u32,
    pub max_scan_rows: u64,
    pub execution_timeout: Duration,
}

impl Default for QueryGuards {
    fn default() -> Self {
        Self {
            max_dimensions: 5,
            max_time_range_days: 365,
            max_scan_rows: 10_000_000,
            execution_timeout: Duration::from_secs(30),
        }
    }
}

impl QueryGuards {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate intent against guards and metric policies
    pub fn validate_intent(
        &self,
        intent: &SemanticSqlIntent,
        registry: &dyn SemanticRegistry,
    ) -> Result<()> {
        // Check dimension count
        if intent.dimensions.len() > self.max_dimensions as usize {
            return Err(RcaError::Execution(format!(
                "Too many dimensions: {} (max: {})",
                intent.dimensions.len(),
                self.max_dimensions
            )));
        }

        // Check time range if specified
        if let Some(ref time_range) = intent.time_range {
            // Simple check - would need to parse dates for accurate validation
            // For now, just check if range is specified
        }

        // Check metric policies
        for metric_name in &intent.metrics {
            if let Some(metric) = registry.metric(metric_name) {
                let policy = metric.policy();

                // Check max time range
                if let Some(max_days) = policy.max_time_range_days {
                    if max_days > self.max_time_range_days {
                        return Err(RcaError::Execution(format!(
                            "Metric '{}' has max time range of {} days, but system limit is {}",
                            metric_name, max_days, self.max_time_range_days
                        )));
                    }
                }

                // Check max dimensions
                if let Some(max_dims) = policy.max_dimensions {
                    if intent.dimensions.len() > max_dims as usize {
                        return Err(RcaError::Execution(format!(
                            "Metric '{}' allows max {} dimensions, but {} specified",
                            metric_name,
                            max_dims,
                            intent.dimensions.len()
                        )));
                    }
                }

                // Check row limit
                if let Some(row_limit) = policy.row_limit {
                    if let Some(limit) = intent.limit {
                        if limit > row_limit {
                            return Err(RcaError::Execution(format!(
                                "Metric '{}' has max row limit of {}, but {} requested",
                                metric_name, row_limit, limit
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}





