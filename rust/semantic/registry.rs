//! Semantic Registry Implementation
//! 
//! Central registry for metrics, dimensions, and join resolution.

use crate::error::{RcaError, Result};
use crate::semantic::dimension::{DimensionDefinition, SemanticDimension};
use crate::semantic::join_graph::{resolve_joins, JoinEdge};
use crate::semantic::metric::{MetricDefinition, SemanticMetric};
use std::collections::HashMap;
use std::sync::Arc;

/// Semantic registry trait
pub trait SemanticRegistry: Send + Sync {
    fn metric(&self, name: &str) -> Option<Arc<dyn MetricDefinition>>;
    fn dimension(&self, name: &str) -> Option<Arc<dyn DimensionDefinition>>;
    fn list_metrics(&self) -> Vec<String>;
    fn list_dimensions(&self) -> Vec<String>;
    fn resolve_joins_for_query(
        &self,
        metric_name: &str,
        dimension_names: &[String],
    ) -> Result<Vec<JoinEdge>>;
}

/// In-memory semantic registry implementation
pub struct InMemorySemanticRegistry {
    metrics: HashMap<String, Arc<dyn MetricDefinition>>,
    dimensions: HashMap<String, Arc<dyn DimensionDefinition>>,
}

impl InMemorySemanticRegistry {
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
            dimensions: HashMap::new(),
        }
    }

    pub fn register_metric(&mut self, metric: Arc<dyn MetricDefinition>) {
        self.metrics.insert(metric.name().to_string(), metric);
    }

    pub fn register_dimension(&mut self, dimension: Arc<dyn DimensionDefinition>) {
        self.dimensions.insert(dimension.name().to_string(), dimension);
    }

    pub fn register_metric_boxed(&mut self, metric: Box<dyn MetricDefinition>) {
        self.metrics.insert(metric.name().to_string(), Arc::from(metric));
    }

    pub fn register_dimension_boxed(&mut self, dimension: Box<dyn DimensionDefinition>) {
        self.dimensions.insert(dimension.name().to_string(), Arc::from(dimension));
    }
}

impl Default for InMemorySemanticRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticRegistry for InMemorySemanticRegistry {
    fn metric(&self, name: &str) -> Option<Arc<dyn MetricDefinition>> {
        // Try exact match first
        if let Some(m) = self.metrics.get(name) {
            return Some(Arc::clone(m));
        }

        // Try case-insensitive match
        for (key, metric) in &self.metrics {
            if key.eq_ignore_ascii_case(name) {
                return Some(Arc::clone(metric));
            }
        }

        None
    }

    fn dimension(&self, name: &str) -> Option<Arc<dyn DimensionDefinition>> {
        // Try exact match first
        if let Some(d) = self.dimensions.get(name) {
            return Some(Arc::clone(d));
        }

        // Try case-insensitive match
        for (key, dimension) in &self.dimensions {
            if key.eq_ignore_ascii_case(name) {
                return Some(Arc::clone(dimension));
            }
        }

        None
    }

    fn list_metrics(&self) -> Vec<String> {
        self.metrics.keys().cloned().collect()
    }

    fn list_dimensions(&self) -> Vec<String> {
        self.dimensions.keys().cloned().collect()
    }

    fn resolve_joins_for_query(
        &self,
        metric_name: &str,
        dimension_names: &[String],
    ) -> Result<Vec<JoinEdge>> {
        // Get metric
        let metric = self
            .metric(metric_name)
            .ok_or_else(|| RcaError::Execution(format!("Metric '{}' not found", metric_name)))?;

        // Get dimensions
        let mut dimension_objs: Vec<Arc<dyn DimensionDefinition>> = Vec::new();
        for dim_name in dimension_names {
            let dim = self
                .dimension(dim_name)
                .ok_or_else(|| RcaError::Execution(format!("Dimension '{}' not found", dim_name)))?;
            dimension_objs.push(dim);
        }

        // Validate metric-dimension compatibility
        let allowed_dims: HashSet<String> = metric.allowed_dimensions().iter().cloned().collect();
        for dim in &dimension_objs {
            if !allowed_dims.contains(dim.name()) {
                return Err(RcaError::Execution(format!(
                    "Dimension '{}' is not allowed for metric '{}'. Allowed dimensions: {:?}",
                    dim.name(),
                    metric_name,
                    metric.allowed_dimensions()
                )));
            }
        }

        // Collect join paths from dimensions
        let join_paths: Vec<&[JoinEdge]> = dimension_objs
            .iter()
            .map(|d| d.join_path())
            .collect();

        // Resolve joins
        resolve_joins(metric.base_table(), &join_paths)
    }
}

use std::collections::HashSet;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic::metric::{Aggregation, MetricPolicy, SemanticMetric, TimeGrain};

    #[test]
    fn test_registry_operations() {
        let mut registry = InMemorySemanticRegistry::new();

        // Register a metric
        let metric = Arc::new(SemanticMetric::new(
            "active_users".to_string(),
            "Count of active users".to_string(),
            Aggregation::CountDistinct,
            "user_sessions".to_string(),
            TimeGrain::Day,
            "COUNT(DISTINCT user_id)".to_string(),
        ));

        registry.register_metric(metric);

        assert!(registry.metric("active_users").is_some());
        assert!(registry.metric("unknown").is_none());
        assert_eq!(registry.list_metrics(), vec!["active_users"]);
    }
}





