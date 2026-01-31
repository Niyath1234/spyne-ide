//! Join Graph Resolver
//! 
//! Deterministic join resolution from metric base table to dimensions.

use crate::error::{RcaError, Result};
use crate::semantic::dimension::DimensionDefinition;
use crate::semantic::join_graph::{resolve_joins, JoinEdge};
use crate::semantic::metric::MetricDefinition;
use crate::semantic::registry::SemanticRegistry;
use std::sync::Arc;

/// Join resolver for semantic intents
pub struct JoinResolver {
    semantic_registry: Arc<dyn SemanticRegistry>,
}

impl JoinResolver {
    pub fn new(semantic_registry: Arc<dyn SemanticRegistry>) -> Self {
        Self { semantic_registry }
    }

    /// Resolve joins for a metric and set of dimensions
    pub fn resolve_joins_for_intent(
        &self,
        metric_name: &str,
        dimension_names: &[String],
    ) -> Result<Vec<JoinEdge>> {
        // Use the registry's built-in join resolution
        self.semantic_registry
            .resolve_joins_for_query(metric_name, dimension_names)
    }
}





