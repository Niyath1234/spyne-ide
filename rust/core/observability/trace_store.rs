//! Trace Store
//! 
//! Stores execution traces for debugging and observability.

use crate::core::observability::execution_trace::ExecutionTrace;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// In-memory trace store
#[derive(Clone)]
pub struct TraceStore {
    traces: Arc<RwLock<HashMap<String, ExecutionTrace>>>,
}

impl TraceStore {
    /// Create a new trace store
    pub fn new() -> Self {
        Self {
            traces: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store an execution trace
    pub fn store(&self, trace: ExecutionTrace) {
        let request_id = trace.request_id.clone();
        if let Ok(mut traces) = self.traces.write() {
            traces.insert(request_id, trace);
        }
    }

    /// Retrieve an execution trace by request ID
    pub fn get(&self, request_id: &str) -> Option<ExecutionTrace> {
        if let Ok(traces) = self.traces.read() {
            traces.get(request_id).cloned()
        } else {
            None
        }
    }

    /// List all request IDs
    pub fn list_request_ids(&self) -> Vec<String> {
        if let Ok(traces) = self.traces.read() {
            traces.keys().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Clear all traces (useful for testing or memory management)
    pub fn clear(&self) {
        if let Ok(mut traces) = self.traces.write() {
            traces.clear();
        }
    }

    /// Get trace count
    pub fn count(&self) -> usize {
        if let Ok(traces) = self.traces.read() {
            traces.len()
        } else {
            0
        }
    }
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new()
    }
}

// Global trace store instance
use lazy_static::lazy_static;

lazy_static! {
    pub static ref GLOBAL_TRACE_STORE: TraceStore = TraceStore::new();
}

