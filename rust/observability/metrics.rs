//! System Metrics
//! 
//! Track system performance and usage metrics.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// System metrics tracker
pub struct SystemMetrics {
    metric_usage: Arc<Mutex<HashMap<String, u64>>>,
    dimension_usage: Arc<Mutex<HashMap<String, u64>>>,
    error_counts: Arc<Mutex<HashMap<String, u64>>>,
    avg_execution_time: Arc<Mutex<HashMap<String, f64>>>,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self {
            metric_usage: Arc::new(Mutex::new(HashMap::new())),
            dimension_usage: Arc::new(Mutex::new(HashMap::new())),
            error_counts: Arc::new(Mutex::new(HashMap::new())),
            avg_execution_time: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn record_metric_usage(&self, metric_name: &str) {
        let mut usage = self.metric_usage.lock().unwrap();
        *usage.entry(metric_name.to_string()).or_insert(0) += 1;
    }

    pub fn record_dimension_usage(&self, dimension_name: &str) {
        let mut usage = self.dimension_usage.lock().unwrap();
        *usage.entry(dimension_name.to_string()).or_insert(0) += 1;
    }

    pub fn record_error(&self, error_class: &str) {
        let mut errors = self.error_counts.lock().unwrap();
        *errors.entry(error_class.to_string()).or_insert(0) += 1;
    }

    pub fn record_execution_time(&self, query_type: &str, time_ms: u64) {
        let mut avg_times = self.avg_execution_time.lock().unwrap();
        let entry = avg_times.entry(query_type.to_string()).or_insert(0.0);
        // Simple moving average (would be better with count)
        *entry = (*entry + time_ms as f64) / 2.0;
    }

    pub fn get_metric_usage(&self) -> HashMap<String, u64> {
        self.metric_usage.lock().unwrap().clone()
    }

    pub fn get_dimension_usage(&self) -> HashMap<String, u64> {
        self.dimension_usage.lock().unwrap().clone()
    }

    pub fn get_error_counts(&self) -> HashMap<String, u64> {
        self.error_counts.lock().unwrap().clone()
    }
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self::new()
    }
}





