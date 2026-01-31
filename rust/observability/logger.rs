//! Execution Logger
//! 
//! Structured logging for query execution, metrics usage, and system events.

use crate::error::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Log entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
    Debug,
}

/// Query execution log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryLogEntry {
    pub timestamp: DateTime<Utc>,
    pub query_id: String,
    pub query_text: String,
    pub user_id: Option<String>,
    pub user_role: Option<String>,
    pub metrics_used: Vec<String>,
    pub dimensions_used: Vec<String>,
    pub execution_time_ms: u64,
    pub success: bool,
    pub error_message: Option<String>,
    pub sql_generated: Option<String>,
    pub rows_returned: Option<u64>,
}

/// Metric usage log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricUsageLog {
    pub timestamp: DateTime<Utc>,
    pub metric_name: String,
    pub user_id: Option<String>,
    pub user_role: Option<String>,
    pub query_id: String,
    pub execution_time_ms: u64,
}

/// Access control log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessControlLog {
    pub timestamp: DateTime<Utc>,
    pub user_id: String,
    pub user_role: String,
    pub metric_name: String,
    pub access_granted: bool,
    pub reason: Option<String>,
}

/// Execution logger
pub struct ExecutionLogger {
    log_file: Option<PathBuf>,
    query_logs: Arc<Mutex<Vec<QueryLogEntry>>>,
    metric_logs: Arc<Mutex<Vec<MetricUsageLog>>>,
    access_logs: Arc<Mutex<Vec<AccessControlLog>>>,
    max_in_memory_logs: usize,
}

impl ExecutionLogger {
    pub fn new(log_file: Option<PathBuf>, max_in_memory_logs: usize) -> Self {
        Self {
            log_file,
            query_logs: Arc::new(Mutex::new(Vec::new())),
            metric_logs: Arc::new(Mutex::new(Vec::new())),
            access_logs: Arc::new(Mutex::new(Vec::new())),
            max_in_memory_logs,
        }
    }

    /// Log a query execution
    pub fn log_query(&self, entry: QueryLogEntry) -> Result<()> {
        // Add to in-memory logs
        {
            let mut logs = self.query_logs.lock().unwrap();
            logs.push(entry.clone());
            if logs.len() > self.max_in_memory_logs {
                logs.remove(0);
            }
        }

        // Write to file if configured
        if let Some(ref log_file) = self.log_file {
            self.write_query_log(entry)?;
        }

        Ok(())
    }

    /// Log metric usage
    pub fn log_metric_usage(&self, entry: MetricUsageLog) -> Result<()> {
        {
            let mut logs = self.metric_logs.lock().unwrap();
            logs.push(entry.clone());
            if logs.len() > self.max_in_memory_logs {
                logs.remove(0);
            }
        }

        if let Some(ref log_file) = self.log_file {
            self.write_metric_log(entry)?;
        }

        Ok(())
    }

    /// Log access control event
    pub fn log_access_control(&self, entry: AccessControlLog) -> Result<()> {
        {
            let mut logs = self.access_logs.lock().unwrap();
            logs.push(entry.clone());
            if logs.len() > self.max_in_memory_logs {
                logs.remove(0);
            }
        }

        if let Some(ref log_file) = self.log_file {
            self.write_access_log(entry)?;
        }

        Ok(())
    }

    /// Write query log to file
    fn write_query_log(&self, entry: QueryLogEntry) -> Result<()> {
        if let Some(ref log_file) = self.log_file {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_file)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to open log file: {}", e)))?;

            let json = serde_json::to_string(&entry)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to serialize log: {}", e)))?;

            writeln!(file, "{}", json)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to write log: {}", e)))?;
        }

        Ok(())
    }

    /// Write metric log to file
    fn write_metric_log(&self, entry: MetricUsageLog) -> Result<()> {
        if let Some(ref log_file) = self.log_file {
            let metric_log_file = log_file.parent()
                .map(|p| p.join("metric_usage.log"))
                .unwrap_or_else(|| PathBuf::from("metric_usage.log"));

            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&metric_log_file)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to open metric log file: {}", e)))?;

            let json = serde_json::to_string(&entry)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to serialize log: {}", e)))?;

            writeln!(file, "{}", json)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to write log: {}", e)))?;
        }

        Ok(())
    }

    /// Write access control log to file
    fn write_access_log(&self, entry: AccessControlLog) -> Result<()> {
        if let Some(ref log_file) = self.log_file {
            let access_log_file = log_file.parent()
                .map(|p| p.join("access_control.log"))
                .unwrap_or_else(|| PathBuf::from("access_control.log"));

            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&access_log_file)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to open access log file: {}", e)))?;

            let json = serde_json::to_string(&entry)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to serialize log: {}", e)))?;

            writeln!(file, "{}", json)
                .map_err(|e| crate::error::RcaError::Execution(format!("Failed to write log: {}", e)))?;
        }

        Ok(())
    }

    /// Get recent query logs
    pub fn get_recent_queries(&self, limit: usize) -> Vec<QueryLogEntry> {
        let logs = self.query_logs.lock().unwrap();
        logs.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get metric usage statistics
    pub fn get_metric_usage_stats(&self) -> HashMap<String, u64> {
        let logs = self.metric_logs.lock().unwrap();
        let mut stats = HashMap::new();
        for log in logs.iter() {
            *stats.entry(log.metric_name.clone()).or_insert(0) += 1;
        }
        stats
    }

    /// Get access control statistics
    pub fn get_access_stats(&self) -> (u64, u64) {
        let logs = self.access_logs.lock().unwrap();
        let granted = logs.iter().filter(|l| l.access_granted).count() as u64;
        let denied = logs.iter().filter(|l| !l.access_granted).count() as u64;
        (granted, denied)
    }

    /// Export all logs to JSON
    pub fn export_logs(&self) -> Result<String> {
        let query_logs = self.query_logs.lock().unwrap().clone();
        let metric_logs = self.metric_logs.lock().unwrap().clone();
        let access_logs = self.access_logs.lock().unwrap().clone();

        let export = serde_json::json!({
            "query_logs": query_logs,
            "metric_logs": metric_logs,
            "access_logs": access_logs,
            "exported_at": Utc::now().to_rfc3339()
        });

        serde_json::to_string_pretty(&export)
            .map_err(|e| crate::error::RcaError::Execution(format!("Failed to serialize logs: {}", e)))
    }
}

impl Default for ExecutionLogger {
    fn default() -> Self {
        Self::new(None, 1000)
    }
}





