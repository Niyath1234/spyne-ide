//! Execution Logging
//! 
//! Structured logging for execution attempts.

use crate::execution_loop::error_classifier::SqlErrorClass;
use crate::intent::SemanticSqlIntent;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Execution log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionLog {
    pub query_id: String,
    pub user_query: String,
    pub attempt: u8,
    pub intent: SemanticSqlIntent,
    pub error_class: Option<SqlErrorClass>,
    pub compiler_error: Option<String>,
    pub db_error: Option<String>,
    pub final_sql: Option<String>,
    pub execution_time_ms: u64,
    pub success: bool,
    pub timestamp: u64,
}

impl ExecutionLog {
    pub fn new(query_id: String, user_query: String) -> Self {
        Self {
            query_id,
            user_query,
            attempt: 0,
            intent: SemanticSqlIntent::new(vec![]),
            error_class: None,
            compiler_error: None,
            db_error: None,
            final_sql: None,
            execution_time_ms: 0,
            success: false,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub fn with_attempt(mut self, attempt: u8) -> Self {
        self.attempt = attempt;
        self
    }

    pub fn with_intent(mut self, intent: SemanticSqlIntent) -> Self {
        self.intent = intent;
        self
    }

    pub fn with_error(mut self, error_class: SqlErrorClass, error_msg: String) -> Self {
        self.error_class = Some(error_class);
        self.compiler_error = Some(error_msg);
        self.success = false;
        self
    }

    pub fn with_success(mut self, sql: String, execution_time_ms: u64) -> Self {
        self.final_sql = Some(sql);
        self.execution_time_ms = execution_time_ms;
        self.success = true;
        self
    }
}

/// Execution log store
pub struct ExecutionLogStore {
    logs: Vec<ExecutionLog>,
}

impl ExecutionLogStore {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    pub fn add_log(&mut self, log: ExecutionLog) {
        self.logs.push(log);
    }

    pub fn get_logs(&self) -> &[ExecutionLog] {
        &self.logs
    }

    pub fn get_logs_by_query(&self, query_id: &str) -> Vec<&ExecutionLog> {
        self.logs
            .iter()
            .filter(|log| log.query_id == query_id)
            .collect()
    }
}

impl Default for ExecutionLogStore {
    fn default() -> Self {
        Self::new()
    }
}





