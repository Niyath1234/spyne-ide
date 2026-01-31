//! Failure Analyzer
//! 
//! Analyzes execution logs to identify patterns and generate regression tests.

use crate::observability::execution_log::ExecutionLog;
use std::collections::HashMap;

/// Failure pattern identified from logs
#[derive(Debug, Clone)]
pub struct FailurePattern {
    pub error_class: String,
    pub frequency: usize,
    pub example_queries: Vec<String>,
    pub suggested_fix: String,
}

/// Test case generated from successful retries
#[derive(Debug, Clone)]
pub struct TestCase {
    pub query: String,
    pub expected_intent: String, // JSON representation
    pub description: String,
}

/// Failure analyzer
pub struct FailureAnalyzer {
    logs: Vec<ExecutionLog>,
}

impl FailureAnalyzer {
    pub fn new(logs: Vec<ExecutionLog>) -> Self {
        Self { logs }
    }

    /// Analyze failure patterns
    pub fn analyze_patterns(&self) -> Vec<FailurePattern> {
        let mut error_counts: HashMap<String, Vec<String>> = HashMap::new();

        for log in &self.logs {
            if !log.success {
                if let Some(ref error_class) = log.error_class {
                    let error_str = format!("{:?}", error_class);
                    error_counts
                        .entry(error_str)
                        .or_insert_with(Vec::new)
                        .push(log.user_query.clone());
                }
            }
        }

        error_counts
            .into_iter()
            .map(|(error_class, queries)| {
                let frequency = queries.len();
                let example_queries = queries.into_iter().take(3).collect();
                let suggested_fix = self.suggest_fix(&error_class);

                FailurePattern {
                    error_class,
                    frequency,
                    example_queries,
                    suggested_fix,
                }
            })
            .collect()
    }

    /// Generate regression tests from successful retries
    pub fn generate_regression_tests(&self) -> Vec<TestCase> {
        let mut tests = Vec::new();

        // Find queries that succeeded after retries
        let mut successful_queries: HashMap<String, &ExecutionLog> = HashMap::new();

        for log in &self.logs {
            if log.success && log.attempt > 1 {
                // This was a successful retry
                if !successful_queries.contains_key(&log.query_id) {
                    successful_queries.insert(log.query_id.clone(), log);
                }
            }
        }

        for log in successful_queries.values() {
            tests.push(TestCase {
                query: log.user_query.clone(),
                expected_intent: serde_json::to_string(&log.intent).unwrap_or_default(),
                description: format!(
                    "Regression test for query that succeeded after {} attempts",
                    log.attempt
                ),
            });
        }

        tests
    }

    fn suggest_fix(&self, error_class: &str) -> String {
        match error_class {
            s if s.contains("MetricNotFound") => {
                "Ensure metric names match exactly with semantic registry".to_string()
            }
            s if s.contains("DimensionNotAllowed") => {
                "Check metric's allowed_dimensions list and use only valid dimensions".to_string()
            }
            s if s.contains("ColumnNotFound") => {
                "Verify column names exist in the referenced tables".to_string()
            }
            s if s.contains("JoinPathFailure") => {
                "Ensure dimensions have valid join paths from metric's base table".to_string()
            }
            _ => "Review intent structure and schema compatibility".to_string(),
        }
    }
}





