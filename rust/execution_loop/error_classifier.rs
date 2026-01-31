//! Error Classifier
//! 
//! Classifies SQL errors into a taxonomy for recovery.

use crate::error::RcaError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// SQL error classification taxonomy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SqlErrorClass {
    MetricNotFound,
    DimensionNotAllowed,
    ColumnNotFound,
    TableNotFound,
    AmbiguousColumn,
    InvalidAggregation,
    TimeGrainMismatch,
    JoinPathFailure,
    ExecutionError(String),
    CompilerError(String),
}

impl fmt::Display for SqlErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlErrorClass::MetricNotFound => write!(f, "MetricNotFound"),
            SqlErrorClass::DimensionNotAllowed => write!(f, "DimensionNotAllowed"),
            SqlErrorClass::ColumnNotFound => write!(f, "ColumnNotFound"),
            SqlErrorClass::TableNotFound => write!(f, "TableNotFound"),
            SqlErrorClass::AmbiguousColumn => write!(f, "AmbiguousColumn"),
            SqlErrorClass::InvalidAggregation => write!(f, "InvalidAggregation"),
            SqlErrorClass::TimeGrainMismatch => write!(f, "TimeGrainMismatch"),
            SqlErrorClass::JoinPathFailure => write!(f, "JoinPathFailure"),
            SqlErrorClass::ExecutionError(msg) => write!(f, "ExecutionError({})", msg),
            SqlErrorClass::CompilerError(msg) => write!(f, "CompilerError({})", msg),
        }
    }
}

/// Error classifier
pub struct ErrorClassifier;

impl ErrorClassifier {
    pub fn new() -> Self {
        Self
    }

    /// Classify an error into the taxonomy
    pub fn classify(&self, error: &RcaError) -> SqlErrorClass {
        let error_msg = error.to_string().to_lowercase();

        // Pattern matching on error messages
        if error_msg.contains("metric") && (error_msg.contains("not found") || error_msg.contains("unknown")) {
            return SqlErrorClass::MetricNotFound;
        }

        if error_msg.contains("dimension") && (error_msg.contains("not allowed") || error_msg.contains("cannot")) {
            return SqlErrorClass::DimensionNotAllowed;
        }

        if error_msg.contains("column") && (error_msg.contains("not found") || error_msg.contains("does not exist")) {
            return SqlErrorClass::ColumnNotFound;
        }

        if error_msg.contains("table") && (error_msg.contains("not found") || error_msg.contains("does not exist")) {
            return SqlErrorClass::TableNotFound;
        }

        if error_msg.contains("ambiguous") && error_msg.contains("column") {
            return SqlErrorClass::AmbiguousColumn;
        }

        if error_msg.contains("aggregation") || error_msg.contains("aggregate") {
            return SqlErrorClass::InvalidAggregation;
        }

        if error_msg.contains("time grain") || error_msg.contains("grain mismatch") {
            return SqlErrorClass::TimeGrainMismatch;
        }

        if error_msg.contains("join") && (error_msg.contains("failure") || error_msg.contains("unreachable") || error_msg.contains("cycle")) {
            return SqlErrorClass::JoinPathFailure;
        }

        if error_msg.contains("compiler") || error_msg.contains("compile") {
            return SqlErrorClass::CompilerError(error.to_string());
        }

        // Default to execution error
        SqlErrorClass::ExecutionError(error.to_string())
    }
}

impl Default for ErrorClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_metric_not_found() {
        let classifier = ErrorClassifier::new();
        let error = RcaError::Execution("Metric 'unknown_metric' not found".to_string());
        assert_eq!(classifier.classify(&error), SqlErrorClass::MetricNotFound);
    }

    #[test]
    fn test_classify_column_not_found() {
        let classifier = ErrorClassifier::new();
        let error = RcaError::Execution("Column 'user_id' does not exist".to_string());
        assert_eq!(classifier.classify(&error), SqlErrorClass::ColumnNotFound);
    }
}

