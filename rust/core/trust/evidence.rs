//! Evidence Storage
//! 
//! Stores evidence of RCA execution for auditability and reproducibility.
//! Records inputs, outputs, and intermediate results.

use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::fs;
use chrono::{DateTime, Utc};

/// Evidence record for a single RCA execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceRecord {
    /// Unique execution ID
    pub execution_id: String,
    
    /// Timestamp of execution
    pub timestamp: DateTime<Utc>,
    
    /// Problem description
    pub problem_description: String,
    
    /// Input parameters
    pub inputs: ExecutionInputs,
    
    /// Output results
    pub outputs: ExecutionOutputs,
    
    /// Intermediate results (hashes/summaries to save space)
    pub intermediates: HashMap<String, String>,
    
    /// Metadata
    pub metadata: HashMap<String, String>,
}

/// Execution inputs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionInputs {
    /// System A name
    pub system_a: String,
    
    /// System B name
    pub system_b: String,
    
    /// Metric name
    pub metric: String,
    
    /// Rule IDs used
    pub rule_ids: Vec<String>,
    
    /// Value columns
    pub value_columns: Vec<String>,
    
    /// Reported mismatch
    pub reported_mismatch: f64,
    
    /// Additional parameters
    pub parameters: HashMap<String, serde_json::Value>,
}

/// Execution outputs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOutputs {
    /// Summary statistics
    pub summary: OutputSummary,
    
    /// Reconciliation result
    pub reconciliation_passes: bool,
    
    /// Number of root causes identified
    pub root_cause_count: usize,
    
    /// Output file paths (if saved)
    pub output_files: Vec<PathBuf>,
}

/// Output summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputSummary {
    pub total_rows: usize,
    pub missing_left_count: usize,
    pub missing_right_count: usize,
    pub mismatch_count: usize,
    pub aggregate_mismatch: f64,
}

/// Evidence store
pub struct EvidenceStore {
    storage_dir: PathBuf,
}

impl EvidenceStore {
    /// Create a new evidence store
    pub fn new(storage_dir: PathBuf) -> Self {
        // Create directory if it doesn't exist
        if !storage_dir.exists() {
            fs::create_dir_all(&storage_dir).ok();
        }
        
        Self { storage_dir }
    }
    
    /// Store evidence record
    pub fn store(&self, record: &EvidenceRecord) -> Result<()> {
        let file_path = self.storage_dir.join(format!("{}.json", record.execution_id));
        
        let json = serde_json::to_string_pretty(record)
            .map_err(|e| RcaError::Execution(format!("Failed to serialize evidence: {}", e)))?;
        
        fs::write(&file_path, json)
            .map_err(|e| RcaError::Execution(format!("Failed to write evidence: {}", e)))?;
        
        Ok(())
    }
    
    /// Load evidence record
    pub fn load(&self, execution_id: &str) -> Result<EvidenceRecord> {
        let file_path = self.storage_dir.join(format!("{}.json", execution_id));
        
        let json = fs::read_to_string(&file_path)
            .map_err(|e| RcaError::Execution(format!("Failed to read evidence: {}", e)))?;
        
        let record: EvidenceRecord = serde_json::from_str(&json)
            .map_err(|e| RcaError::Execution(format!("Failed to parse evidence: {}", e)))?;
        
        Ok(record)
    }
    
    /// List all execution IDs
    pub fn list_executions(&self) -> Result<Vec<String>> {
        let mut execution_ids = Vec::new();
        
        if !self.storage_dir.exists() {
            return Ok(execution_ids);
        }
        
        for entry in fs::read_dir(&self.storage_dir)
            .map_err(|e| RcaError::Execution(format!("Failed to read storage directory: {}", e)))? {
            let entry = entry
                .map_err(|e| RcaError::Execution(format!("Failed to read entry: {}", e)))?;
            
            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.ends_with(".json") {
                    if let Some(id) = file_name.strip_suffix(".json") {
                        execution_ids.push(id.to_string());
                    }
                }
            }
        }
        
        Ok(execution_ids)
    }
    
    /// Generate execution ID
    pub fn generate_execution_id(&self) -> String {
        use uuid::Uuid;
        Uuid::new_v4().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    
    #[test]
    fn test_evidence_store() {
        // Use a temporary directory for testing
        let temp_dir = std::env::temp_dir().join("rca_evidence_test");
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).ok();
        }
        fs::create_dir_all(&temp_dir).unwrap();
        
        let store = EvidenceStore::new(temp_dir.clone());
        
        let record = EvidenceRecord {
            execution_id: "test-123".to_string(),
            timestamp: Utc::now(),
            problem_description: "Test problem".to_string(),
            inputs: ExecutionInputs {
                system_a: "system_a".to_string(),
                system_b: "system_b".to_string(),
                metric: "tos".to_string(),
                rule_ids: vec!["rule1".to_string()],
                value_columns: vec!["value".to_string()],
                reported_mismatch: 100.0,
                parameters: HashMap::new(),
            },
            outputs: ExecutionOutputs {
                summary: OutputSummary {
                    total_rows: 100,
                    missing_left_count: 10,
                    missing_right_count: 5,
                    mismatch_count: 15,
                    aggregate_mismatch: 100.0,
                },
                reconciliation_passes: true,
                root_cause_count: 5,
                output_files: vec![],
            },
            intermediates: HashMap::new(),
            metadata: HashMap::new(),
        };
        
        store.store(&record).unwrap();
        let loaded = store.load("test-123").unwrap();
        
        assert_eq!(loaded.execution_id, "test-123");
        assert_eq!(loaded.problem_description, "Test problem");
        
        // Cleanup
        fs::remove_dir_all(&temp_dir).ok();
    }
}

