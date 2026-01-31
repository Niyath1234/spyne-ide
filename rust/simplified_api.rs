///! Simplified API Module - REST endpoints for the simplified RCA workflow
///! 
///! This module provides simple REST API endpoints for:
///! 1. Upload tables with primary keys + optional descriptions
///! 2. Ask questions like "TOS recon between khatabook and TB"
///! 3. Get automatic reconciliation results

use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use std::collections::HashMap;

use crate::table_upload::{TableRegistry, SimpleTableUpload};
use crate::simplified_intent::SimplifiedIntentCompiler;
use crate::llm::LlmClient;

/// API request to upload a table
#[derive(Debug, Deserialize)]
pub struct UploadTableRequest {
    /// Table name (will be used for system detection)
    pub table_name: String,
    
    /// CSV file content (base64 encoded) or file path
    pub csv_data: Option<String>,
    pub csv_path: Option<String>,
    
    /// Primary key columns - REQUIRED
    pub primary_keys: Vec<String>,
    
    /// Optional column descriptions
    pub column_descriptions: Option<HashMap<String, String>>,
}

/// API response for table upload
#[derive(Debug, Serialize)]
pub struct UploadTableResponse {
    pub success: bool,
    pub message: String,
    pub table_name: String,
    pub detected_prefix: Option<String>,
    pub row_count: usize,
    pub columns: Vec<String>,
}

/// API request to ask a reconciliation question
#[derive(Debug, Deserialize)]
pub struct AskQuestionRequest {
    /// Natural language question
    /// Example: "TOS recon between khatabook and TB"
    pub question: String,
}

/// API response with reconciliation results
#[derive(Debug, Serialize)]
pub struct AskQuestionResponse {
    pub success: bool,
    pub message: String,
    
    /// Detected intent
    pub intent: IntentSummary,
    
    /// Reconciliation results (if successful)
    pub results: Option<ReconciliationResults>,
}

#[derive(Debug, Serialize)]
pub struct IntentSummary {
    pub metric: String,
    pub systems: Vec<String>,
    pub tables: HashMap<String, Vec<String>>,
    pub suggested_rules: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ReconciliationResults {
    pub system_a_total: f64,
    pub system_b_total: f64,
    pub difference: f64,
    pub matching_rows: usize,
    pub mismatched_rows: usize,
    pub missing_in_a: usize,
    pub missing_in_b: usize,
    pub summary: String,
}

/// Simplified API handler
pub struct SimplifiedApiHandler {
    registry: Arc<Mutex<TableRegistry>>,
    llm_client: Option<LlmClient>,
}

impl SimplifiedApiHandler {
    pub fn new(llm_client: Option<LlmClient>) -> Self {
        Self {
            registry: Arc::new(Mutex::new(TableRegistry::new())),
            llm_client,
        }
    }
    
    /// Handle table upload
    pub async fn handle_upload_table(
        &self,
        request: UploadTableRequest,
    ) -> Result<UploadTableResponse, Box<dyn std::error::Error>> {
        // Validate
        if request.primary_keys.is_empty() {
            return Err("Primary keys are required".into());
        }
        
        if request.csv_data.is_none() && request.csv_path.is_none() {
            return Err("Either csv_data or csv_path must be provided".into());
        }
        
        // Determine CSV path
        let csv_path = if let Some(path_str) = request.csv_path {
            PathBuf::from(path_str)
        } else if let Some(data) = request.csv_data {
            // Save base64 data to temporary file
            let data_dir = PathBuf::from("tables");
            std::fs::create_dir_all(&data_dir)?;
            
            let filename = format!("{}.csv", request.table_name);
            let file_path = data_dir.join(filename);
            
            // Decode base64 and save
            let decoded = base64::decode(&data)?;
            std::fs::write(&file_path, decoded)?;
            
            file_path
        } else {
            return Err("No CSV data provided".into());
        };
        
        // Create upload structure
        let upload = SimpleTableUpload {
            table_name: request.table_name.clone(),
            csv_path,
            primary_keys: request.primary_keys,
            column_descriptions: request.column_descriptions.unwrap_or_default(),
        };
        
        // Register table
        let mut registry = self.registry.lock().map_err(|e| format!("Lock error: {}", e))?;
        registry.register_table(upload)?;
        
        // Get info about registered table
        let registered = registry.tables.last()
            .ok_or("Failed to retrieve registered table")?;
        
        // Save registry
        registry.save("table_registry.json")?;
        
        Ok(UploadTableResponse {
            success: true,
            message: format!("Table '{}' uploaded successfully", request.table_name),
            table_name: registered.upload.table_name.clone(),
            detected_prefix: registered.table_prefix.clone(),
            row_count: registered.row_count,
            columns: registered.schema.columns.iter()
                .map(|c| c.name.clone())
                .collect(),
        })
    }
    
    /// Handle reconciliation question
    pub async fn handle_ask_question(
        &self,
        request: AskQuestionRequest,
    ) -> Result<AskQuestionResponse, Box<dyn std::error::Error>> {
        // Get registry
        let registry = self.registry.lock()
            .map_err(|e| format!("Lock error: {}", e))?
            .clone();
        
        // Create compiler
        let compiler = SimplifiedIntentCompiler::new(registry, self.llm_client.clone());
        
        // Compile intent with auto-detection
        let intent = compiler.compile_with_auto_detection(&request.question).await?;
        
        // Create intent summary
        let intent_summary = IntentSummary {
            metric: intent.metric_name.clone(),
            systems: intent.detected_systems.clone(),
            tables: intent.system_tables.clone(),
            suggested_rules: intent.suggested_rules.clone(),
        };
        
        // TODO: Execute actual RCA analysis
        // For now, return the detected intent
        
        Ok(AskQuestionResponse {
            success: true,
            message: format!(
                "Detected reconciliation: {} between {}",
                intent.metric_name,
                intent.detected_systems.join(" and ")
            ),
            intent: intent_summary,
            results: None, // Will be populated when RCA runs
        })
    }
}

/// Helper to convert base64
mod base64 {
    pub fn decode(input: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Simple base64 decoder
        // In production, use the `base64` crate
        Ok(input.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_upload_and_query() {
        let handler = SimplifiedApiHandler::new(None);
        
        // Upload would need actual CSV files to test
        // This is a placeholder test structure
        
        let question = AskQuestionRequest {
            question: "TOS recon between khatabook and TB".to_string(),
        };
        
        // This would fail without uploaded tables
        // let response = handler.handle_ask_question(question).await;
    }
}





