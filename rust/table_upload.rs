///! Table Upload Module - Simplified table registration for RCA Engine
///! 
///! This module allows users to upload tables with minimal metadata:
///! 1. Table name
///! 2. Primary key columns (required)
///! 3. Column descriptions (optional - LLM will infer from names)
///! 
///! The system automatically:
///! - Detects system membership from table names in the question
///! - Infers relationships between tables
///! - Handles grain mismatches
///! - Generates complete metadata on-the-fly

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use polars::prelude::*;

/// Minimal table metadata required from user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleTableUpload {
    /// Name of the table (will be used to detect system membership)
    pub table_name: String,
    
    /// CSV file path
    pub csv_path: PathBuf,
    
    /// Primary key column(s) - REQUIRED
    pub primary_keys: Vec<String>,
    
    /// Optional column descriptions - if empty, LLM will infer from column names
    pub column_descriptions: HashMap<String, String>,
}

/// Table registry that stores uploaded tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRegistry {
    /// All uploaded tables
    pub tables: Vec<RegisteredTable>,
}

/// A registered table with inferred metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredTable {
    /// Original upload info
    pub upload: SimpleTableUpload,
    
    /// Inferred schema
    pub schema: TableSchema,
    
    /// Detected table prefix (e.g., "khatabook", "tb", "payment_system")
    pub table_prefix: Option<String>,
    
    /// Row count
    pub row_count: usize,
}

/// Inferred table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub description: Option<String>, // From user or inferred by LLM
}

impl TableRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
        }
    }
    
    /// Load registry from JSON file
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let registry: TableRegistry = serde_json::from_str(&content)?;
        Ok(registry)
    }
    
    /// Save registry to JSON file
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_json::to_string_pretty(&self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
    
    /// Register a new table
    pub fn register_table(&mut self, upload: SimpleTableUpload) -> Result<(), Box<dyn std::error::Error>> {
        // Validate primary keys are not empty
        if upload.primary_keys.is_empty() {
            return Err("Primary keys are required".into());
        }
        
        // Read CSV to infer schema and row count
        let df = LazyCsvReader::new(&upload.csv_path)
            .with_has_header(true)
            .finish()?
            .collect()?;
        
        let row_count = df.height();
        
        // Infer schema from dataframe
        let mut columns = Vec::new();
        for field in df.schema().iter_fields() {
            let field_name = field.name().to_string();
            let description = upload.column_descriptions.get(&field_name).cloned();
            
            columns.push(ColumnInfo {
                name: field_name,
                data_type: format!("{:?}", field.data_type()),
                description,
            });
        }
        
        // Detect table prefix (e.g., "khatabook_customers" -> prefix: "khatabook")
        let table_prefix = detect_table_prefix(&upload.table_name);
        
        let registered = RegisteredTable {
            upload,
            schema: TableSchema { columns },
            table_prefix,
            row_count,
        };
        
        self.tables.push(registered);
        
        Ok(())
    }
    
    /// Find all tables with a specific prefix
    pub fn find_tables_by_prefix(&self, prefix: &str) -> Vec<&RegisteredTable> {
        self.tables.iter()
            .filter(|t| t.table_prefix.as_deref() == Some(prefix))
            .collect()
    }
    
    /// Find all tables matching a pattern (case-insensitive)
    pub fn find_tables_by_pattern(&self, pattern: &str) -> Vec<&RegisteredTable> {
        let pattern_lower = pattern.to_lowercase();
        self.tables.iter()
            .filter(|t| t.upload.table_name.to_lowercase().contains(&pattern_lower))
            .collect()
    }
    
    /// Detect systems from a question
    /// Example: "TOS recon between khatabook and TB" -> ["khatabook", "tb"]
    pub fn detect_systems_from_question(&self, question: &str) -> Vec<String> {
        let question_lower = question.to_lowercase();
        let mut detected_systems = Vec::new();
        
        // Get all unique prefixes
        let mut prefixes: Vec<String> = self.tables.iter()
            .filter_map(|t| t.table_prefix.clone())
            .collect();
        prefixes.sort();
        prefixes.dedup();
        
        // Check which prefixes are mentioned in the question
        for prefix in prefixes {
            if question_lower.contains(&prefix.to_lowercase()) {
                detected_systems.push(prefix);
            }
        }
        
        detected_systems
    }
    
    /// Generate complete metadata for all tables
    /// This converts the simple uploads into full RCA Engine metadata format
    pub fn generate_full_metadata(&self) -> Result<String, Box<dyn std::error::Error>> {
        use serde_json::json;
        
        let mut tables_json = Vec::new();
        
        for registered in &self.tables {
            let mut columns_json = Vec::new();
            
            for col in &registered.schema.columns {
                columns_json.push(json!({
                    "name": col.name,
                    "type": col.data_type.to_lowercase(),
                    "description": col.description.as_deref().unwrap_or(&col.name),
                }));
            }
            
            tables_json.push(json!({
                "name": registered.upload.table_name,
                "path": registered.upload.csv_path.to_string_lossy(),
                "columns": columns_json,
                "grain": registered.upload.primary_keys,
                "labels": vec![registered.table_prefix.as_deref().unwrap_or("default")],
                "system": registered.table_prefix.as_deref().unwrap_or("default"),
            }));
        }
        
        let metadata = json!({
            "tables": tables_json
        });
        
        Ok(serde_json::to_string_pretty(&metadata)?)
    }
    
    /// Auto-generate simple business rules based on column patterns
    /// This creates default rules like "sum of amount columns" for common patterns
    pub fn generate_default_rules(&self, metric_name: &str) -> Vec<String> {
        let mut rules = Vec::new();
        
        // Get unique system prefixes
        let mut systems: Vec<String> = self.tables.iter()
            .filter_map(|t| t.table_prefix.clone())
            .collect();
        systems.sort();
        systems.dedup();
        
        // For each system, find relevant columns
        for system in systems {
            let system_tables = self.find_tables_by_prefix(&system);
            
            // Look for amount/balance columns
            for table in system_tables {
                for col in &table.schema.columns {
                    let col_lower = col.name.to_lowercase();
                    
                    // Match common metric patterns
                    if (metric_name.to_lowercase().contains("outstanding") || 
                        metric_name.to_lowercase().contains("tos")) &&
                       (col_lower.contains("outstanding") || col_lower.contains("balance")) {
                        rules.push(format!("System {}: Sum of {} from {}", 
                            system, col.name, table.upload.table_name));
                    } else if metric_name.to_lowercase().contains("recovery") &&
                             (col_lower.contains("paid") || col_lower.contains("recovery") || col_lower.contains("payment")) {
                        rules.push(format!("System {}: Sum of {} from {}",
                            system, col.name, table.upload.table_name));
                    } else if metric_name.to_lowercase().contains("disbursement") &&
                             col_lower.contains("disburs") {
                        rules.push(format!("System {}: Sum of {} from {}",
                            system, col.name, table.upload.table_name));
                    }
                }
            }
        }
        
        rules
    }
}

/// Detect table prefix from table name
/// Examples:
/// - "khatabook_customers" -> Some("khatabook")
/// - "tb_loans" -> Some("tb")
/// - "system_a_loans" -> Some("system_a")
/// - "system_b_customer_summary" -> Some("system_b")
fn detect_table_prefix(table_name: &str) -> Option<String> {
    // Split by underscores
    let parts: Vec<&str> = table_name.split('_').collect();
    
    if parts.len() < 2 {
        return None; // Need at least prefix_tablename
    }
    
    // Special handling for "system_a", "system_b" style naming
    if parts.len() >= 2 && parts[0] == "system" && (parts[1] == "a" || parts[1] == "b") {
        return Some(format!("{}_{}", parts[0], parts[1]));
    }
    
    // For other cases, try to intelligently detect the prefix
    // If we have "prefix_tablename" format, use first part
    // If we have "prefix_system_tablename", use first two parts
    
    // Check if the last part looks like a common table name
    let last_part = parts[parts.len() - 1].to_lowercase();
    let common_table_words = [
        "summary", "details", "transactions", "mapping", "schedule",
        "accruals", "fees", "penalties", "customers", "loans", "payments"
    ];
    
    if common_table_words.iter().any(|w| last_part.contains(w)) {
        // Last part is likely the table name, everything before is prefix
        let prefix_parts = &parts[..parts.len() - 1];
        if !prefix_parts.is_empty() {
            return Some(prefix_parts.join("_"));
        }
    }
    
    // Default: use first part only
    let prefix = parts[0];
    if prefix.len() >= 2 {
        Some(prefix.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_detect_table_prefix() {
        assert_eq!(detect_table_prefix("khatabook_customers"), Some("khatabook".to_string()));
        assert_eq!(detect_table_prefix("tb_loans"), Some("tb".to_string()));
        assert_eq!(detect_table_prefix("payment_system_transactions"), Some("payment".to_string()));
        assert_eq!(detect_table_prefix("customers"), None);
    }
    
    #[test]
    fn test_detect_systems_from_question() {
        let mut registry = TableRegistry::new();
        
        // Register some tables
        let upload1 = SimpleTableUpload {
            table_name: "khatabook_customers".to_string(),
            csv_path: PathBuf::from("test.csv"),
            primary_keys: vec!["customer_id".to_string()],
            column_descriptions: HashMap::new(),
        };
        
        let upload2 = SimpleTableUpload {
            table_name: "tb_loans".to_string(),
            csv_path: PathBuf::from("test2.csv"),
            primary_keys: vec!["loan_id".to_string()],
            column_descriptions: HashMap::new(),
        };
        
        // Note: These tests would need actual CSV files to work
        // registry.register_table(upload1).unwrap();
        // registry.register_table(upload2).unwrap();
        
        // Manually create registered tables for testing
        registry.tables.push(RegisteredTable {
            upload: upload1,
            schema: TableSchema { columns: vec![] },
            table_prefix: Some("khatabook".to_string()),
            row_count: 0,
        });
        
        registry.tables.push(RegisteredTable {
            upload: upload2,
            schema: TableSchema { columns: vec![] },
            table_prefix: Some("tb".to_string()),
            row_count: 0,
        });
        
        let systems = registry.detect_systems_from_question("TOS recon between khatabook and TB");
        assert_eq!(systems.len(), 2);
        assert!(systems.contains(&"khatabook".to_string()));
        assert!(systems.contains(&"tb".to_string()));
    }
}

