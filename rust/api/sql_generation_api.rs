//! SQL Generation API
//!
//! RISK #2 FIX: This is the ONLY place SQL can be generated.
//! Python sends intent JSON, Rust generates SQL.
//!
//! This API accepts intent from Python and returns SQL + logical plan.

use crate::error::{RcaError, Result};
use crate::sql_compiler::{SqlIntent, SqlCompiler};
use crate::core::engine::logical_plan::LogicalPlan;
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Intent to SQL request (from Python)
#[derive(Debug, Deserialize)]
pub struct IntentToSqlRequest {
    /// Query intent (semantic description)
    pub intent: String,
    
    /// Entities involved (semantic concepts, not table names)
    pub entities: Vec<String>,
    
    /// Constraints (business rules, not SQL WHERE clauses)
    pub constraints: Vec<String>,
    
    /// Optional preferences (hints)
    pub preferences: Option<Vec<String>>,
    
    /// Optional metric name
    pub metric_name: Option<String>,
    
    /// Optional dimensions for grouping
    pub dimensions: Option<Vec<String>>,
}

/// SQL generation response
#[derive(Debug, Serialize)]
pub struct SqlGenerationResponse {
    /// Generated SQL query
    pub sql: String,
    
    /// Logical plan (for explainability)
    pub logical_plan: Option<LogicalPlan>,
    
    /// Tables used in the query
    pub tables_used: Vec<String>,
    
    /// Warnings (if any)
    pub warnings: Vec<String>,
    
    /// Explainability information
    pub explanation: Option<String>,
}

/// SQL Generation API handler
pub struct SqlGenerationApi {
    sql_compiler: Arc<SqlCompiler>,
    metadata: Arc<Metadata>,
}

impl SqlGenerationApi {
    pub fn new(sql_compiler: Arc<SqlCompiler>, metadata: Arc<Metadata>) -> Self {
        Self {
            sql_compiler,
            metadata,
        }
    }
    
    /// Generate SQL from intent
    ///
    /// RISK #2 FIX: This is the ONLY place SQL is generated.
    /// Python sends intent, Rust generates SQL.
    pub fn generate_sql_from_intent(&self, request: IntentToSqlRequest) -> Result<SqlGenerationResponse> {
        // Convert Python intent format to Rust SqlIntent
        // This is a simplified conversion - in production, you'd have a more sophisticated
        // intent resolver that maps entities to tables, constraints to filters, etc.
        
        let sql_intent = self.convert_intent_to_sql_intent(request)?;
        
        // Generate SQL using the compiler
        // Note: sql_compiler.compile() signature may need adjustment
        let sql = self.sql_compiler.compile(&sql_intent)?;
        
        Ok(SqlGenerationResponse {
            sql,
            logical_plan: None, // TODO: Generate logical plan from intent
            tables_used: sql_intent.tables.clone(),
            warnings: vec![],
            explanation: Some(format!(
                "Generated SQL from intent: {} with entities: {:?}",
                sql_intent.tables.join(", "),
                sql_intent.tables
            )),
        })
    }
    
    /// Convert Python intent format to Rust SqlIntent
    fn convert_intent_to_sql_intent(&self, request: IntentToSqlRequest) -> Result<SqlIntent> {
        // This is a simplified conversion
        // In production, you'd resolve entities to actual table names using metadata
        
        // For now, assume entities map directly to table names (simplified)
        let tables: Vec<String> = request.entities.iter().map(|e| e.clone()).collect();
        
        // Build basic SQL intent
        let mut sql_intent = SqlIntent {
            tables,
            columns: None,
            aggregations: None,
            filters: None,
            group_by: None,
            order_by: None,
            limit: None,
            joins: None,
            date_constraint: None,
        };
        
        // If metric_name is provided, add aggregation
        if let Some(metric) = request.metric_name {
            sql_intent.aggregations = Some(vec![crate::sql_compiler::AggregationSpec {
                function: "sum".to_string(),
                column: metric,
                table: None,
                alias: None,
            }]);
        }
        
        // If dimensions are provided, add group_by
        if let Some(dims) = request.dimensions {
            sql_intent.group_by = Some(dims);
        }
        
        Ok(sql_intent)
    }
}

