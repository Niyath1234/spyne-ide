//! Query Plan - Explicit query type and constraint declaration
//! 
//! This module implements the query planning step that determines:
//! 1. Query type (METRIC vs RELATIONAL vs HYBRID)
//! 2. Expected output cardinality
//! 3. Allowed operations
//! 4. Entities and domains involved
//! 
//! This constrains the LLM's action space BEFORE schema retrieval.

use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use crate::intent::function_schema::{ChatMessage, FunctionDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Query type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum QueryType {
    /// Query asks for aggregated metric values (SUM, COUNT, AVG, etc.)
    /// Example: "What is the total revenue?", "Show me the count of active users"
    Metric,
    /// Query asks for individual records/rows
    /// Example: "Show me all customers", "List orders with status pending"
    Relational,
    /// Query combines both - returns aggregated values but grouped by entities
    /// Example: "Show revenue by customer", "Count orders by region"
    Hybrid,
}

/// Output cardinality
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OutputCardinality {
    /// Single aggregated value (e.g., "What is the total?")
    SingleValue,
    /// Multiple rows (e.g., "Show me all customers")
    MultiRow,
}

/// Query Plan - explicit declaration of query intent and constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Query type classification
    pub query_type: QueryType,
    
    /// Expected output cardinality
    pub output_cardinality: OutputCardinality,
    
    /// Entities mentioned in the query (e.g., ["customer", "account", "loan"])
    pub entities: Vec<String>,
    
    /// Domains/contexts mentioned (e.g., ["financial", "support"])
    #[serde(default)]
    pub domains: Vec<String>,
    
    /// Entities to exclude (e.g., ["order", "support_ticket"] if query is about customers)
    #[serde(default)]
    pub exclude_entities: Vec<String>,
    
    /// Whether aggregations are allowed
    pub aggregations_allowed: bool,
    
    /// Whether GROUP BY is allowed
    pub group_by_allowed: bool,
    
    /// Whether JOINs are allowed
    pub joins_allowed: bool,
    
    /// Required columns mentioned in query (e.g., ["account_balance", "loan_amount"])
    #[serde(default)]
    pub required_columns: Vec<String>,
    
    /// Confidence score (0.0-1.0)
    #[serde(default)]
    pub confidence: f64,
}

impl QueryPlan {
    /// Create a new query plan
    pub fn new(
        query_type: QueryType,
        output_cardinality: OutputCardinality,
        entities: Vec<String>,
    ) -> Self {
        let aggregations_allowed = matches!(query_type, QueryType::Metric | QueryType::Hybrid);
        let group_by_allowed = matches!(query_type, QueryType::Hybrid);
        let joins_allowed = true; // Always allowed, but type depends on query_type
        
        Self {
            query_type,
            output_cardinality,
            entities,
            domains: Vec::new(),
            exclude_entities: Vec::new(),
            aggregations_allowed,
            group_by_allowed,
            joins_allowed,
            required_columns: Vec::new(),
            confidence: 1.0,
        }
    }
    
    /// Check if a table entity matches this plan
    pub fn matches_entity(&self, entity: &str) -> bool {
        if self.exclude_entities.iter().any(|e| entity.contains(e)) {
            return false;
        }
        
        if self.entities.is_empty() {
            return true; // No entity filter, allow all
        }
        
        self.entities.iter().any(|e| entity.contains(e))
    }
    
    /// Check if metrics should be shown
    pub fn should_show_metrics(&self) -> bool {
        matches!(self.query_type, QueryType::Metric | QueryType::Hybrid)
    }
    
    /// Check if tables should be shown
    pub fn should_show_tables(&self) -> bool {
        matches!(self.query_type, QueryType::Relational | QueryType::Hybrid)
    }
}

/// Generate query plan using LLM
pub async fn generate_query_plan(
    query: &str,
    llm: &LlmClient,
) -> Result<QueryPlan> {
    let function = create_query_plan_function();
    
    let prompt = format!(
        r#"You are a query planner. Analyze the user query and determine:

1. Query type: METRIC (aggregated values), RELATIONAL (individual records), or HYBRID (aggregated by groups)
2. Output cardinality: SINGLE_VALUE (one number) or MULTI_ROW (multiple rows)
3. Entities mentioned: Extract entity names like "customer", "account", "loan", "order", etc.
4. Domains/contexts: Extract domain names like "financial", "support", "sales", etc.
5. Exclude entities: Entities that should NOT be included (e.g., if query is about customers, exclude "order" entities)
6. Required columns: Column names explicitly mentioned (e.g., "account_balance", "loan_amount")
7. Allowed operations: Based on query type, determine if aggregations/GROUP BY/JOINs are allowed

Examples:
- "What is the total revenue?" → METRIC, SINGLE_VALUE, aggregations_allowed: true
- "Show me all customers" → RELATIONAL, MULTI_ROW, aggregations_allowed: false
- "Show revenue by region" → HYBRID, MULTI_ROW, aggregations_allowed: true, group_by_allowed: true
- "Show me customers with accounts" → RELATIONAL, MULTI_ROW, entities: ["customer", "account"], joins_allowed: true

USER QUERY: {}"#,
        query
    );
    
    let messages = vec![
        ChatMessage {
            role: "system".to_string(),
            content: Some(
                "You are a query planner. Use the create_query_plan function to analyze queries.".to_string(),
            ),
            function_call: None,
            name: None,
        },
        ChatMessage {
            role: "user".to_string(),
            content: Some(prompt),
            function_call: None,
            name: None,
        },
    ];
    
    let function_call = llm
        .call_llm_with_functions(&messages, &[function])
        .await?;
    
    let plan: QueryPlan = serde_json::from_str(&function_call.arguments)
        .map_err(|e| RcaError::Execution(format!("Failed to parse query plan: {}", e)))?;
    
    Ok(plan)
}

/// Create function schema for query planning
fn create_query_plan_function() -> FunctionDefinition {
    FunctionDefinition {
        name: "create_query_plan".to_string(),
        description: "Create a query plan that determines query type, entities, and allowed operations".to_string(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "query_type": {
                    "type": "string",
                    "enum": ["METRIC", "RELATIONAL", "HYBRID"],
                    "description": "Query type: METRIC (aggregated values), RELATIONAL (individual records), HYBRID (aggregated by groups)"
                },
                "output_cardinality": {
                    "type": "string",
                    "enum": ["SINGLE_VALUE", "MULTI_ROW"],
                    "description": "Expected output cardinality"
                },
                "entities": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Entity names mentioned in query (e.g., ['customer', 'account', 'loan'])"
                },
                "domains": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Domain/context names (e.g., ['financial', 'support'])",
                    "default": []
                },
                "exclude_entities": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Entities to exclude (e.g., ['order'] if query is about customers)",
                    "default": []
                },
                "aggregations_allowed": {
                    "type": "boolean",
                    "description": "Whether aggregations (SUM, COUNT, AVG) are allowed"
                },
                "group_by_allowed": {
                    "type": "boolean",
                    "description": "Whether GROUP BY is allowed"
                },
                "joins_allowed": {
                    "type": "boolean",
                    "description": "Whether JOINs are allowed (always true, but join type depends on query_type)"
                },
                "required_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Column names explicitly mentioned in query",
                    "default": []
                },
                "confidence": {
                    "type": "number",
                    "description": "Confidence score (0.0-1.0)",
                    "default": 1.0,
                    "minimum": 0.0,
                    "maximum": 1.0
                }
            },
            "required": ["query_type", "output_cardinality", "entities", "aggregations_allowed", "group_by_allowed", "joins_allowed"]
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query_plan_matches_entity() {
        let plan = QueryPlan::new(
            QueryType::Relational,
            OutputCardinality::MultiRow,
            vec!["customer".to_string(), "account".to_string()],
        );
        
        assert!(plan.matches_entity("customer"));
        assert!(plan.matches_entity("customer_master"));
        assert!(plan.matches_entity("account"));
        assert!(plan.matches_entity("customer_accounts"));
        assert!(!plan.matches_entity("loan"));
        assert!(!plan.matches_entity("order"));
    }
    
    #[test]
    fn test_query_plan_exclude_entities() {
        let mut plan = QueryPlan::new(
            QueryType::Relational,
            OutputCardinality::MultiRow,
            vec!["customer".to_string()],
        );
        plan.exclude_entities = vec!["order".to_string(), "support".to_string()];
        
        assert!(plan.matches_entity("customer"));
        assert!(plan.matches_entity("customer_master"));
        assert!(!plan.matches_entity("customer_orders"));
        assert!(!plan.matches_entity("customer_support"));
    }
}





