//! OpenAI Function Schema Definitions
//! 
//! Defines function schemas for OpenAI function calling API.

use crate::intent::SemanticSqlIntent;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Function definition for OpenAI API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

/// Function call response from OpenAI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String, // JSON string
}

/// Message in OpenAI chat format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String, // "system", "user", "assistant", "function"
    pub content: Option<String>,
    pub function_call: Option<FunctionCall>,
    pub name: Option<String>, // For function role
}

/// Generate the function schema for SQL intent generation
/// 
/// ## Key Design Principle
/// 
/// **LLMs decide dimension usage (Filter vs Select), NOT join types.**
/// 
/// The LLM should output:
/// - Dimension names with usage semantics (how they're used in the query)
/// - NOT join types (LEFT vs INNER) - those are determined by the compiler
pub fn generate_sql_intent_function() -> FunctionDefinition {
    FunctionDefinition {
        name: "generate_sql_intent".to_string(),
        description: r#"Generate SQL intent from natural language query using semantic metrics and dimensions.

CRITICAL: You must specify dimension usage (select/filter/both), NOT join types.
- 'select': Dimension appears in SELECT/GROUP BY (augmentation)
- 'filter': Dimension appears in WHERE clause (restriction)  
- 'both': Dimension used for both filtering and selection

Join types (LEFT vs INNER) are automatically determined by the compiler based on:
1. Dimension usage (filter → INNER, select + optional → LEFT)
2. Schema metadata (cardinality, optionality)

DO NOT output join types - only dimension usage."#.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "metrics": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "List of metric names from semantic registry (e.g., 'tos', 'pos', 'active_users'). REQUIRED for METRIC queries, OPTIONAL for RELATIONAL queries (can be empty array)."
                },
                "dimension_intents": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Dimension name from semantic registry (e.g., 'date', 'region', 'customer_category')"
                            },
                            "usage": {
                                "type": "string",
                                "enum": ["select", "filter", "both"],
                                "description": "How the dimension is being used: 'select' = appears in SELECT/GROUP BY (augmentation), 'filter' = appears in WHERE (restriction), 'both' = used for both filtering and selection"
                            }
                        },
                        "required": ["name", "usage"]
                    },
                    "description": "Dimension intents with explicit usage semantics. The LLM should determine HOW each dimension is used (filtering vs augmentation), NOT which join type to use. Join types are determined automatically by the compiler based on usage + metadata."
                },
                "dimensions": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "Legacy field: List of dimension names for grouping. Use 'dimension_intents' instead for explicit usage semantics. If 'dimension_intents' is provided, this field is ignored."
                },
                "filters": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "dimension": {
                                "type": "string",
                                "description": "Dimension name to filter on"
                            },
                            "operator": {
                                "type": "string",
                                "enum": ["=", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"],
                                "description": "Filter operator"
                            },
                            "value": {
                                "description": "Filter value (can be string, number, boolean, or array for IN)"
                            },
                            "relative_date": {
                                "type": "string",
                                "description": "Relative date specification for date dimensions (e.g., '2_days_ago', 'yesterday', 'today', 'N_days_ago', 'N_days_from_now'). When specified, generates date arithmetic instead of direct value comparison."
                            }
                        },
                        "required": ["dimension", "operator"]
                    },
                    "description": "Filters to apply on dimensions"
                },
                "time_grain": {
                    "type": "string",
                    "enum": ["none", "day", "week", "month", "quarter", "year"],
                    "description": "Time grain for aggregation (optional, can be inferred from metric)"
                },
                "time_range": {
                    "type": "object",
                    "properties": {
                        "start": {
                            "type": "string",
                            "description": "Start date (ISO format or relative like 'start_of_year', 'today', 'yesterday')"
                        },
                        "end": {
                            "type": "string",
                            "description": "End date (ISO format or relative like 'end_of_year', 'today')"
                        }
                    },
                    "description": "Time range filter"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of rows to return"
                }
            },
            "required": []
        }),
    }
}

/// Parse function call arguments into SemanticSqlIntent
pub fn parse_function_call(function_call: &FunctionCall) -> Result<SemanticSqlIntent> {
    let intent: SemanticSqlIntent = serde_json::from_str(&function_call.arguments)
        .map_err(|e| crate::error::RcaError::Llm(format!(
            "Failed to parse function call arguments: {}. Arguments: {}",
            e, function_call.arguments
        )))?;
    
    Ok(intent)
}

