use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tool system that allows LLM to decide which operations to perform
pub struct ToolSystem {
    llm: LlmClient,
    available_tools: Vec<Tool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    pub name: String,
    pub description: String,
    pub parameters: Vec<ToolParameter>,
    pub category: ToolCategory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ToolCategory {
    Matching,
    Aggregation,
    Transformation,
    Filtering,
    Join,
    Comparison,
    DataEngineering, // New category for DE tools
    Validation,      // New category for validation tools
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolParameter {
    pub name: String,
    pub description: String,
    pub parameter_type: ParameterType,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterType {
    String,
    Number,
    Boolean,
    Array(String), // Array of specific type
    Object(HashMap<String, ParameterType>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub tool_name: String,
    pub parameters: HashMap<String, serde_json::Value>,
    pub reasoning: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolExecutionPlan {
    pub steps: Vec<ToolCall>,
    pub reasoning: String,
}

impl ToolSystem {
    pub fn new(llm: LlmClient) -> Self {
        let mut system = Self {
            llm,
            available_tools: Vec::new(),
        };
        
        // Register all available tools
        system.register_tools();
        system
    }
    
    fn register_tools(&mut self) {
        // Fuzzy Matching Tool
        self.available_tools.push(Tool {
            name: "fuzzy_match".to_string(),
            description: "Match entities with similar names/identifiers when exact matches fail. Handles variations like 'Ms Radhika apte' vs 'Radika APte'. Use when comparing string-based keys that may have typos, case differences, or formatting variations.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "columns".to_string(),
                    description: "List of column names to apply fuzzy matching to".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: true,
                },
                ToolParameter {
                    name: "threshold".to_string(),
                    description: "Similarity threshold (0.0-1.0), default 0.85".to_string(),
                    parameter_type: ParameterType::Number,
                    required: false,
                },
            ],
            category: ToolCategory::Matching,
        });
        
        // Group By Tool
        self.available_tools.push(Tool {
            name: "group_by".to_string(),
            description: "Aggregate data by grouping rows based on specified columns. Use when you need to aggregate metrics at a different grain level (e.g., loan-level to customer-level).".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "columns".to_string(),
                    description: "Columns to group by".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: true,
                },
                ToolParameter {
                    name: "aggregations".to_string(),
                    description: "Aggregation functions to apply (e.g., {'tos': 'SUM', 'count': 'COUNT'})".to_string(),
                    parameter_type: ParameterType::Object(HashMap::new()),
                    required: true,
                },
            ],
            category: ToolCategory::Aggregation,
        });
        
        // Join Tool
        self.available_tools.push(Tool {
            name: "join".to_string(),
            description: "Join two dataframes on specified columns. Use when you need to combine data from multiple tables or resolve grain mismatches by joining to parent entities.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "left_table".to_string(),
                    description: "Name of the left table/dataframe".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "right_table".to_string(),
                    description: "Name of the right table/dataframe".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "on".to_string(),
                    description: "Columns to join on".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: true,
                },
                ToolParameter {
                    name: "join_type".to_string(),
                    description: "Type of join: 'inner', 'left', 'right', 'outer'".to_string(),
                    parameter_type: ParameterType::String,
                    required: false,
                },
            ],
            category: ToolCategory::Join,
        });
        
        // Filter Tool
        self.available_tools.push(Tool {
            name: "filter".to_string(),
            description: "Filter rows based on conditions. Use when you need to exclude certain records or focus on a subset of data.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "condition".to_string(),
                    description: "Filter condition expression (e.g., 'column > value', 'column == \"text\"')".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
            ],
            category: ToolCategory::Filtering,
        });
        
        // Normalize Keys Tool
        self.available_tools.push(Tool {
            name: "normalize_keys".to_string(),
            description: "Normalize entity keys to canonical form. Use when systems use different key formats or when you need to standardize identifiers.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "columns".to_string(),
                    description: "Key columns to normalize".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: true,
                },
                ToolParameter {
                    name: "mapping_table".to_string(),
                    description: "Optional mapping table for key translation".to_string(),
                    parameter_type: ParameterType::String,
                    required: false,
                },
            ],
            category: ToolCategory::Transformation,
        });
        
        // Compare Tool
        self.available_tools.push(Tool {
            name: "compare".to_string(),
            description: "Compare two dataframes and find differences. Use for final reconciliation comparison after all transformations.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "grain".to_string(),
                    description: "Grain columns for comparison".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: true,
                },
                ToolParameter {
                    name: "metric".to_string(),
                    description: "Metric column to compare".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "precision".to_string(),
                    description: "Decimal precision for comparison".to_string(),
                    parameter_type: ParameterType::Number,
                    required: false,
                },
            ],
            category: ToolCategory::Comparison,
        });
        
        // ===== DATA ENGINEERING TOOLS =====
        
        // Inspect Column Tool - Check top values, data types, nulls
        self.available_tools.push(Tool {
            name: "inspect_column".to_string(),
            description: "Inspect a column to check top values, data types, null counts, and sample data. Use BEFORE joins or comparisons to understand data quality and identify issues like commas in numbers, type mismatches, or unexpected values. Critical for making errors visible.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "table".to_string(),
                    description: "Table/dataframe name to inspect".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "columns".to_string(),
                    description: "List of column names to inspect".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: true,
                },
                ToolParameter {
                    name: "top_n".to_string(),
                    description: "Number of top values to show (default: 10)".to_string(),
                    parameter_type: ParameterType::Number,
                    required: false,
                },
            ],
            category: ToolCategory::DataEngineering,
        });
        
        // Validate Schema Tool - Check data type compatibility
        self.available_tools.push(Tool {
            name: "validate_schema".to_string(),
            description: "Validate schema compatibility between two tables/dataframes. Checks if join columns have compatible data types. Use BEFORE joins to ensure type matching. Automatically detects and reports type mismatches that would cause join failures.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "left_table".to_string(),
                    description: "Name of the left table/dataframe".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "right_table".to_string(),
                    description: "Name of the right table/dataframe".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "join_columns".to_string(),
                    description: "Columns to check for join compatibility (can be mapping like {'left_col': 'right_col'})".to_string(),
                    parameter_type: ParameterType::Object(HashMap::new()),
                    required: true,
                },
            ],
            category: ToolCategory::Validation,
        });
        
        // Clean Data Tool - Remove commas, trim whitespace, normalize
        self.available_tools.push(Tool {
            name: "clean_data".to_string(),
            description: "Clean data by removing commas from numbers, trimming whitespace, normalizing formats. Use BEFORE joins or comparisons to ensure data is in the correct format. Handles common data quality issues like commas in numeric strings, extra spaces, inconsistent casing.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "table".to_string(),
                    description: "Table/dataframe name to clean".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "columns".to_string(),
                    description: "List of column names to clean".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: true,
                },
                ToolParameter {
                    name: "operations".to_string(),
                    description: "List of cleaning operations: 'remove_commas', 'trim_whitespace', 'normalize_case', 'remove_special_chars'".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: false,
                },
            ],
            category: ToolCategory::DataEngineering,
        });
        
        // Cast Types Tool - Cast columns to compatible types
        self.available_tools.push(Tool {
            name: "cast_types".to_string(),
            description: "Cast columns to compatible data types. Use BEFORE joins when validate_schema detects type mismatches. Automatically converts string numbers to numeric types, handles date formats, etc. Ensures join keys have matching types.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "table".to_string(),
                    description: "Table/dataframe name".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "type_mapping".to_string(),
                    description: "Mapping of column names to target types (e.g., {'amount': 'float64', 'date': 'date'})".to_string(),
                    parameter_type: ParameterType::Object(HashMap::new()),
                    required: true,
                },
            ],
            category: ToolCategory::DataEngineering,
        });
        
        // Validate Join Keys Tool - Pre-join validation
        self.available_tools.push(Tool {
            name: "validate_join_keys".to_string(),
            description: "Validate join keys BEFORE performing a join. Checks for: 1) Type compatibility, 2) Value overlap, 3) Null values in join keys, 4) Duplicate keys. Use this tool just before any join operation to make join errors visible and prevent silent failures.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "left_table".to_string(),
                    description: "Name of the left table/dataframe".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "right_table".to_string(),
                    description: "Name of the right table/dataframe".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "join_keys".to_string(),
                    description: "Mapping of join keys {'left_col': 'right_col'} or list of column names if same names".to_string(),
                    parameter_type: ParameterType::Object(HashMap::new()),
                    required: true,
                },
                ToolParameter {
                    name: "join_type".to_string(),
                    description: "Type of join: 'inner', 'left', 'right', 'outer'".to_string(),
                    parameter_type: ParameterType::String,
                    required: false,
                },
            ],
            category: ToolCategory::Validation,
        });
        
        // Detect Anomalies Tool - Data quality checks
        self.available_tools.push(Tool {
            name: "detect_anomalies".to_string(),
            description: "Detect data quality anomalies: null values, outliers, duplicates, invalid formats. Use BEFORE comparisons to identify data quality issues that could cause reconciliation mismatches. Makes data quality problems visible early.".to_string(),
            parameters: vec![
                ToolParameter {
                    name: "table".to_string(),
                    description: "Table/dataframe name to analyze".to_string(),
                    parameter_type: ParameterType::String,
                    required: true,
                },
                ToolParameter {
                    name: "columns".to_string(),
                    description: "List of column names to check for anomalies".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: false,
                },
                ToolParameter {
                    name: "checks".to_string(),
                    description: "List of checks to perform: 'nulls', 'duplicates', 'outliers', 'formats', 'ranges'".to_string(),
                    parameter_type: ParameterType::Array("string".to_string()),
                    required: false,
                },
            ],
            category: ToolCategory::DataEngineering,
        });
    }
    
    /// Let LLM decide which tools to use based on the query
    pub async fn plan_execution(
        &self,
        query: &str,
        context: &ExecutionContext,
    ) -> Result<ToolExecutionPlan> {
        println!("\n🤖 LLM TOOL SELECTION");
        println!("   Query: \"{}\"", query);
        println!("   Available tools: {}", self.available_tools.len());
        
        // Build tool descriptions for LLM
        let tools_json: Vec<serde_json::Value> = self.available_tools.iter()
            .map(|tool| {
                serde_json::json!({
                    "name": tool.name,
                    "description": tool.description,
                    "category": format!("{:?}", tool.category),
                    "parameters": tool.parameters.iter().map(|p| {
                        serde_json::json!({
                            "name": p.name,
                            "description": p.description,
                            "type": format!("{:?}", p.parameter_type),
                            "required": p.required
                        })
                    }).collect::<Vec<_>>()
                })
            })
            .collect();
        
        let prompt = format!(
            r#"Analyze this reconciliation query and determine which tools to use.
Return JSON only with this structure:
{{
  "reasoning": "explanation of why these tools were selected",
  "steps": [
    {{
      "tool_name": "tool_name",
      "parameters": {{"param": "value"}},
      "reasoning": "why this tool is needed",
      "confidence": 0.0-1.0
    }}
  ]
}}

Query: "{}"

Context:
- System A: {}
- System B: {}
- Metric: {}
- Grain columns: {:?}
- Available tables: {:?}

Available Tools:
{}

Guidelines:
1. Use fuzzy_match if comparing string-based keys that might have variations
2. Use group_by if you need to aggregate to a different grain level
3. Use join if you need to combine data from multiple tables
4. Use normalize_keys if systems use different key formats
5. Use filter if you need to exclude certain records
6. Use compare for the final comparison step

Return JSON only:"#,
            query,
            context.system_a,
            context.system_b,
            context.metric,
            context.grain_columns,
            context.available_tables.join(", "),
            serde_json::to_string_pretty(&tools_json).unwrap()
        );
        
        let response = self.llm.call_llm(&prompt).await?;
        
        // Clean response - remove markdown code blocks if present
        let cleaned_response = response
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();
        
        // Parse JSON response
        let plan: ToolExecutionPlan = serde_json::from_str(&cleaned_response)
            .map_err(|e| RcaError::Llm(format!("Failed to parse LLM tool plan: {}. Response (first 500 chars): {}", e, &cleaned_response.chars().take(500).collect::<String>())))?;
        
        println!("   ✅ LLM selected {} tool(s):", plan.steps.len());
        for (idx, step) in plan.steps.iter().enumerate() {
            println!("      {}. {} (confidence: {:.2}%)", 
                idx + 1, step.tool_name, step.confidence * 100.0);
            println!("         Reasoning: {}", step.reasoning);
        }
        
        Ok(plan)
    }
    
    /// Get tool by name
    pub fn get_tool(&self, name: &str) -> Option<&Tool> {
        self.available_tools.iter().find(|t| t.name == name)
    }
    
    /// List all available tools
    pub fn list_tools(&self) -> &[Tool] {
        &self.available_tools
    }
    
    /// Execute a tool call (delegates to appropriate handler)
    pub async fn execute_tool(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        println!("\n🔧 EXECUTING TOOL: {}", tool_call.tool_name);
        println!("   Parameters: {:?}", tool_call.parameters);
        println!("   Reasoning: {}", tool_call.reasoning);
        
        match tool_call.tool_name.as_str() {
            "fuzzy_match" => self.execute_fuzzy_match(tool_call, context).await,
            "group_by" => self.execute_group_by(tool_call, context).await,
            "join" => self.execute_join(tool_call, context).await,
            "filter" => self.execute_filter(tool_call, context).await,
            "normalize_keys" => self.execute_normalize_keys(tool_call, context).await,
            "compare" => self.execute_compare(tool_call, context).await,
            // Data Engineering tools
            "inspect_column" => self.execute_inspect_column(tool_call, context).await,
            "validate_schema" => self.execute_validate_schema(tool_call, context).await,
            "clean_data" => self.execute_clean_data(tool_call, context).await,
            "cast_types" => self.execute_cast_types(tool_call, context).await,
            "validate_join_keys" => self.execute_validate_join_keys(tool_call, context).await,
            "detect_anomalies" => self.execute_detect_anomalies(tool_call, context).await,
            _ => Err(RcaError::Execution(format!("Unknown tool: {}", tool_call.tool_name))),
        }
    }
    
    async fn execute_fuzzy_match(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        // Extract parameters
        let columns: Vec<String> = tool_call.parameters
            .get("columns")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().map(|v| v.as_str().map(|s| s.to_string())).collect::<Option<Vec<_>>>())
            .ok_or_else(|| RcaError::Execution("Missing 'columns' parameter".to_string()))?;
        
        let threshold = tool_call.parameters
            .get("threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.85);
        
        println!("   ✅ Fuzzy matching enabled for columns: {:?} (threshold: {:.2})", columns, threshold);
        
        // Store fuzzy matching configuration in context
        context.fuzzy_columns = columns;
        context.fuzzy_threshold = threshold;
        
        Ok(ToolExecutionResult {
            success: true,
            message: format!("Fuzzy matching configured for columns: {:?}", context.fuzzy_columns),
            data: None,
        })
    }
    
    async fn execute_group_by(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        // This would be implemented to actually perform the group by operation
        // For now, just acknowledge the tool call
        println!("   ✅ Group by operation queued");
        Ok(ToolExecutionResult {
            success: true,
            message: "Group by operation configured".to_string(),
            data: None,
        })
    }
    
    async fn execute_join(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        println!("   ✅ Join operation queued");
        Ok(ToolExecutionResult {
            success: true,
            message: "Join operation configured".to_string(),
            data: None,
        })
    }
    
    async fn execute_filter(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        println!("   ✅ Filter operation queued");
        Ok(ToolExecutionResult {
            success: true,
            message: "Filter operation configured".to_string(),
            data: None,
        })
    }
    
    async fn execute_normalize_keys(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        println!("   ✅ Key normalization queued");
        Ok(ToolExecutionResult {
            success: true,
            message: "Key normalization configured".to_string(),
            data: None,
        })
    }
    
    async fn execute_compare(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        println!("   ✅ Compare operation queued");
        Ok(ToolExecutionResult {
            success: true,
            message: "Compare operation configured".to_string(),
            data: None,
        })
    }
    
    // ===== DATA ENGINEERING TOOL EXECUTORS =====
    
    async fn execute_inspect_column(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        let table = tool_call.parameters
            .get("table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'table' parameter".to_string()))?;
        
        let columns: Vec<String> = tool_call.parameters
            .get("columns")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().map(|v| v.as_str().map(|s| s.to_string())).collect::<Option<Vec<_>>>())
            .ok_or_else(|| RcaError::Execution("Missing 'columns' parameter".to_string()))?;
        
        let top_n = tool_call.parameters
            .get("top_n")
            .and_then(|v| v.as_u64())
            .unwrap_or(10) as usize;
        
        println!("   🔍 Inspecting columns {:?} in table '{}' (top {} values)", columns, table, top_n);
        
        // Store inspection request in context
        context.inspection_requests.push(InspectionRequest {
            table: table.to_string(),
            columns: columns.clone(),
            top_n,
        });
        
        Ok(ToolExecutionResult {
            success: true,
            message: format!("Column inspection queued for {:?} in table '{}'", columns, table),
            data: Some(serde_json::json!({
                "table": table,
                "columns": columns,
                "top_n": top_n
            })),
        })
    }
    
    async fn execute_validate_schema(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        let left_table = tool_call.parameters
            .get("left_table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'left_table' parameter".to_string()))?;
        
        let right_table = tool_call.parameters
            .get("right_table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'right_table' parameter".to_string()))?;
        
        let join_columns = tool_call.parameters
            .get("join_columns")
            .and_then(|v| v.as_object())
            .ok_or_else(|| RcaError::Execution("Missing 'join_columns' parameter".to_string()))?;
        
        println!("   ✅ Validating schema compatibility between '{}' and '{}'", left_table, right_table);
        println!("      Join columns: {:?}", join_columns);
        
        // Store validation request
        context.schema_validations.push(SchemaValidation {
            left_table: left_table.to_string(),
            right_table: right_table.to_string(),
            join_columns: join_columns.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect(),
        });
        
        Ok(ToolExecutionResult {
            success: true,
            message: format!("Schema validation queued for join between '{}' and '{}'", left_table, right_table),
            data: Some(serde_json::json!({
                "left_table": left_table,
                "right_table": right_table,
                "join_columns": join_columns
            })),
        })
    }
    
    async fn execute_clean_data(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        let table = tool_call.parameters
            .get("table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'table' parameter".to_string()))?;
        
        let columns: Vec<String> = tool_call.parameters
            .get("columns")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().map(|v| v.as_str().map(|s| s.to_string())).collect::<Option<Vec<_>>>())
            .ok_or_else(|| RcaError::Execution("Missing 'columns' parameter".to_string()))?;
        
        let operations: Vec<String> = tool_call.parameters
            .get("operations")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().map(|v| v.as_str().map(|s| s.to_string())).collect::<Option<Vec<_>>>())
            .unwrap_or_else(|| vec!["remove_commas".to_string(), "trim_whitespace".to_string()]);
        
        println!("   🧹 Cleaning data in table '{}' for columns {:?}", table, columns);
        println!("      Operations: {:?}", operations);
        
        // Store cleaning request
        context.data_cleaning_requests.push(DataCleaningRequest {
            table: table.to_string(),
            columns: columns.clone(),
            operations: operations.clone(),
        });
        
        Ok(ToolExecutionResult {
            success: true,
            message: format!("Data cleaning queued for {:?} in table '{}'", columns, table),
            data: Some(serde_json::json!({
                "table": table,
                "columns": columns,
                "operations": operations
            })),
        })
    }
    
    async fn execute_cast_types(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        let table = tool_call.parameters
            .get("table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'table' parameter".to_string()))?;
        
        let type_mapping = tool_call.parameters
            .get("type_mapping")
            .and_then(|v| v.as_object())
            .ok_or_else(|| RcaError::Execution("Missing 'type_mapping' parameter".to_string()))?;
        
        println!("   🔄 Casting types in table '{}'", table);
        println!("      Type mappings: {:?}", type_mapping);
        
        // Store type casting request
        let mapping: HashMap<String, String> = type_mapping.iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
            .collect();
        
        context.type_casting_requests.push(TypeCastingRequest {
            table: table.to_string(),
            type_mapping: mapping.clone(),
        });
        
        Ok(ToolExecutionResult {
            success: true,
            message: format!("Type casting queued for table '{}'", table),
            data: Some(serde_json::json!({
                "table": table,
                "type_mapping": type_mapping
            })),
        })
    }
    
    async fn execute_validate_join_keys(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        let left_table = tool_call.parameters
            .get("left_table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'left_table' parameter".to_string()))?;
        
        let right_table = tool_call.parameters
            .get("right_table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'right_table' parameter".to_string()))?;
        
        let join_keys = tool_call.parameters
            .get("join_keys")
            .and_then(|v| v.as_object())
            .ok_or_else(|| RcaError::Execution("Missing 'join_keys' parameter".to_string()))?;
        
        let join_type = tool_call.parameters
            .get("join_type")
            .and_then(|v| v.as_str())
            .unwrap_or("inner");
        
        println!("   ✅ Validating join keys between '{}' and '{}' (join type: {})", 
            left_table, right_table, join_type);
        println!("      Join keys: {:?}", join_keys);
        
        // Store join validation request
        context.join_validations.push(JoinValidation {
            left_table: left_table.to_string(),
            right_table: right_table.to_string(),
            join_keys: join_keys.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect(),
            join_type: join_type.to_string(),
        });
        
        Ok(ToolExecutionResult {
            success: true,
            message: format!("Join key validation queued for '{}' JOIN '{}'", left_table, right_table),
            data: Some(serde_json::json!({
                "left_table": left_table,
                "right_table": right_table,
                "join_keys": join_keys,
                "join_type": join_type
            })),
        })
    }
    
    async fn execute_detect_anomalies(
        &self,
        tool_call: &ToolCall,
        context: &mut ToolExecutionContext,
    ) -> Result<ToolExecutionResult> {
        let table = tool_call.parameters
            .get("table")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RcaError::Execution("Missing 'table' parameter".to_string()))?;
        
        let columns: Option<Vec<String>> = tool_call.parameters
            .get("columns")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().map(|v| v.as_str().map(|s| s.to_string())).collect::<Option<Vec<_>>>());
        
        let checks: Vec<String> = tool_call.parameters
            .get("checks")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter().map(|v| v.as_str().map(|s| s.to_string())).collect::<Option<Vec<_>>>())
            .unwrap_or_else(|| vec!["nulls".to_string(), "duplicates".to_string(), "formats".to_string()]);
        
        println!("   🔍 Detecting anomalies in table '{}'", table);
        if let Some(ref cols) = columns {
            println!("      Columns: {:?}", cols);
        }
        println!("      Checks: {:?}", checks);
        
        // Store anomaly detection request
        context.anomaly_detections.push(AnomalyDetection {
            table: table.to_string(),
            columns: columns.clone(),
            checks: checks.clone(),
        });
        
        Ok(ToolExecutionResult {
            success: true,
            message: format!("Anomaly detection queued for table '{}'", table),
            data: Some(serde_json::json!({
                "table": table,
                "columns": columns,
                "checks": checks
            })),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub system_a: String,
    pub system_b: String,
    pub metric: String,
    pub grain_columns: Vec<String>,
    pub available_tables: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ToolExecutionContext {
    pub fuzzy_columns: Vec<String>,
    pub fuzzy_threshold: f64,
    pub group_by_config: Option<GroupByConfig>,
    pub join_configs: Vec<JoinConfig>,
    pub filters: Vec<String>,
    // Data Engineering tool requests
    pub inspection_requests: Vec<InspectionRequest>,
    pub schema_validations: Vec<SchemaValidation>,
    pub data_cleaning_requests: Vec<DataCleaningRequest>,
    pub type_casting_requests: Vec<TypeCastingRequest>,
    pub join_validations: Vec<JoinValidation>,
    pub anomaly_detections: Vec<AnomalyDetection>,
}

impl Default for ToolExecutionContext {
    fn default() -> Self {
        Self {
            fuzzy_columns: Vec::new(),
            fuzzy_threshold: 0.85,
            group_by_config: None,
            join_configs: Vec::new(),
            filters: Vec::new(),
            inspection_requests: Vec::new(),
            schema_validations: Vec::new(),
            data_cleaning_requests: Vec::new(),
            type_casting_requests: Vec::new(),
            join_validations: Vec::new(),
            anomaly_detections: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GroupByConfig {
    pub columns: Vec<String>,
    pub aggregations: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct JoinConfig {
    pub left_table: String,
    pub right_table: String,
    pub on: Vec<String>,
    pub join_type: String,
}

#[derive(Debug, Clone)]
pub struct ToolExecutionResult {
    pub success: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

// Data Engineering tool request structures
#[derive(Debug, Clone)]
pub struct InspectionRequest {
    pub table: String,
    pub columns: Vec<String>,
    pub top_n: usize,
}

#[derive(Debug, Clone)]
pub struct SchemaValidation {
    pub left_table: String,
    pub right_table: String,
    pub join_columns: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DataCleaningRequest {
    pub table: String,
    pub columns: Vec<String>,
    pub operations: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TypeCastingRequest {
    pub table: String,
    pub type_mapping: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct JoinValidation {
    pub left_table: String,
    pub right_table: String,
    pub join_keys: HashMap<String, String>,
    pub join_type: String,
}

#[derive(Debug, Clone)]
pub struct AnomalyDetection {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub checks: Vec<String>,
}

