//! Join Type Inference Engine
//! 
//! Intelligently determines the best join type by reasoning like a human analyst:
//! 1. Explicit join type from query (highest priority)
//! 2. Query language hints and semantic analysis
//! 3. Business context analysis (task type, purpose, completeness needs)
//! 4. Relationship cardinality reasoning (one-to-many, many-to-one, etc.)
//! 5. Data completeness requirements
//! 6. Human analyst reasoning patterns

use crate::error::Result;
use crate::metadata::Metadata;
use crate::intent_compiler::TaskType;
use serde::{Deserialize, Serialize};

/// Join type inference result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinTypeInference {
    /// Inferred join type: "INNER", "LEFT", "RIGHT", "FULL"
    pub join_type: String,
    
    /// Confidence level (0.0 - 1.0)
    pub confidence: f64,
    
    /// Source of inference: "explicit", "query_language", "lineage", "cardinality", "default"
    pub source: String,
    
    /// Reasoning for the inference
    pub reasoning: String,
    
    /// Alternative join types considered
    pub alternatives: Vec<AlternativeJoinType>,
}

/// Alternative join type that was considered
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlternativeJoinType {
    pub join_type: String,
    pub confidence: f64,
    pub reasoning: String,
}

/// Join Type Inference Engine
pub struct JoinTypeInferenceEngine {
    metadata: Metadata,
}

impl JoinTypeInferenceEngine {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
    
    /// Infer join type using intelligent reasoning like a human analyst
    /// 
    /// Priority order:
    /// 1. Explicit join type from query (highest priority)
    /// 2. Query language hints and semantic analysis
    /// 3. Business context reasoning (task type, purpose, completeness)
    /// 4. Relationship cardinality analysis
    /// 5. Human analyst reasoning patterns
    /// 6. Default (reasoned default based on context)
    pub fn infer_join_type(
        &self,
        left_table: &str,
        right_table: &str,
        explicit_type: Option<&str>,
        query_hints: Option<&QueryLanguageHints>,
        task_type: Option<&TaskType>,
        query_context: Option<&str>,
    ) -> Result<JoinTypeInference> {
        // Strategy 1: Explicit join type (highest priority)
        if let Some(explicit) = explicit_type {
            return Ok(JoinTypeInference {
                join_type: normalize_join_type(explicit),
                confidence: 1.0,
                source: "explicit".to_string(),
                reasoning: format!("Join type explicitly specified: {}", explicit),
                alternatives: vec![],
            });
        }
        
        // Strategy 2: Query language hints
        if let Some(hints) = query_hints {
            if let Some(inference) = self.infer_from_query_language(hints)? {
                return Ok(inference);
            }
        }
        
        // Strategy 3: Business context reasoning (NEW - thinks like human analyst)
        if let Some(inference) = self.infer_from_business_context(
            left_table,
            right_table,
            task_type,
            query_context,
        )? {
            return Ok(inference);
        }
        
        // Strategy 4: Relationship cardinality analysis (enhanced)
        if let Some(inference) = self.infer_from_cardinality_reasoning(left_table, right_table)? {
            return Ok(inference);
        }
        
        // Strategy 5: Human analyst reasoning patterns
        if let Some(inference) = self.infer_from_analyst_patterns(left_table, right_table, task_type)? {
            return Ok(inference);
        }
        
        // Strategy 6: Context-aware default
        Ok(self.get_contextual_default(left_table, right_table, task_type))
    }
    
    /// Infer join type from query language hints
    fn infer_from_query_language(&self, hints: &QueryLanguageHints) -> Result<Option<JoinTypeInference>> {
        let mut alternatives = Vec::new();
        
        // Check for explicit language patterns
        if hints.include_all_left {
            return Ok(Some(JoinTypeInference {
                join_type: "LEFT".to_string(),
                confidence: 0.9,
                source: "query_language".to_string(),
                reasoning: "Query language indicates 'include all' from left table".to_string(),
                alternatives: vec![],
            }));
        }
        
        if hints.include_all_right {
            return Ok(Some(JoinTypeInference {
                join_type: "RIGHT".to_string(),
                confidence: 0.9,
                source: "query_language".to_string(),
                reasoning: "Query language indicates 'include all' from right table".to_string(),
                alternatives: vec![],
            }));
        }
        
        if hints.include_all_both {
            return Ok(Some(JoinTypeInference {
                join_type: "FULL".to_string(),
                confidence: 0.9,
                source: "query_language".to_string(),
                reasoning: "Query language indicates 'include all records from both tables'".to_string(),
                alternatives: vec![],
            }));
        }
        
        if hints.only_matching {
            return Ok(Some(JoinTypeInference {
                join_type: "INNER".to_string(),
                confidence: 0.9,
                source: "query_language".to_string(),
                reasoning: "Query language indicates 'only matching records'".to_string(),
                alternatives: vec![],
            }));
        }
        
        // Check for semantic patterns
        if hints.has_exists_check {
            alternatives.push(AlternativeJoinType {
                join_type: "INNER".to_string(),
                confidence: 0.7,
                reasoning: "EXISTS check suggests INNER join".to_string(),
            });
        }
        
        if hints.has_not_exists_check {
            alternatives.push(AlternativeJoinType {
                join_type: "LEFT".to_string(),
                confidence: 0.7,
                reasoning: "NOT EXISTS check suggests LEFT join with filter".to_string(),
            });
        }
        
        // If we have some hints but not definitive, return with lower confidence
        if !alternatives.is_empty() {
            // Find best alternative
            let best_idx = alternatives.iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.confidence.partial_cmp(&b.confidence).unwrap())
                .map(|(idx, _)| idx)
                .unwrap();
            
            // Clone best before moving alternatives
            let best = alternatives[best_idx].clone();
            let best_type = best.join_type.clone();
            
            // Filter alternatives (move happens here)
            let filtered_alternatives: Vec<_> = alternatives.into_iter()
                .enumerate()
                .filter(|(idx, a)| *idx != best_idx && a.join_type != best_type)
                .map(|(_, a)| a)
                .collect();
            
            return Ok(Some(JoinTypeInference {
                join_type: best_type,
                confidence: best.confidence,
                source: "query_language".to_string(),
                reasoning: best.reasoning,
                alternatives: filtered_alternatives,
            }));
        }
        
        Ok(None)
    }
    
    /// Infer join type from business context - thinks like a human analyst
    fn infer_from_business_context(
        &self,
        left_table: &str,
        right_table: &str,
        task_type: Option<&TaskType>,
        query_context: Option<&str>,
    ) -> Result<Option<JoinTypeInference>> {
        // Get table metadata to understand entities
        let _left_entity = self.get_table_entity(left_table);
        let _right_entity = self.get_table_entity(right_table);
        
        // Analyze based on task type
        if let Some(task) = task_type {
            match task {
                TaskType::RCA => {
                    // For RCA: Need complete data, prefer LEFT to see all records
                    // But consider: if comparing systems, might need INNER for apples-to-apples
                    let reasoning = if query_context.map(|c| c.contains("compare") || c.contains("mismatch")).unwrap_or(false) {
                        // Comparing systems: INNER join for fair comparison
                        ("INNER".to_string(), 0.85, format!(
                            "RCA comparison task: Use INNER join to compare only matching records between {} and {} for fair analysis",
                            left_table, right_table
                        ))
                    } else {
                        // General RCA: LEFT join to see all records and identify missing data
                        ("LEFT".to_string(), 0.9, format!(
                            "RCA analysis: Use LEFT join to preserve all records from {} and identify missing data in {}",
                            left_table, right_table
                        ))
                    };
                    
                    let join_type = reasoning.0.clone();
                    let confidence = reasoning.1;
                    let reasoning_text = reasoning.2.clone();
                    
                    return Ok(Some(JoinTypeInference {
                        join_type: join_type.clone(),
                        confidence,
                        source: "business_context".to_string(),
                        reasoning: reasoning_text,
                        alternatives: vec![
                            AlternativeJoinType {
                                join_type: if join_type == "LEFT" { "INNER".to_string() } else { "LEFT".to_string() },
                                confidence: confidence - 0.15,
                                reasoning: format!("Alternative for {}: {} join if completeness requirements differ", 
                                                 task_type_str(task),
                                                 if join_type == "LEFT" { "INNER" } else { "LEFT" }),
                            },
                        ],
                    }));
                }
                TaskType::QUERY => {
                    // For queries: Depends on what user wants
                    // If asking "what is X", usually want all X even without Y
                    let reasoning = if query_context.map(|c| c.contains("all") || c.contains("every")).unwrap_or(false) {
                        ("LEFT".to_string(), 0.85, format!(
                            "Query asks for 'all' records: Use LEFT join to include all {} even without matching {}",
                            left_table, right_table
                        ))
                    } else if query_context.map(|c| c.contains("only") || c.contains("matching")).unwrap_or(false) {
                        ("INNER".to_string(), 0.85, format!(
                            "Query asks for 'only matching': Use INNER join to get only {} that have {}",
                            left_table, right_table
                        ))
                    } else {
                        // Default for queries: LEFT (more informative)
                        ("LEFT".to_string(), 0.75, format!(
                            "Query context: Default to LEFT join to show all {} with {} where available",
                            left_table, right_table
                        ))
                    };
                    
                    return Ok(Some(JoinTypeInference {
                        join_type: reasoning.0,
                        confidence: reasoning.1,
                        source: "business_context".to_string(),
                        reasoning: reasoning.2,
                        alternatives: vec![],
                    }));
                }
                TaskType::DV => {
                    // For data validation: Usually need INNER to validate relationships exist
                    return Ok(Some(JoinTypeInference {
                        join_type: "INNER".to_string(),
                        confidence: 0.8,
                        source: "business_context".to_string(),
                        reasoning: format!(
                            "Data validation: Use INNER join to validate that relationships exist between {} and {}",
                            left_table, right_table
                        ),
                        alternatives: vec![],
                    }));
                }
            }
        }
        
        Ok(None)
    }
    
    /// Infer join type from cardinality reasoning - enhanced logic
    fn infer_from_cardinality_reasoning(&self, left_table: &str, right_table: &str) -> Result<Option<JoinTypeInference>> {
        // Find matching edge
        for edge in &self.metadata.lineage.edges {
            let matches_forward = edge.from == left_table && edge.to == right_table;
            let matches_reverse = edge.from == right_table && edge.to == left_table;
            
            if matches_forward || matches_reverse {
                let (actual_left, actual_right, relationship) = if matches_forward {
                    (left_table, right_table, &edge.relationship)
                } else {
                    (right_table, left_table, &edge.relationship)
                };
                
                // Check if relationship is directly a join type (e.g., "left_join")
                if relationship == "left_join" || relationship == "left" {
                    return Ok(Some(JoinTypeInference {
                        join_type: "LEFT".to_string(),
                        confidence: 0.95,
                        source: "lineage_join_type".to_string(),
                        reasoning: format!("Lineage metadata specifies LEFT join between {} and {}", actual_left, actual_right),
                        alternatives: vec![],
                    }));
                }
                
                if relationship == "inner_join" || relationship == "inner" {
                    return Ok(Some(JoinTypeInference {
                        join_type: "INNER".to_string(),
                        confidence: 0.95,
                        source: "lineage_join_type".to_string(),
                        reasoning: format!("Lineage metadata specifies INNER join between {} and {}", actual_left, actual_right),
                        alternatives: vec![],
                    }));
                }
                
                // Reason about cardinality
                let (join_type, confidence, reasoning) = match relationship.as_str() {
                    "one_to_one" => {
                        // One-to-one: Usually LEFT to preserve all from primary entity
                        // But consider: if both sides are equally important, could use INNER
                        let left_entity = self.get_table_entity(actual_left);
                        let right_entity = self.get_table_entity(actual_right);
                        
                        // If left is a core entity (like customer, loan), preserve it
                        if self.is_core_entity(&left_entity) {
                            ("LEFT".to_string(), 0.9, format!(
                                "One-to-one: {} is a core entity, preserve all records with LEFT join",
                                left_entity
                            ))
                        } else {
                            ("INNER".to_string(), 0.8, format!(
                                "One-to-one: Both {} and {} are equally important, use INNER join",
                                left_entity, right_entity
                            ))
                        }
                    }
                    "one_to_many" => {
                        // One-to-many: LEFT join to preserve all parent records
                        // This is the most common pattern: parent -> children
                        ("LEFT".to_string(), 0.92, format!(
                            "One-to-many: {} has many {}, preserve all {} records with LEFT join (standard parent-child pattern)",
                            actual_left, actual_right, actual_left
                        ))
                    }
                    "many_to_one" => {
                        // Many-to-one: This is tricky - depends on what we want
                        // If we want all child records: LEFT (children -> parent)
                        // If we want only children with parents: INNER
                        // Default: LEFT to see all children
                        ("LEFT".to_string(), 0.85, format!(
                            "Many-to-one: Many {} map to one {}, use LEFT join to see all {} records",
                            actual_left, actual_right, actual_left
                        ))
                    }
                    "many_to_many" => {
                        // Many-to-many: Usually need INNER to get matching pairs
                        // But could use LEFT if one side is primary
                        ("INNER".to_string(), 0.88, format!(
                            "Many-to-many: Use INNER join to get matching pairs between {} and {}",
                            actual_left, actual_right
                        ))
                    }
                    _ => {
                        // Unknown relationship: Reason from entity types
                        let left_entity = self.get_table_entity(actual_left);
                        let right_entity = self.get_table_entity(actual_right);
                        
                        if self.is_core_entity(&left_entity) {
                            ("LEFT".to_string(), 0.7, format!(
                                "Unknown relationship: {} is a core entity, default to LEFT join to preserve all records",
                                left_entity
                            ))
                        } else {
                            ("INNER".to_string(), 0.65, format!(
                                "Unknown relationship: Default to INNER join for {} and {}",
                                left_entity, right_entity
                            ))
                        }
                    }
                };
                
                let join_type_clone = join_type.clone();
                return Ok(Some(JoinTypeInference {
                    join_type,
                    confidence,
                    source: "cardinality_reasoning".to_string(),
                    reasoning,
                    alternatives: self.generate_alternatives(&join_type_clone, &relationship, actual_left, actual_right),
                }));
            }
        }
        
        Ok(None)
    }
    
    /// Infer from human analyst reasoning patterns
    fn infer_from_analyst_patterns(
        &self,
        left_table: &str,
        right_table: &str,
        task_type: Option<&TaskType>,
    ) -> Result<Option<JoinTypeInference>> {
        let left_entity = self.get_table_entity(left_table);
        let right_entity = self.get_table_entity(right_table);
        
        // Pattern 1: Core entity joins (customer, loan, account) -> usually LEFT
        if self.is_core_entity(&left_entity) {
            return Ok(Some(JoinTypeInference {
                join_type: "LEFT".to_string(),
                confidence: 0.8,
                source: "analyst_pattern".to_string(),
                reasoning: format!(
                    "Analyst pattern: {} is a core entity, analysts typically use LEFT join to preserve all {} records",
                    left_entity, left_entity
                ),
                alternatives: vec![],
            }));
        }
        
        // Pattern 2: Lookup/reference tables -> usually LEFT (preserve main records)
        if self.is_lookup_table(&right_entity) {
            return Ok(Some(JoinTypeInference {
                join_type: "LEFT".to_string(),
                confidence: 0.85,
                source: "analyst_pattern".to_string(),
                reasoning: format!(
                    "Analyst pattern: {} is a lookup table, use LEFT join to preserve all {} records",
                    right_entity, left_entity
                ),
                alternatives: vec![],
            }));
        }
        
        // Pattern 3: Transaction/event tables -> depends on task
        if self.is_transaction_table(&right_entity) {
            let join_type = match task_type {
                Some(TaskType::RCA) => "LEFT", // RCA: want to see all transactions
                Some(TaskType::QUERY) => "LEFT", // Query: usually want all
                _ => "INNER", // Default: only matching
            };
            
            return Ok(Some(JoinTypeInference {
                join_type: join_type.to_string(),
                confidence: 0.75,
                source: "analyst_pattern".to_string(),
                reasoning: format!(
                    "Analyst pattern: {} is a transaction table, use {} join based on task type",
                    right_entity, join_type
                ),
                alternatives: vec![],
            }));
        }
        
        Ok(None)
    }
    
    /// Get context-aware default join type
    fn get_contextual_default(
        &self,
        left_table: &str,
        right_table: &str,
        task_type: Option<&TaskType>,
    ) -> JoinTypeInference {
        let left_entity = self.get_table_entity(left_table);
        
        // Default reasoning based on context
        let (join_type, confidence, reasoning) = match task_type {
            Some(TaskType::RCA) => {
                ("LEFT".to_string(), 0.6, format!(
                    "RCA default: Use LEFT join to preserve all records from {} for comprehensive root cause analysis",
                    left_table
                ))
            }
            Some(TaskType::QUERY) => {
                ("LEFT".to_string(), 0.65, format!(
                    "Query default: Use LEFT join to show all {} with {} where available (more informative)",
                    left_table, right_table
                ))
            }
            Some(TaskType::DV) => {
                ("INNER".to_string(), 0.6, format!(
                    "Validation default: Use INNER join to validate relationships exist",
                ))
            }
            None => {
                if self.is_core_entity(&left_entity) {
                    ("LEFT".to_string(), 0.55, format!(
                        "Default: {} is a core entity, use LEFT join to preserve all records",
                        left_entity
                    ))
                } else {
                    ("INNER".to_string(), 0.5, format!(
                        "Default: Use INNER join for {} and {} (conservative choice)",
                        left_table, right_table
                    ))
                }
            }
        };
        
        let join_type_clone = join_type.clone();
        JoinTypeInference {
            join_type,
            confidence,
            source: "contextual_default".to_string(),
            reasoning,
            alternatives: vec![
                AlternativeJoinType {
                    join_type: if join_type_clone == "LEFT" { "INNER".to_string() } else { "LEFT".to_string() },
                    confidence: confidence - 0.2,
                    reasoning: format!("Alternative: {} join if different completeness requirements", 
                                     if join_type_clone == "LEFT" { "INNER" } else { "LEFT" }),
                },
            ],
        }
    }
    
    // Helper methods
    
    fn get_table_entity(&self, table_name: &str) -> String {
        self.metadata.tables.iter()
            .find(|t| t.name == table_name)
            .map(|t| t.entity.clone())
            .unwrap_or_else(|| table_name.to_string())
    }
    
    fn is_core_entity(&self, entity: &str) -> bool {
        let entity_lower = entity.to_lowercase();
        entity_lower.contains("customer") ||
        entity_lower.contains("loan") ||
        entity_lower.contains("account") ||
        entity_lower.contains("product") ||
        entity_lower.contains("order")
    }
    
    fn is_lookup_table(&self, entity: &str) -> bool {
        let entity_lower = entity.to_lowercase();
        entity_lower.contains("lookup") ||
        entity_lower.contains("reference") ||
        entity_lower.contains("code") ||
        entity_lower.contains("type") ||
        entity_lower.contains("category") ||
        entity_lower.contains("picklist")
    }
    
    fn is_transaction_table(&self, entity: &str) -> bool {
        let entity_lower = entity.to_lowercase();
        entity_lower.contains("transaction") ||
        entity_lower.contains("event") ||
        entity_lower.contains("payment") ||
        entity_lower.contains("activity") ||
        entity_lower.contains("history")
    }
    
    fn generate_alternatives(
        &self,
        primary_type: &str,
        relationship: &str,
        left_table: &str,
        right_table: &str,
    ) -> Vec<AlternativeJoinType> {
        let mut alternatives = Vec::new();
        
        match relationship {
            "one_to_many" => {
                // Alternative: INNER if we only want parents with children
                if primary_type == "LEFT" {
                    alternatives.push(AlternativeJoinType {
                        join_type: "INNER".to_string(),
                        confidence: 0.7,
                        reasoning: format!(
                            "Alternative: Use INNER join if you only want {} that have {}",
                            left_table, right_table
                        ),
                    });
                }
            }
            "many_to_one" => {
                // Alternative: INNER if we only want children with parents
                if primary_type == "LEFT" {
                    alternatives.push(AlternativeJoinType {
                        join_type: "INNER".to_string(),
                        confidence: 0.75,
                        reasoning: format!(
                            "Alternative: Use INNER join if you only want {} that have matching {}",
                            left_table, right_table
                        ),
                    });
                }
            }
            _ => {}
        }
        
        alternatives
    }
}

/// Query language hints extracted from natural language
#[derive(Debug, Clone, Default)]
pub struct QueryLanguageHints {
    /// "include all records from X" - suggests LEFT join
    pub include_all_left: bool,
    
    /// "include all records from Y" - suggests RIGHT join
    pub include_all_right: bool,
    
    /// "all records from both" - suggests FULL join
    pub include_all_both: bool,
    
    /// "only matching", "where exists" - suggests INNER join
    pub only_matching: bool,
    
    /// Query contains EXISTS check
    pub has_exists_check: bool,
    
    /// Query contains NOT EXISTS check
    pub has_not_exists_check: bool,
    
    /// Explicit join type mentioned in query
    pub explicit_join_type: Option<String>,
}

impl QueryLanguageHints {
    /// Extract hints from query text
    pub fn from_query(query: &str) -> Self {
        let query_lower = query.to_lowercase();
        
        let include_all_left = query_lower.contains("include all") && 
                              (query_lower.contains("from") || query_lower.contains("left")) ||
                              query_lower.contains("all records from") ||
                              query_lower.contains("preserve all");
        
        let include_all_right = query_lower.contains("include all") && 
                               query_lower.contains("right") ||
                               query_lower.contains("all records from right");
        
        let include_all_both = query_lower.contains("all records from both") ||
                               query_lower.contains("all from both tables") ||
                               query_lower.contains("full outer");
        
        let only_matching = query_lower.contains("only matching") ||
                           query_lower.contains("matching records only") ||
                           query_lower.contains("where exists") ||
                           query_lower.contains("that have");
        
        let has_exists_check = query_lower.contains("exists") && 
                               !query_lower.contains("not exists");
        
        let has_not_exists_check = query_lower.contains("not exists") ||
                                  query_lower.contains("excluding") ||
                                  query_lower.contains("without");
        
        // Extract explicit join type
        let explicit_join_type = if query_lower.contains("inner join") {
            Some("INNER".to_string())
        } else if query_lower.contains("left join") || query_lower.contains("left outer join") {
            Some("LEFT".to_string())
        } else if query_lower.contains("right join") || query_lower.contains("right outer join") {
            Some("RIGHT".to_string())
        } else if query_lower.contains("full join") || query_lower.contains("full outer join") {
            Some("FULL".to_string())
        } else {
            None
        };
        
        Self {
            include_all_left,
            include_all_right,
            include_all_both,
            only_matching,
            has_exists_check,
            has_not_exists_check,
            explicit_join_type,
        }
    }
}

/// Normalize join type string to standard format
fn normalize_join_type(join_type: &str) -> String {
    match join_type.to_uppercase().trim() {
        "INNER" | "INNER JOIN" => "INNER".to_string(),
        "LEFT" | "LEFT JOIN" | "LEFT OUTER" | "LEFT OUTER JOIN" => "LEFT".to_string(),
        "RIGHT" | "RIGHT JOIN" | "RIGHT OUTER" | "RIGHT OUTER JOIN" => "RIGHT".to_string(),
        "FULL" | "FULL JOIN" | "FULL OUTER" | "FULL OUTER JOIN" | "OUTER" => "FULL".to_string(),
        _ => join_type.to_uppercase(),
    }
}

/// Helper to convert TaskType to string for reasoning
fn task_type_str(task_type: &TaskType) -> &str {
    match task_type {
        TaskType::RCA => "RCA",
        TaskType::DV => "DV",
        TaskType::QUERY => "QUERY",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query_hints_extraction() {
        let hints = QueryLanguageHints::from_query("include all records from customers");
        assert!(hints.include_all_left);
        
        let hints = QueryLanguageHints::from_query("only matching records");
        assert!(hints.only_matching);
        
        let hints = QueryLanguageHints::from_query("customers LEFT JOIN orders");
        assert_eq!(hints.explicit_join_type, Some("LEFT".to_string()));
    }
    
    #[test]
    fn test_normalize_join_type() {
        assert_eq!(normalize_join_type("inner"), "INNER");
        assert_eq!(normalize_join_type("LEFT JOIN"), "LEFT");
        assert_eq!(normalize_join_type("full outer join"), "FULL");
    }
}

