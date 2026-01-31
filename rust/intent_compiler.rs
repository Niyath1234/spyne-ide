//! Intent Compiler - LLM as Compiler
//! 
//! Compiles natural language queries into strict JSON specifications.
//! The LLM acts as a compiler, not a thinker - outputs only validated JSON.
//! 
//! ## Fail-Fast Mechanism
//! 
//! Before attempting compilation, the system assesses information availability:
//! - If confident (>= threshold): Proceeds with compilation
//! - If not confident (< threshold): Fails fast and asks ONE clarifying question
//!   covering ALL missing pieces
//! 
//! This prevents wasted computation and provides better UX.

use crate::error::{RcaError, Result};
use crate::llm::LlmClient;
use crate::join_inference::{JoinTypeInferenceEngine, QueryLanguageHints};
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, debug};

/// Intent specification compiled from natural language
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentSpec {
    /// Task type: "RCA", "DV", or "QUERY"
    pub task_type: TaskType,
    
    /// Target metrics (for RCA)
    pub target_metrics: Vec<String>,
    
    /// Entities involved
    pub entities: Vec<String>,
    
    /// Constraints (filters, conditions)
    pub constraints: Vec<ConstraintSpec>,
    
    /// Required grain level
    pub grain: Vec<String>,
    
    /// Time scope (as_of_date, date_range, etc.)
    pub time_scope: Option<TimeScope>,
    
    /// Systems involved (for RCA)
    pub systems: Vec<String>,
    
    /// Validation constraint (for DV)
    pub validation_constraint: Option<ValidationConstraintSpec>,
    
    /// Join specifications - tables to join and how
    #[serde(default)]
    pub joins: Vec<JoinSpec>,
    
    /// Tables involved in the query (for complex multi-table queries)
    #[serde(default)]
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskType {
    RCA,
    DV,
    QUERY,  // Direct query: "What is TOS for khatabook as of date?"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintSpec {
    pub column: Option<String>,
    pub operator: Option<String>,
    pub value: Option<serde_json::Value>,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeScope {
    pub as_of_date: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub time_grain: Option<String>, // "daily", "monthly", etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConstraintSpec {
    pub constraint_type: String,
    pub description: String,
    pub details: serde_json::Value,
}

/// Join specification extracted from query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinSpec {
    /// Left table name (or entity/pattern)
    pub left_table: String,
    
    /// Right table name (or entity/pattern)
    pub right_table: String,
    
    /// Join type: "INNER", "LEFT", "RIGHT", "FULL", or null for inference
    /// Can be inferred from query language:
    /// - "include all", "all records" -> LEFT or FULL
    /// - "only matching", "where exists" -> INNER
    /// - "all from both" -> FULL
    /// - Default: inferred from lineage relationship
    pub join_type: Option<String>,
    
    /// Join conditions (column pairs)
    pub conditions: Vec<JoinCondition>,
    
    /// Confidence in join type inference (0.0-1.0)
    #[serde(default)]
    pub confidence: f64,
    
    /// Reasoning for join type choice
    #[serde(default)]
    pub reasoning: Option<String>,
}

/// Join condition - column pair for joining
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    /// Left column name (or pattern)
    pub left_column: String,
    
    /// Right column name (or pattern)
    pub right_column: String,
    
    /// Optional operator (default: "=")
    #[serde(default)]
    pub operator: Option<String>,
}

// ============================================================================
// FAIL-FAST CLARIFICATION SYSTEM
// ============================================================================

/// Result of intent compilation - supports fail-fast with clarification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntentCompilationResult {
    /// Successfully compiled intent with high confidence
    Success(IntentSpec),
    /// Needs clarification - contains a single question covering all doubts
    NeedsClarification(ClarificationRequest),
    /// Failed to compile even after clarification
    Failed(String),
}

/// Request for clarification when confidence is low
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClarificationRequest {
    /// Single consolidated question covering ALL missing information
    pub question: String,
    /// List of missing/ambiguous pieces
    pub missing_pieces: Vec<MissingPiece>,
    /// Confidence level (0.0 - 1.0) - why we need clarification
    pub confidence: f64,
    /// What we understood so far (partial extraction)
    pub partial_understanding: PartialIntent,
    /// Suggested response format/examples
    pub response_hints: Vec<String>,
}

/// A specific piece of missing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingPiece {
    /// Field name: "systems", "metrics", "grain", "constraints", etc.
    pub field: String,
    /// Human-readable description of what's missing
    pub description: String,
    /// How important is this piece
    pub importance: Importance,
    /// Possible values/examples if known
    pub suggestions: Vec<String>,
}

/// Importance level for missing information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Importance {
    /// Required - cannot proceed without this
    Required,
    /// Helpful - would improve accuracy but can proceed with defaults
    Helpful,
}

/// Partial intent - what we understood before failing fast
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartialIntent {
    /// Detected task type (if any)
    pub task_type: Option<TaskType>,
    /// Detected systems (even if incomplete)
    pub systems: Vec<String>,
    /// Detected metrics (even if incomplete)  
    pub metrics: Vec<String>,
    /// Detected entities
    pub entities: Vec<String>,
    /// Detected grain
    pub grain: Vec<String>,
    /// Detected constraints
    pub constraints: Vec<String>,
    /// Raw keywords extracted
    pub keywords: Vec<String>,
    /// Detected tables (for multi-table queries)
    #[serde(default)]
    pub tables: Vec<String>,
    /// Detected joins (even if incomplete)
    #[serde(default)]
    pub joins: Vec<String>,
}

/// Confidence assessment result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfidenceAssessment {
    /// Overall confidence (0.0 - 1.0)
    pub confidence: f64,
    /// Is information sufficient to proceed?
    pub is_sufficient: bool,
    /// Missing pieces identified
    pub missing_pieces: Vec<MissingPiece>,
    /// Partial understanding
    pub partial_intent: PartialIntent,
    /// Reasoning for the confidence score
    pub reasoning: String,
}

/// Intent Compiler - Uses LLM to compile natural language to strict JSON
/// 
/// Supports fail-fast mechanism: if confidence is below threshold,
/// returns a clarification request instead of attempting compilation.
pub struct IntentCompiler {
    llm: LlmClient,
    max_retries: usize,
    /// Confidence threshold (0.0 - 1.0). Below this, ask for clarification.
    confidence_threshold: f64,
    /// Whether to use fail-fast mechanism
    fail_fast_enabled: bool,
}

impl IntentCompiler {
    pub fn new(llm: LlmClient) -> Self {
        Self {
            llm,
            max_retries: 2,
            confidence_threshold: 0.7, // Default: 70% confidence required
            fail_fast_enabled: true,   // Enable by default
        }
    }

    /// Create with custom confidence threshold
    pub fn with_confidence_threshold(mut self, threshold: f64) -> Self {
        self.confidence_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Enable/disable fail-fast mechanism
    pub fn with_fail_fast(mut self, enabled: bool) -> Self {
        self.fail_fast_enabled = enabled;
        self
    }

    // ========================================================================
    // MAIN ENTRY POINT: compile_with_clarification
    // ========================================================================

    /// Compile with fail-fast clarification support
    /// 
    /// This is the RECOMMENDED entry point. It:
    /// 1. Assesses confidence FIRST
    /// 2. If confident enough → proceeds with compilation
    /// 3. If NOT confident → fails fast with ONE clarifying question
    /// 
    /// # Example
    /// ```ignore
    /// let result = compiler.compile_with_clarification(query).await?;
    /// match result {
    ///     IntentCompilationResult::Success(intent) => { /* proceed */ }
    ///     IntentCompilationResult::NeedsClarification(request) => {
    ///         // Show request.question to user
    ///         // Get their answer
    ///         // Call compile_with_answer(query, answer)
    ///     }
    ///     IntentCompilationResult::Failed(msg) => { /* handle error */ }
    /// }
    /// ```
    pub async fn compile_with_clarification(&self, query: &str) -> Result<IntentCompilationResult> {
        info!("🔍 Assessing query confidence: {}", query);
        
        // Step 1: Assess confidence (fail-fast check)
        if self.fail_fast_enabled {
            let assessment = self.assess_confidence(query).await?;
            
            info!("📊 Confidence assessment: {:.0}% (threshold: {:.0}%)", 
                  assessment.confidence * 100.0, 
                  self.confidence_threshold * 100.0);
            
            if !assessment.is_sufficient {
                // FAIL FAST: Generate clarification question
                info!("⚠️  Confidence below threshold. Generating clarification question...");
                let clarification = self.generate_clarification_question(&assessment, query).await?;
                return Ok(IntentCompilationResult::NeedsClarification(clarification));
            }
            
            info!("✅ Confidence sufficient. Proceeding with compilation...");
        }
        
        // Step 2: Proceed with compilation
        match self.compile(query).await {
            Ok(intent) => Ok(IntentCompilationResult::Success(intent)),
            Err(e) => Ok(IntentCompilationResult::Failed(e.to_string())),
        }
    }

    /// Compile with user's answer to clarification question
    /// 
    /// Call this after user provides answer to clarification question.
    /// Combines original query with answer for better compilation.
    pub async fn compile_with_answer(
        &self, 
        original_query: &str, 
        user_answer: &str
    ) -> Result<IntentCompilationResult> {
        info!("📝 Compiling with clarification answer");
        
        // Combine original query with answer
        let enhanced_query = format!(
            "Original query: {}\n\nAdditional context provided by user: {}",
            original_query, user_answer
        );
        
        // Try compilation with enhanced query (skip confidence check since user answered)
        match self.compile(&enhanced_query).await {
            Ok(intent) => Ok(IntentCompilationResult::Success(intent)),
            Err(e) => Ok(IntentCompilationResult::Failed(e.to_string())),
        }
    }

    // ========================================================================
    // CONFIDENCE ASSESSMENT
    // ========================================================================

    /// Assess confidence in understanding the query
    async fn assess_confidence(&self, query: &str) -> Result<ConfidenceAssessment> {
        let assessment_prompt = self.get_confidence_assessment_prompt();
        let user_prompt = format!("Query to assess: {}", query);
        
        let combined = format!("{}\n\n{}", assessment_prompt, user_prompt);
        let response = self.llm.call_llm(&combined).await?;
        
        // Parse LLM response
        self.parse_confidence_assessment(&response, query)
    }

    fn get_confidence_assessment_prompt(&self) -> String {
        format!(r#"You are an Intent Assessment Agent. Your job is to assess whether a user's query has ENOUGH INFORMATION to perform Root Cause Analysis (RCA), Data Validation (DV), or Direct Query (QUERY).

REQUIRED INFORMATION FOR QUERY (Direct Query):
1. SYSTEM (Required): Which system to query (e.g., "khatabook", "tb", "system_a")
2. METRIC (Required): What metric to retrieve (e.g., "TOS", "recovery", "balance")
3. GRAIN (Helpful): Level of aggregation (e.g., "customer_id", "loan_id") - can be inferred
4. TIME_SCOPE (Helpful): As-of date or date range - optional
5. CONSTRAINTS (Helpful): Filters like "active loans", "for customer X" - optional

REQUIRED INFORMATION FOR RCA:
1. SYSTEMS (Required): Determine how many systems are needed based on task type:
   - Reconciliation/comparison tasks: Multiple systems required (minimum varies by task)
   - Single system analysis: One system sufficient
   - Validation tasks: Can work with one or multiple systems
   - Examples: "system_a vs system_b", "khatabook vs tally", "analyze system_x"
2. METRICS (Required): What to compare/analyze (e.g., "TOS", "recovery", "balance", "outstanding")
3. GRAIN (Helpful): Level of comparison (e.g., "loan_id", "customer_id") - can be inferred
4. CONSTRAINTS (Helpful): Filters like "active loans", "for customer X" - optional

REQUIRED INFORMATION FOR DV:
1. CONSTRAINT TYPE (Required): What to validate (e.g., "uniqueness", "nullability", "range")
2. TARGET (Required): What entity/column to validate
3. CONDITION (Required): The validation rule

SYSTEM REQUIREMENT REASONING (Chain of Thought):
- Analyze query keywords to determine task type (reconciliation, validation, single-system analysis)
- Reconciliation tasks require comparing across multiple systems
- Single-system analysis requires only one system
- Validation can work with single or multiple systems
- The number of systems needed should be determined dynamically based on the task

SCORING RULES:
- If task type is clear AND required systems are detected → confidence >= 0.8
- If systems are detected but task type is unclear → confidence 0.5-0.7
- If task type is clear but systems are missing → confidence 0.5-0.7
- If BOTH task type AND systems are vague/missing → confidence < 0.5

OUTPUT FORMAT (JSON only, no markdown):
{{
  "confidence": 0.0-1.0,
  "is_sufficient": true/false,
  "missing_pieces": [
    {{
      "field": "systems|metrics|grain|constraints|validation_rule",
      "description": "What is missing/unclear",
      "importance": "Required|Helpful",
      "suggestions": ["possible value 1", "possible value 2"]
    }}
  ],
  "partial_intent": {{
    "task_type": "RCA|DV|QUERY|null",
    "systems": ["detected systems"],
    "metrics": ["detected metrics"],
    "entities": ["detected entities"],
    "grain": ["detected grain"],
    "constraints": ["detected constraints as strings"],
    "keywords": ["extracted keywords"],
    "tables": ["detected table names"],
    "joins": ["detected join relationships"]
  }},
  "reasoning": "Brief explanation of confidence score including chain of thought about task type and system requirements"
}}

IMPORTANT:
- Threshold for "is_sufficient" is {:.0}%
- Be conservative - if unsure, mark as insufficient
- Extract as much partial understanding as possible
- Provide helpful suggestions for missing pieces
"#, self.confidence_threshold * 100.0)
    }

    fn parse_confidence_assessment(&self, response: &str, _query: &str) -> Result<ConfidenceAssessment> {
        // Extract JSON from response
        let json_str = self.extract_json(response);
        
        // Try to parse
        match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(json) => {
                let confidence = json["confidence"].as_f64().unwrap_or(0.5);
                let is_sufficient = json["is_sufficient"].as_bool()
                    .unwrap_or(confidence >= self.confidence_threshold);
                
                // Parse missing pieces
                let missing_pieces = json["missing_pieces"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| {
                                Some(MissingPiece {
                                    field: v["field"].as_str()?.to_string(),
                                    description: v["description"].as_str()?.to_string(),
                                    importance: if v["importance"].as_str() == Some("Required") {
                                        Importance::Required
                                    } else {
                                        Importance::Helpful
                                    },
                                    suggestions: v["suggestions"]
                                        .as_array()
                                        .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                                        .unwrap_or_default(),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                
                // Parse partial intent
                let partial = &json["partial_intent"];
                let partial_intent = PartialIntent {
                    task_type: partial["task_type"].as_str().and_then(|s| {
                        match s.to_uppercase().as_str() {
                            "RCA" => Some(TaskType::RCA),
                            "DV" => Some(TaskType::DV),
                            "QUERY" => Some(TaskType::QUERY),
                            _ => None,
                        }
                    }),
                    systems: partial["systems"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    metrics: partial["metrics"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    entities: partial["entities"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    grain: partial["grain"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    constraints: partial["constraints"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    keywords: partial["keywords"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    tables: partial["tables"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                    joins: partial["joins"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                };
                
                let reasoning = json["reasoning"].as_str()
                    .unwrap_or("Assessment completed")
                    .to_string();
                
                Ok(ConfidenceAssessment {
                    confidence,
                    is_sufficient,
                    missing_pieces,
                    partial_intent,
                    reasoning,
                })
            }
            Err(e) => {
                warn!("Failed to parse confidence assessment: {}. Using fallback.", e);
                // Fallback: assume low confidence for safety
                Ok(ConfidenceAssessment {
                    confidence: 0.4,
                    is_sufficient: false,
                    missing_pieces: vec![
                        MissingPiece {
                            field: "systems".to_string(),
                            description: "Could not determine which systems to compare".to_string(),
                            importance: Importance::Required,
                            suggestions: vec!["system_a".to_string(), "system_b".to_string()],
                        },
                        MissingPiece {
                            field: "metrics".to_string(),
                            description: "Could not determine which metrics to analyze".to_string(),
                            importance: Importance::Required,
                            suggestions: vec!["TOS".to_string(), "recovery".to_string(), "balance".to_string()],
                        },
                    ],
                    partial_intent: PartialIntent::default(),
                    reasoning: format!("Failed to parse LLM response: {}", e),
                })
            }
        }
    }

    // ========================================================================
    // CLARIFICATION QUESTION GENERATION
    // ========================================================================

    /// Generate ONE clarification question covering all missing pieces
    async fn generate_clarification_question(
        &self,
        assessment: &ConfidenceAssessment,
        original_query: &str,
    ) -> Result<ClarificationRequest> {
        // Build context for question generation
        let missing_required: Vec<_> = assessment.missing_pieces.iter()
            .filter(|p| p.importance == Importance::Required)
            .collect();
        
        let missing_helpful: Vec<_> = assessment.missing_pieces.iter()
            .filter(|p| p.importance == Importance::Helpful)
            .collect();
        
        // Generate the ONE question
        let question = self.build_clarification_question(
            &missing_required,
            &missing_helpful,
            &assessment.partial_intent,
            original_query,
        ).await?;
        
        // Build response hints
        let response_hints = self.build_response_hints(&assessment.missing_pieces);
        
        Ok(ClarificationRequest {
            question,
            missing_pieces: assessment.missing_pieces.clone(),
            confidence: assessment.confidence,
            partial_understanding: assessment.partial_intent.clone(),
            response_hints,
        })
    }

    async fn build_clarification_question(
        &self,
        required: &[&MissingPiece],
        helpful: &[&MissingPiece],
        partial: &PartialIntent,
        original_query: &str,
    ) -> Result<String> {
        // Determine task type from query to provide context-aware clarification
        let query_lower = original_query.to_lowercase();
        let is_reconciliation = query_lower.contains("recon") || 
                               query_lower.contains("compare") || 
                               query_lower.contains("mismatch");
        let task_context = if is_reconciliation {
            "reconciliation/comparison"
        } else {
            "analysis"
        };
        
        // Determine how many systems might be needed
        let system_requirement = if is_reconciliation {
            "multiple systems (for comparison)"
        } else {
            "one or more systems"
        };
        
        // If LLM available, use it to generate natural question
        let prompt = format!(r#"Generate ONE clear, friendly clarification question for a data analyst.

ORIGINAL QUERY: "{}"

WHAT WE UNDERSTOOD:
- Task type: {:?}
- Systems detected: {:?}
- Metrics: {:?}
- Entities: {:?}
- Task context: {} (requires {})

MISSING REQUIRED INFO:
{}

MISSING HELPFUL INFO:
{}

RULES:
1. Generate EXACTLY ONE question that covers ALL missing pieces
2. Be conversational and friendly
3. Provide examples where helpful
4. Keep it concise but complete
5. Do NOT explicitly mention "two systems" - let the user specify what they need
6. For reconciliation tasks, ask for systems to compare without specifying a number
7. Output ONLY the question text, nothing else

EXAMPLE OUTPUTS:
- For reconciliation: "I need a bit more context. Could you specify: (1) which systems you want to compare (e.g., khatabook vs tally, or system_a vs system_b vs system_c), (2) what metric you're interested in (e.g., TOS, recovery, balance), and optionally (3) any filters like date range or loan type?"
- For single system: "I need a bit more context. Could you specify: (1) which system you want to analyze, (2) what metric you're interested in, and optionally (3) any filters?"
"#,
            original_query,
            partial.task_type,
            partial.systems,
            partial.metrics,
            partial.entities,
            task_context,
            system_requirement,
            required.iter()
                .map(|p| format!("- {}: {} (suggestions: {:?})", p.field, p.description, p.suggestions))
                .collect::<Vec<_>>()
                .join("\n"),
            helpful.iter()
                .map(|p| format!("- {}: {} (suggestions: {:?})", p.field, p.description, p.suggestions))
                .collect::<Vec<_>>()
                .join("\n"),
        );

        let response = self.llm.call_llm(&prompt).await?;
        
        // Clean up response
        let question = response.trim()
            .trim_matches('"')
            .trim()
            .to_string();
        
        // Fallback if LLM returns empty
        if question.is_empty() {
            return Ok(self.fallback_clarification_question(required, helpful));
        }
        
        Ok(question)
    }

    fn fallback_clarification_question(
        &self,
        required: &[&MissingPiece],
        helpful: &[&MissingPiece],
    ) -> String {
        let mut parts = Vec::new();
        
        // Add required pieces - dynamically adjust language based on field
        for (i, piece) in required.iter().enumerate() {
            let suggestions = if piece.suggestions.is_empty() {
                String::new()
            } else {
                format!(" (e.g., {})", piece.suggestions.join(", "))
            };
            
            // Customize description for systems field to avoid mentioning specific numbers
            let description = if piece.field == "systems" {
                "which systems you want to compare or analyze".to_string()
            } else {
                piece.description.clone()
            };
            
            parts.push(format!("({}) {}{}", i + 1, description, suggestions));
        }
        
        // Add helpful pieces
        let offset = required.len();
        for (i, piece) in helpful.iter().enumerate() {
            let suggestions = if piece.suggestions.is_empty() {
                String::new()
            } else {
                format!(" (e.g., {})", piece.suggestions.join(", "))
            };
            parts.push(format!("({}) [optional] {}{}", offset + i + 1, piece.description, suggestions));
        }
        
        if parts.is_empty() {
            "Could you provide more details about what you'd like to analyze?".to_string()
        } else {
            format!(
                "I need a bit more information to help you. Could you please specify: {}",
                parts.join("; ")
            )
        }
    }

    fn build_response_hints(&self, missing: &[MissingPiece]) -> Vec<String> {
        missing.iter()
            .flat_map(|p| {
                if p.suggestions.is_empty() {
                    vec![format!("{}: <your value>", p.field)]
                } else {
                    p.suggestions.iter()
                        .map(|s| format!("{}: {}", p.field, s))
                        .collect()
                }
            })
            .collect()
    }

    // ========================================================================
    // ORIGINAL COMPILE (kept for backward compatibility)
    // ========================================================================

    /// Compile natural language query to IntentSpec (legacy method)
    /// 
    /// For new code, prefer `compile_with_clarification()` which supports
    /// fail-fast with clarification questions.
    pub async fn compile(&self, query: &str) -> Result<IntentSpec> {
        info!("Compiling intent from query: {}", query);
        
        let schema_prompt = self.get_schema_prompt();
        let user_prompt = format!("Query: {}\n\nCompile this query into the IntentSpec JSON schema.", query);
        
        for attempt in 0..=self.max_retries {
            debug!("Compilation attempt {}", attempt + 1);
            
            match compile_intent_helper(&self.llm, &schema_prompt, &user_prompt).await {
                Ok(json_str) => {
                    match self.parse_and_validate(&json_str) {
                        Ok(spec) => {
                            info!("Successfully compiled intent: {:?}", spec.task_type);
                            return Ok(spec);
                        }
                        Err(e) => {
                            warn!("Failed to parse/validate JSON on attempt {}: {}", attempt + 1, e);
                            if attempt < self.max_retries {
                                continue;
                            } else {
                                return Err(RcaError::Llm(format!(
                                    "Failed to compile intent after {} attempts: {}",
                                    self.max_retries + 1, e
                                )));
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("LLM call failed on attempt {}: {}", attempt + 1, e);
                    if attempt < self.max_retries {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        
        Err(RcaError::Llm("Failed to compile intent after all retries".to_string()))
    }

    fn parse_and_validate(&self, json_str: &str) -> Result<IntentSpec> {
        // Extract JSON from markdown code blocks if present
        let json_str = self.extract_json(json_str);
        
        // Parse JSON
        let mut spec: IntentSpec = serde_json::from_str(&json_str)
            .map_err(|e| RcaError::Llm(format!("Invalid JSON: {}", e)))?;
        
        // Enhance join types using inference engine (if metadata available)
        // Note: This requires metadata, which we don't have in the compiler.
        // The enhancement will happen later when metadata is available.
        
        // Validate schema structure (syntax validation)
        self.validate_schema(&spec)?;
        
        // NOTE: Actual table/column validation happens later via IntentValidator
        // when metadata is available. This prevents hallucinated names from being used.
        
        Ok(spec)
    }
    
    /// Validate intent against metadata to prevent hallucination
    /// 
    /// This should be called after parsing and before SQL generation.
    /// It ensures all tables, columns, and relationships actually exist.
    /// Optionally uses learning store for user-approved corrections.
    pub fn validate_against_metadata(
        intent: &mut IntentSpec,
        metadata: &Metadata,
    ) -> Result<crate::intent_validator::ValidationResult> {
        Self::validate_against_metadata_with_learning(intent, metadata, None)
    }
    
    /// Validate intent against metadata with optional learning store
    pub fn validate_against_metadata_with_learning(
        intent: &mut IntentSpec,
        metadata: &Metadata,
        learning_store: Option<std::sync::Arc<crate::learning_store::LearningStore>>,
    ) -> Result<crate::intent_validator::ValidationResult> {
        use crate::intent_validator::IntentValidator;
        
        let mut validator = IntentValidator::new(metadata.clone());
        if let Some(store) = learning_store {
            validator = validator.with_learning_store(store);
        }
        
        let result = validator.resolve_intent(intent)?;
        
        if !result.is_valid {
            warn!("Intent validation found errors: {:?}", result.errors);
            warn!("Intent validation warnings: {:?}", result.warnings);
        }
        
        Ok(result)
    }
    
    /// Enhance join types in intent spec using intelligent inference engine
    /// 
    /// This should be called after parsing when metadata is available.
    /// It will intelligently infer join types by reasoning like a human analyst.
    pub fn enhance_join_types(
        intent: &mut IntentSpec,
        metadata: &Metadata,
        original_query: &str,
    ) -> Result<()> {
        use crate::join_inference::{JoinTypeInferenceEngine, QueryLanguageHints};
        
        let inference_engine = JoinTypeInferenceEngine::new(metadata.clone());
        let query_hints = QueryLanguageHints::from_query(original_query);
        
        // Enhance each join that doesn't have an explicit type
        for join in &mut intent.joins {
            if join.join_type.is_none() {
                // Intelligently infer join type using business context
                let inference = inference_engine.infer_join_type(
                    &join.left_table,
                    &join.right_table,
                    None, // No explicit type
                    Some(&query_hints),
                    Some(&intent.task_type), // Pass task type for context
                    Some(original_query), // Pass query for context analysis
                )?;
                
                join.join_type = Some(inference.join_type.clone());
                join.confidence = inference.confidence;
                join.reasoning = Some(format!(
                    "Intelligently inferred from {}: {}",
                    inference.source,
                    inference.reasoning
                ));
                
                info!(
                    "Enhanced join {} -> {}: type={}, confidence={:.2}, source={}, reasoning={}",
                    join.left_table,
                    join.right_table,
                    inference.join_type,
                    inference.confidence,
                    inference.source,
                    inference.reasoning
                );
                
                // Log alternatives for transparency
                if !inference.alternatives.is_empty() {
                    debug!(
                        "Alternative join types considered: {:?}",
                        inference.alternatives.iter()
                            .map(|a| format!("{} (conf={:.2})", a.join_type, a.confidence))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
        
        Ok(())
    }

    fn extract_json(&self, text: &str) -> String {
        // Remove markdown code blocks if present
        let text = text.trim();
        if text.starts_with("```json") {
            text.strip_prefix("```json")
                .or_else(|| text.strip_prefix("```"))
                .and_then(|s| s.strip_suffix("```"))
                .map(|s| s.trim())
                .unwrap_or(text)
                .to_string()
        } else if text.starts_with("```") {
            text.strip_prefix("```")
                .and_then(|s| s.strip_suffix("```"))
                .map(|s| s.trim())
                .unwrap_or(text)
                .to_string()
        } else {
            text.to_string()
        }
    }

    fn validate_schema(&self, spec: &IntentSpec) -> Result<()> {
        // Validate task type
        match spec.task_type {
            TaskType::RCA => {
                if spec.systems.is_empty() {
                    return Err(RcaError::Llm("RCA task requires at least one system".to_string()));
                }
                if spec.target_metrics.is_empty() {
                    return Err(RcaError::Llm("RCA task requires at least one target metric".to_string()));
                }
            }
            TaskType::DV => {
                if spec.validation_constraint.is_none() {
                    return Err(RcaError::Llm("DV task requires validation_constraint".to_string()));
                }
            }
            TaskType::QUERY => {
                if spec.systems.is_empty() {
                    return Err(RcaError::Llm("QUERY task requires at least one system".to_string()));
                }
                if spec.target_metrics.is_empty() {
                    return Err(RcaError::Llm("QUERY task requires at least one target metric".to_string()));
                }
            }
        }
        
        // Validate grain is not empty
        if spec.grain.is_empty() {
            return Err(RcaError::Llm("Grain cannot be empty".to_string()));
        }
        
        // Validate joins if present
        for join in &spec.joins {
            if join.left_table.is_empty() || join.right_table.is_empty() {
                return Err(RcaError::Llm("Join specification requires both left_table and right_table".to_string()));
            }
            if join.conditions.is_empty() {
                warn!("Join between {} and {} has no conditions specified", join.left_table, join.right_table);
            }
        }
        
        Ok(())
    }

    fn get_schema_prompt(&self) -> String {
        r#"You are an Intent Compiler. Your job is to compile natural language queries into strict JSON specifications.

You MUST output ONLY valid JSON matching this exact schema:

{
  "task_type": "RCA" | "DV" | "QUERY",
  "target_metrics": ["metric1", "metric2"],
  "entities": ["entity1", "entity2"],
  "constraints": [
    {
      "column": "column_name",
      "operator": "=" | ">" | "<" | ">=" | "<=" | "!=" | "in" | "contains",
      "value": <json_value>,
      "description": "human readable description"
    }
  ],
  "grain": ["grain_column1", "grain_column2"],
  "time_scope": {
    "as_of_date": "YYYY-MM-DD" | null,
    "start_date": "YYYY-MM-DD" | null,
    "end_date": "YYYY-MM-DD" | null,
    "time_grain": "daily" | "monthly" | "yearly" | null
  } | null,
  "systems": ["system1", "system2"],
  "validation_constraint": {
    "constraint_type": "value" | "range" | "set" | "uniqueness" | "nullability" | "referential" | "aggregation" | "cross_column" | "format" | "drift" | "volume" | "freshness" | "schema" | "cardinality" | "composition",
    "description": "human readable description",
    "details": { <any json object with constraint-specific details> }
  } | null,
  "joins": [
    {
      "left_table": "table1",
      "right_table": "table2",
      "join_type": "INNER" | "LEFT" | "RIGHT" | "FULL" | null,
      "conditions": [
        {
          "left_column": "col1",
          "right_column": "col2",
          "operator": "=" | null
        }
      ],
      "confidence": 0.0-1.0,
      "reasoning": "explanation" | null
    }
  ],
  "tables": ["table1", "table2"]
}

JOIN INFERENCE RULES:
1. Extract join information from query language:
   - "join X with Y", "combine X and Y", "X joined to Y" → Extract tables and infer join
   - "include all records from X" → LEFT join (preserve all from X)
   - "only matching records", "where X exists in Y" → INNER join
   - "all records from both tables" → FULL join
   - "X that have Y" → LEFT join (X is left, Y is right)
   - "X where Y exists" → INNER join
   - "X including Y" → LEFT join
   - "X excluding Y" → LEFT join with NOT EXISTS filter

2. Infer join conditions from query:
   - Look for explicit join keys: "on customer_id", "by loan_id", "using account_number"
   - Infer from entity relationships: customer -> loan (customer_id), loan -> transaction (loan_id)
   - Common patterns: "X.customer_id = Y.customer_id", "X.id = Y.foreign_id"
   - If not specified, use common foreign key patterns (entity_id, id, etc.)

3. Join type inference priority:
   a) Explicit in query ("inner join", "left join", etc.) → Use that
   b) Query language hints ("all", "include all", "only matching") → Infer from language
   c) If not specified → Set to null (will be inferred from lineage metadata later)

4. For complex multi-table joins:
   - Extract all mentioned tables into "tables" array
   - Create JoinSpec for each pair that needs joining
   - Order matters: first join is left_table, subsequent joins chain

5. Confidence scoring:
   - 1.0: Explicit join type and conditions in query
   - 0.8-0.9: Clear language hints + explicit conditions
   - 0.6-0.7: Language hints but inferred conditions
   - 0.4-0.5: Only tables mentioned, no clear join info
   - <0.4: Ambiguous, should ask for clarification

Rules:
- For RCA: systems (2+) and target_metrics are required
- For DV: validation_constraint is required
- For QUERY: systems (1+) and target_metrics are required
- grain is always required (cannot be empty)
- IMPORTANT: grain should be entity-level keys (e.g., ["loan_id"], ["customer_id"], ["account_id"])
- DO NOT use filter values as grain (e.g., if query says "for PERSONAL loans", grain should be ["loan_id"], NOT ["loan_type"])
- If query mentions a filter like "for PERSONAL loans", add it as a constraint with column="loan_type", operator="=", value="PERSONAL"
- Extract ALL join information from query - even if implicit
- If multiple tables are mentioned, create join specs for relationships
- Output ONLY the JSON object, no markdown, no explanation, no code blocks
- If uncertain about a field, use null or empty array
- Be precise and extract all relevant information from the query"#.to_string()
    }
}

// Helper function to compile intent using LlmClient
async fn compile_intent_helper(
    llm: &LlmClient,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<String> {
    // Use existing call_llm method with combined prompt
    let combined_prompt = format!("{}\n\n{}", system_prompt, user_prompt);
    
    // Check for mock mode by trying to call and checking response
    let response = llm.call_llm(&combined_prompt).await?;
    
    // If response looks like mock, return it; otherwise it's real
    Ok(response)
}

fn mock_compile_intent(query: &str) -> String {
    // Mock implementation for testing
    let query_lower = query.to_lowercase();
    
    if query_lower.contains("~dv") || query_lower.contains("validation") || query_lower.contains("must") || query_lower.contains("cannot") {
        // Data Validation task
        r#"{
  "task_type": "DV",
  "target_metrics": [],
  "entities": ["loan"],
  "constraints": [],
  "grain": ["loan_id"],
  "time_scope": null,
  "systems": [],
  "validation_constraint": {
    "constraint_type": "value",
    "description": "Mock validation constraint",
    "details": {}
  }
}"#.to_string()
    } else {
        // RCA task
        r#"{
  "task_type": "RCA",
  "target_metrics": ["tos"],
  "entities": ["loan"],
  "constraints": [],
  "grain": ["loan_id"],
  "time_scope": {
    "as_of_date": "2025-12-31",
    "start_date": null,
    "end_date": null,
    "time_grain": null
  },
  "systems": ["khatabook", "tb"],
  "validation_constraint": null
}"#.to_string()
    }
}

