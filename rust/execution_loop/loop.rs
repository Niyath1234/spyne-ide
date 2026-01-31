//! Execution Loop
//! 
//! Bounded retry loop with error recovery.

use crate::error::{RcaError, Result};
use crate::execution_loop::error_classifier::{ErrorClassifier, SqlErrorClass};
use crate::execution_loop::error_recovery::ErrorRecovery;
use crate::execution_loop::query_plan::{QueryPlan, generate_query_plan};
use crate::intent::SemanticSqlIntent;
use crate::llm::LlmClient;
use crate::intent::function_schema::{ChatMessage, FunctionDefinition, generate_sql_intent_function, parse_function_call};
use crate::semantic::registry::SemanticRegistry;
use crate::schema_rag::retriever::{RetrievedSchema, SchemaRAG};
use std::sync::Arc;
use tracing::{info, warn};

/// Execution context for the loop
pub struct ExecutionContext {
    pub schema_rag: Arc<SchemaRAG>,
    pub semantic_registry: Arc<dyn SemanticRegistry>,
    pub llm: LlmClient,
}

/// Execution result
pub struct ExecutionResult {
    pub intent: SemanticSqlIntent,
    pub sql: String,
    pub attempts: u8,
}

/// Execution loop with bounded retries
pub struct ExecutionLoop {
    max_attempts: u8,
    abort_on_repeat_error: bool,
    error_classifier: ErrorClassifier,
}

impl ExecutionLoop {
    pub fn new(max_attempts: u8, abort_on_repeat_error: bool) -> Self {
        Self {
            max_attempts,
            abort_on_repeat_error,
            error_classifier: ErrorClassifier::new(),
        }
    }

    /// Execute with retry logic
    pub async fn execute_with_retry(
        &self,
        query: &str,
        context: &ExecutionContext,
    ) -> Result<ExecutionResult> {
        let mut previous_error: Option<SqlErrorClass> = None;
        let mut previous_intent: Option<SemanticSqlIntent> = None;
        let mut error_recovery = ErrorRecovery::new(Arc::clone(&context.semantic_registry));

        for attempt in 1..=self.max_attempts {
            info!("Execution attempt {} of {}", attempt, self.max_attempts);

            // Step 1: Generate query plan (determine query type, entities, constraints)
            let plan = match generate_query_plan(query, &context.llm).await {
                Ok(p) => {
                    info!("Query plan: {:?}", p);
                    Some(p)
                }
                Err(e) => {
                    warn!("Failed to generate query plan: {}, continuing without plan", e);
                    None
                }
            };

            // Step 2: Retrieve relevant schema using multi-layered filtering
            let min_similarity = 0.5; // Threshold for filtering low-confidence matches
            let retrieved_schema = if let Some(ref plan) = plan {
                context
                    .schema_rag
                    .retrieve_relevant_schema_with_plan(query, 20, Some(plan), min_similarity)
                    .await?
            } else {
                context
                    .schema_rag
                    .retrieve_relevant_schema(query, 10)
                    .await?
            };

            // Step 3: Build prompt with schema context and plan constraints
            let mut prompt = self.build_prompt(query, &retrieved_schema, plan.as_ref());

            // Step 4: Add error recovery context if this is a retry
            if attempt > 1 {
                if let (Some(ref prev_error), Some(ref prev_intent)) = (&previous_error, &previous_intent) {
                    let recovery_prompt = error_recovery.build_recovery_prompt(
                        prev_error,
                        prev_intent,
                        attempt,
                    );
                    prompt = format!("{}\n\n{}", recovery_prompt, prompt);
                }
            }

            // Step 5: Generate intent using function calling
            let intent = match self.generate_intent(&context.llm, &prompt).await {
                Ok(i) => i,
                Err(e) => {
                    let error_class = self.error_classifier.classify(&e);
                    if self.should_abort(&error_class, &previous_error) {
                        return Err(e);
                    }
                    previous_error = Some(error_class);
                    previous_intent = None;
                    continue;
                }
            };

            // Step 6: Validate semantically
            if let Err(e) = self.validate_intent(&intent, context.semantic_registry.as_ref()) {
                let error_class = self.error_classifier.classify(&e);
                if self.should_abort(&error_class, &previous_error) {
                    return Err(e);
                }
                previous_error = Some(error_class);
                previous_intent = Some(intent);
                continue;
            }

            // Step 7: Compile SQL (this will be done in integration phase)
            // For now, we'll just return success
            let sql = format!(
                "SELECT {} FROM {}",
                intent.metrics.join(", "),
                "base_table" // Will be resolved from metric in integration
            );

            info!("✅ Execution succeeded on attempt {}", attempt);
            return Ok(ExecutionResult {
                intent,
                sql,
                attempts: attempt,
            });
        }

        Err(RcaError::Execution(format!(
            "Max retries ({}) exceeded",
            self.max_attempts
        )))
    }

    /// Generate intent using function calling
    async fn generate_intent(
        &self,
        llm: &LlmClient,
        prompt: &str,
    ) -> Result<SemanticSqlIntent> {
        let function = generate_sql_intent_function();

        let messages = vec![
            ChatMessage {
                role: "system".to_string(),
                content: Some(
                    "You are a SQL intent generator. Use the generate_sql_intent function to create structured intents from natural language queries.".to_string(),
                ),
                function_call: None,
                name: None,
            },
            ChatMessage {
                role: "user".to_string(),
                content: Some(prompt.to_string()),
                function_call: None,
                name: None,
            },
        ];

        let function_call = llm
            .call_llm_with_functions(&messages, &[function])
            .await?;

        parse_function_call(&function_call)
    }

    /// Validate intent against semantic registry
    fn validate_intent(
        &self,
        intent: &SemanticSqlIntent,
        registry: &dyn SemanticRegistry,
    ) -> Result<()> {
        // For relational queries, metrics can be empty
        // For metric queries, at least one metric is required
        if intent.metrics.is_empty() {
            // This is OK for relational queries, but we'll validate later in SQL compilation
            // For now, just validate dimensions exist
            for dim_name in &intent.dimensions {
                if registry.dimension(dim_name).is_none() {
                    return Err(RcaError::Execution(format!(
                        "Dimension '{}' not found",
                        dim_name
                    )));
                }
            }
            return Ok(());
        }

        // Validate metrics exist
        for metric_name in &intent.metrics {
            if registry.metric(metric_name).is_none() {
                return Err(RcaError::Execution(format!(
                    "Metric '{}' not found",
                    metric_name
                )));
            }
        }

        // Validate dimensions exist and are allowed
        for metric_name in &intent.metrics {
            if let Some(metric) = registry.metric(metric_name) {
                let allowed_dims: std::collections::HashSet<String> = metric
                    .allowed_dimensions()
                    .iter()
                    .cloned()
                    .collect();

                for dim_name in &intent.dimensions {
                    if !allowed_dims.contains(dim_name) {
                        return Err(RcaError::Execution(format!(
                            "Dimension '{}' is not allowed for metric '{}'",
                            dim_name, metric_name
                        )));
                    }

                    if registry.dimension(dim_name).is_none() {
                        return Err(RcaError::Execution(format!(
                            "Dimension '{}' not found",
                            dim_name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Build prompt with schema context and plan constraints
    fn build_prompt(&self, query: &str, schema: &RetrievedSchema, plan: Option<&QueryPlan>) -> String {
        let mut parts = Vec::new();

        parts.push(format!("USER QUESTION: {}", query));
        
        // Add query plan information if available
        if let Some(plan) = plan {
            parts.push(format!("\nQUERY PLAN:"));
            parts.push(format!("- Query Type: {:?}", plan.query_type));
            parts.push(format!("- Output Cardinality: {:?}", plan.output_cardinality));
            parts.push(format!("- Entities: {:?}", plan.entities));
            if !plan.exclude_entities.is_empty() {
                parts.push(format!("- Exclude Entities: {:?}", plan.exclude_entities));
            }
            parts.push(format!("- Aggregations Allowed: {}", plan.aggregations_allowed));
            parts.push(format!("- GROUP BY Allowed: {}", plan.group_by_allowed));
            parts.push(format!("- JOINs Allowed: {}", plan.joins_allowed));
        }
        
        parts.push("\nRELEVANT SCHEMA:".to_string());

        // Only show metrics if plan allows it (or if no plan)
        let should_show_metrics = plan.map(|p| p.should_show_metrics()).unwrap_or(true);
        if should_show_metrics && !schema.metrics.is_empty() {
            parts.push(format!("\nAvailable metrics: {}", schema.metrics.join(", ")));
        }

        if !schema.dimensions.is_empty() {
            parts.push(format!("Available dimensions: {}", schema.dimensions.join(", ")));
        }

        // Only show tables if plan allows it (or if no plan)
        let should_show_tables = plan.map(|p| p.should_show_tables()).unwrap_or(true);
        if should_show_tables && !schema.tables.is_empty() {
            parts.push("\nRelevant tables:".to_string());
            for table in &schema.tables {
                parts.push(format!("- {} (system: {}, entity: {})", table.name, table.system, table.entity));
            }
        }

        // Add constraints based on plan
        if let Some(plan) = plan {
            if !plan.aggregations_allowed {
                parts.push("\nCONSTRAINT: Aggregations (SUM, COUNT, AVG, etc.) are NOT allowed.".to_string());
            }
            if !plan.group_by_allowed {
                parts.push("CONSTRAINT: GROUP BY is NOT allowed.".to_string());
            }
            if plan.joins_allowed {
                match plan.query_type {
                    crate::execution_loop::query_plan::QueryType::Relational => {
                        parts.push("CONSTRAINT: JOINs are allowed. Use INNER JOIN for required relationships.".to_string());
                    }
                    _ => {
                        parts.push("CONSTRAINT: JOINs are allowed.".to_string());
                    }
                }
            }
        }
        
        parts.push("\nDIMENSION USAGE SEMANTICS (CRITICAL):".to_string());
        parts.push("- For each dimension, specify 'usage': 'select', 'filter', or 'both'".to_string());
        parts.push("- 'select': Dimension appears in SELECT/GROUP BY (augmentation) - e.g., 'Show revenue by region'".to_string());
        parts.push("- 'filter': Dimension appears in WHERE (restriction) - e.g., 'Show revenue for VIP customers'".to_string());
        parts.push("- 'both': Used for both - e.g., 'Show revenue by region for VIP customers'".to_string());
        parts.push("- DO NOT specify join types - the compiler determines them automatically".to_string());
        parts.push("\nSPECIAL DIMENSION USAGE:".to_string());
        parts.push("- order_type_transformed: Use for CASE transformation (credin → credin, else → Digital)".to_string());
        parts.push("- region_literal: Hardcoded 'OS' value - use when region = 'OS'".to_string());
        parts.push("- product_group_literal: Hardcoded 'Digital' value - use when product_group = 'Digital'".to_string());
        parts.push("- nbfc_name_coalesced: Use for NBFC filtering (uses COALESCE of colending and parent)".to_string());
        parts.push("- order_type_filter: Use for case-insensitive order type filtering (uses lower())".to_string());
        parts.push("- writeoff_status: Use with IS NULL operator to exclude written off orders (usage: 'filter')".to_string());
        parts.push("- provisional_writeoff_status: Use with IS NULL operator to exclude provisionally written off orders (usage: 'filter')".to_string());
        parts.push("\nRELATIVE DATES:".to_string());
        parts.push("- Use relative_date field in filters for date dimensions".to_string());
        parts.push("- Patterns: '2_days_ago', 'yesterday', 'today', 'N_days_ago' (e.g., '3_days_ago')".to_string());
        parts.push("- Example: {\"dimension\": \"last_day_dim\", \"operator\": \"=\", \"relative_date\": \"2_days_ago\"}".to_string());
        parts.push("\nFILTER OPERATORS:".to_string());
        parts.push("- IS NULL: Use with writeoff_status, provisional_writeoff_status (no value needed)".to_string());
        parts.push("- IN / NOT IN: Use with array values".to_string());
        parts.push("- =, !=, >, <, >=, <=: Standard operators".to_string());
        parts.push("\nGenerate a SQL intent using the generate_sql_intent function.".to_string());

        parts.join("\n")
    }

    /// Determine if we should abort based on error patterns
    fn should_abort(
        &self,
        current_error: &SqlErrorClass,
        previous_error: &Option<SqlErrorClass>,
    ) -> bool {
        if !self.abort_on_repeat_error {
            return false;
        }

        if let Some(ref prev) = previous_error {
            // Abort if same error repeats
            if prev == current_error {
                warn!("Same error repeated, aborting: {:?}", current_error);
                return true;
            }
        }

        false
    }
}

