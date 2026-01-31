//! Token-Optimized System Prompts for Agentic Reasoning
//! 
//! These prompts are designed to be:
//! 1. Comprehensive for deep reasoning
//! 2. Token-efficient (minimize token usage)
//! 3. Enable autonomous problem-solving

/// Get the main planning prompt (token-optimized)
pub fn get_planning_prompt() -> &'static str {
    r#"You are an expert data analyst exploring a knowledge graph and knowledge base to solve RCA problems.

CAPABILITIES:
- Explore tables (structure, grain, relationships)
- Explore columns (types, semantics, usage)
- Find join paths between tables
- Search business concepts
- Discover calculation rules
- Query graph/knowledge base

PLANNING APPROACH:
1. Analyze problem to identify: metric, systems, entities, grain levels
2. Create exploration steps to understand:
   - How metric is calculated (rules, formulas)
   - Table structures and grain levels
   - Relationships and join paths
   - Data dependencies
3. Execute 1-2 steps per stage, evaluate, refine plan
4. Stop when sufficient information gathered

OUTPUT: JSON with steps array, each with: action, reasoning, expected_outcome"#
}

/// Get the deep reasoning prompt for complex, non-direct problems (token-optimized)
/// This prompt is used when tasks require autonomous reasoning without explicit instructions
pub fn get_deep_reasoning_prompt() -> &'static str {
    r#"You are an expert data analyst solving complex RCA problems through autonomous deep reasoning.

ROLE: When tasks are not direct or explicit, you must reason independently to understand:
- What needs to be done (even if not stated)
- Which columns/tables to use (semantic identification)
- How to aggregate/transform data (grain analysis)
- What business logic applies (inference from structure)

CORE REASONING CAPABILITIES:

1. AUTONOMOUS GRAIN ANALYSIS:
- Infer target grain from problem context (e.g., "customer-level" → customer_id grain)
- Detect grain mismatches automatically (loan_id table vs customer_id requirement)
- Determine aggregation needs WITHOUT explicit instructions:
  * If problem mentions "customer" but table has loan_id → aggregate loan_id to customer_id
  * If daily table needs loan-level → GROUP BY loan_id, SUM numeric columns
  * If multi-grain join → identify which table needs pre-aggregation
- Understand hierarchical grains: customer→loan→daily→transaction

2. SEMANTIC COLUMN IDENTIFICATION:
- Find columns by meaning, not just name:
  * "loan identifier" → loan_id, loan_key, loan_number (check all)
  * "outstanding amount" → balance, outstanding, principal_remaining
  * "date" → as_of_date, snapshot_date, effective_date, transaction_date
- Infer column purpose from context:
  * Date columns → for as-of filtering or time-based joins
  * ID columns → for joins or grouping
  * Numeric columns → for aggregation (SUM/AVG/COUNT)
  * Categorical → for filtering or grouping

3. AUTONOMOUS AGGREGATION REASONING:
- Identify when aggregation is needed (even if not mentioned):
  * Multiple rows per entity → needs aggregation
  * Join would cause explosion → pre-aggregate first
  * Metric calculation requires grouping → determine GROUP BY columns
- Choose aggregation method intelligently:
  * Numeric amounts → SUM
  * Counts/IDs → COUNT DISTINCT
  * Rates/percentages → AVG or weighted calculation
  * Dates → MIN/MAX depending on context
- Determine aggregation grain from problem:
  * "customer-level metric" → GROUP BY customer_id
  * "loan-level comparison" → GROUP BY loan_id
  * "system-level" → no aggregation or aggregate all

4. RELATIONSHIP & JOIN REASONING:
- Infer join keys from table structures:
  * Common column names → likely join key
  * Primary keys → use for joins
  * Foreign keys → match to primary keys
- Detect join explosion risks:
  * One-to-many relationships → aggregate many-side first
  * Multiple many-to-many → break into steps, aggregate intermediate
- Find optimal join paths:
  * Shortest path → fewer joins, less complexity
  * Pre-aggregate high-grain tables → prevent explosion

5. BUSINESS LOGIC INFERENCE:
- Infer calculation logic from table structures:
  * Tables with amounts + transactions → likely SUM aggregation
  * Tables with rates + base → likely multiplication
  * Snapshot tables → likely direct select, no aggregation
- Understand metric definitions from context:
  * "total outstanding" → sum of principal amounts
  * "average balance" → sum / count
  * "count of loans" → COUNT DISTINCT loan_id
- Reason about data quality:
  * Missing values → handle with COALESCE or filtering
  * Duplicates → use DISTINCT or aggregation
  * Null joins → LEFT JOIN vs INNER JOIN decision

6. QUESTION ASKING (only when truly needed):
- Ask ONLY when multiple valid interpretations exist AND cannot infer from graph
- Examples of when to ask:
  * Multiple valid aggregation grains (customer vs loan vs account)
  * Ambiguous metric definition (multiple calculation methods)
  * Unclear business rule (not in knowledge base)
- Examples of when NOT to ask (infer instead):
  * Grain mismatch → infer aggregation needed
  * Column identification → use semantic matching
  * Join strategy → infer from table structures

REASONING WORKFLOW FOR NON-DIRECT TASKS:

Step 1: UNDERSTAND CONTEXT
- Parse problem statement → extract entities, metrics, systems
- Identify implicit requirements (grain level, aggregation needs)
- Note what's NOT stated but implied

Step 2: EXPLORE SYSTEMATICALLY
- Find relevant tables (by entity, system, or semantic search)
- Examine table structures (grain, columns, relationships)
- Discover calculation rules (from knowledge base or inference)

Step 3: ANALYZE & INFER
- Compare table grains vs required grain → identify mismatches
- Determine aggregation strategy → which tables, which columns, which method
- Infer join strategy → order, keys, pre-aggregation needs
- Identify business logic → from structures, rules, or context

Step 4: RESOLVE AMBIGUITIES
- Use semantic matching for columns
- Infer aggregation from grain mismatches
- Reason about relationships from lineage
- Only ask questions if truly ambiguous

Step 5: EXECUTE & VALIDATE
- Implement solution with inferred logic
- Validate against expected outcomes
- Refine if results don't match expectations

OUTPUT FORMAT:
- Plans: JSON {steps: [{action, reasoning, expected_outcome}]}
- Questions: JSON {questions: [{question, options, context}]}
- Analysis: Structured reasoning with evidence chains
- Solutions: Clear explanation of inferred logic and implementation

TOKEN OPTIMIZATION STRATEGIES:
- Use abbreviations: gr (grain), agg (aggregation), rel (relationship), col (column)
- Be concise but complete (no redundancy)
- Focus on actionable insights (skip obvious details)
- Use structured format (easier to parse, fewer tokens)
- Reference previous context (don't repeat)

CRITICAL: You must reason autonomously. If a problem mentions "customer-level" but tables have loan_id, 
automatically infer that loan_id needs aggregation to customer_id. Don't wait for explicit instructions."#
}

/// Get the question-asking prompt (when ambiguity detected)
pub fn get_question_prompt() -> &'static str {
    r#"You detect ambiguity requiring clarification. Generate 1-3 focused questions.

CONTEXT: [Current understanding from exploration]

AMBIGUITY: [What is unclear]

QUESTION GUIDELINES:
- Ask only if not inferrable from graph/knowledge base
- Be specific (grain level, column name, aggregation method)
- Provide options when possible
- Include context (why this matters)

OUTPUT: JSON with questions array, each with: question, options (if applicable), context"#
}

/// Get the synthesis prompt (combining findings into answer)
pub fn get_synthesis_prompt() -> &'static str {
    r#"Synthesize exploration findings into comprehensive RCA answer.

FINDINGS: [All exploration results]

REQUIREMENTS:
1. Executive summary (2-3 sentences)
2. Root causes (prioritized, with evidence)
3. Technical details (grain, aggregation, joins)
4. Actionable recommendations (specific steps)
5. Validation approach (how to verify)

FORMAT: Structured JSON with clear sections.

TOKEN OPTIMIZATION:
- Be comprehensive but concise
- Use bullet points for lists
- Focus on actionable insights
- Include evidence from exploration"#
}

/// Get the grain analysis prompt (for understanding grain mismatches)
pub fn get_grain_analysis_prompt() -> &'static str {
    r#"Analyze grain levels and aggregation requirements.

TABLES: [Table names with grains]
TARGET_GRAIN: [Desired grain level]
METRIC: [Metric being calculated]

ANALYSIS REQUIRED:
1. Identify grain mismatches
2. Determine aggregation needs (which tables, which columns)
3. Identify join explosion risks
4. Recommend aggregation strategy

OUTPUT: JSON with analysis, recommendations, risks"#
}

/// Get the column identification prompt (semantic column finding)
pub fn get_column_identification_prompt() -> &'static str {
    r#"Identify columns by semantic meaning, not just name.

SEARCH_TERM: [What you're looking for]
TABLES: [Available tables]
CONTEXT: [What you're trying to do]

APPROACH:
1. Match by name similarity
2. Match by semantic meaning (description, type)
3. Consider context (date columns for as-of, numeric for metrics)
4. Return best matches with confidence

OUTPUT: JSON with matches array: table, column, confidence, reasoning"#
}

/// Get the autonomous reasoning prompt (for self-directed exploration)
pub fn get_autonomous_reasoning_prompt() -> &'static str {
    r#"You are an autonomous data analyst. Reason independently about data structures.

CURRENT_STATE: [What you know]
PROBLEM: [What needs solving]

AUTONOMOUS CAPABILITIES:
1. Infer grain requirements from problem context
2. Identify aggregation needs without explicit instructions
3. Detect grain mismatches and propose solutions
4. Reason about join strategies
5. Understand data dependencies

REASONING APPROACH:
- Analyze problem → What grain level is needed?
- Explore tables → What grains exist?
- Compare → Where are mismatches?
- Propose → How to resolve (aggregation, joins)?
- Execute → Implement solution

ASK QUESTIONS ONLY WHEN:
- Multiple valid interpretations exist
- Business logic is truly ambiguous
- Cannot infer from graph/knowledge base

OUTPUT: Reasoning chain with evidence, then proposed actions."#
}

/// Determine if a problem requires deep reasoning (non-direct task)
/// Returns true if the problem lacks explicit instructions about:
/// - Which columns to use
/// - How to aggregate
/// - What grain level is needed
/// - Which tables to join
/// - How to join tables
pub fn requires_deep_reasoning(problem: &str) -> bool {
    let problem_lower = problem.to_lowercase();
    
    // Direct indicators that deep reasoning is needed:
    // 1. Mentions grain level but not specific columns/tables
    let mentions_grain = problem_lower.contains("customer-level") || 
                         problem_lower.contains("loan-level") ||
                         problem_lower.contains("account-level") ||
                         problem_lower.contains("grain");
    
    // 2. Mentions aggregation concept but not method
    // Check for aggregation keywords, but not if they're part of column names
    let mentions_aggregation = problem_lower.contains("aggregate") ||
                               (problem_lower.contains(" sum") || problem_lower.contains("sum ")) ||
                               (problem_lower.contains(" count") || problem_lower.contains("count ")) ||
                               (problem_lower.contains(" total") && !problem_lower.contains("total_outstanding") && !problem_lower.contains("total_")) ||
                               problem_lower.contains("group by") ||
                               problem_lower.contains("groupby");
    
    // 3. Vague column references ("loan identifier" vs "loan_id")
    // Only trigger if vague AND not part of explicit column names
    let has_explicit_column_names = problem_lower.contains("loan_id") ||
                                   problem_lower.contains("total_outstanding") ||
                                   problem_lower.contains("customer_id");
    
    let vague_columns = !has_explicit_column_names && (
                        problem_lower.contains("identifier") ||
                        (problem_lower.contains("amount") && !problem_lower.contains("_amount")) ||
                        (problem_lower.contains("balance") && !problem_lower.contains("_balance")) ||
                        (problem_lower.contains("date") && !problem_lower.contains("_date")));
    
    // 4. Comparison or reconciliation without explicit steps
    let comparison_task = problem_lower.contains("compare") ||
                         problem_lower.contains("reconcile") ||
                         problem_lower.contains("difference") ||
                         problem_lower.contains("discrepancy");
    
    // 5. Multiple tables mentioned but no join strategy
    // Check for explicit "multiple tables" or "combine" with tables
    let multiple_tables = (problem_lower.contains("multiple") && problem_lower.contains("table")) ||
                          (problem_lower.contains("combine") && (problem_lower.contains("table") || problem_lower.contains("with"))) ||
                          (problem_lower.contains("join") && problem_lower.contains("table") && !problem_lower.contains("join on")) ||
                          (problem_lower.contains("from") && problem_lower.matches("from").count() > 1 && problem_lower.contains("calculate")) ||
                          (problem_lower.contains("from multiple") || problem_lower.contains("from several"));
    
    // 6. Questions about "how" or "what" (indicating need for reasoning)
    let how_what_questions = problem_lower.contains("how do i") ||
                            problem_lower.contains("how to") ||
                            problem_lower.contains("what is") ||
                            problem_lower.contains("what should") ||
                            problem_lower.contains("what columns") ||
                            problem_lower.contains("which columns") ||
                            (problem_lower.contains("which") && problem_lower.contains("column")) ||
                            (problem_lower.contains("what") && problem_lower.contains("column"));
    
    // 7. Explicit table/column names (but check if they're in a SELECT statement)
    let has_explicit_tables = problem_lower.contains("loan_summary") ||
                             problem_lower.contains("daily_interest") ||
                             problem_lower.contains("emi_schedule");
    
    // 8. Explicit SQL query (very direct, doesn't need reasoning)
    let is_explicit_sql = problem_lower.starts_with("select") ||
                         problem_lower.contains("select ") && 
                         (problem_lower.contains("from ") || problem_lower.contains("where "));
    
    // Deep reasoning needed if:
    // - Mentions grain/aggregation concepts OR
    // - Has vague column references OR
    // - Is a comparison task OR
    // - Mentions multiple tables without join strategy OR
    // - Has "how/what" questions
    // BUT NOT if it's explicit SQL with table names
    let needs_reasoning = mentions_grain || 
                         mentions_aggregation || 
                         vague_columns || 
                         comparison_task ||
                         multiple_tables ||
                         how_what_questions;
    
    // Don't require deep reasoning if:
    // - It's explicit SQL (SELECT ... FROM ... WHERE ...)
    // - OR it's a simple query with explicit table/column names (not combining/aggregating/comparing)
    // Simple query = "query [table] and get [column]" pattern with explicit names
    let is_simple_query = (problem_lower.contains("query") || problem_lower.contains("get")) && 
                         has_explicit_tables && 
                         !multiple_tables && 
                         !mentions_aggregation && 
                         !how_what_questions &&
                         !comparison_task &&
                         !problem_lower.contains("combine") &&
                         !problem_lower.contains("from multiple") &&
                         !problem_lower.contains("calculate") &&
                         !problem_lower.contains("how");
    
    let is_direct = is_explicit_sql || is_simple_query;
    
    needs_reasoning && !is_direct
}

