pub mod agentic_prompts;
pub mod agentic_reasoner;
pub mod ambiguity;
pub mod data_engineering;
pub mod data_utils;
pub mod de_executor;
pub mod drilldown;
pub mod error;
pub mod explain;
pub mod fuzzy_matcher;
pub mod faiss_fuzzy_matcher;
pub mod llm_value_matcher;
pub mod diff;
pub mod tool_system;
pub mod grain_resolver;
pub mod granularity_understanding;
pub mod graph;
pub mod graph_adapter;
pub mod knowledge_hints;
pub mod identity;
pub mod intelligent_rule_builder;
pub mod intelligent_rule_parser;
pub mod intent_compiler;
pub mod intent_validator;
pub mod join_inference;
pub mod llm;
pub mod metadata;
pub mod metric_similarity;
pub mod operators;
pub mod rca;
pub mod rule_compiler;
pub mod task_grounder;
pub mod execution_planner;
pub mod execution_engine;
pub mod execution;
pub mod goal_directed_explorer;
pub mod one_shot_runner;
pub mod safety_guardrails;
pub mod explainability;
pub mod rule_reasoner;
pub mod time;
pub mod validation;
pub mod hybrid_reasoner;
pub mod ingestion;
pub mod modules;
pub mod core;
pub mod sql_engine;
pub mod sql_compiler;
pub mod graph_traversal;
pub mod agent_prompts;
pub mod table_upload;
pub mod simplified_intent;
pub mod simplified_api;
pub mod semantic_column_resolver;
pub mod agent;
pub mod node_registry;
pub mod query_engine;
pub mod data_assistant;
pub mod learning_store;
pub mod optimized_search;
pub mod semantic;
pub mod intent;
pub mod schema_rag;
pub mod execution_loop;
pub mod compiler;
pub mod security;
pub mod observability;
pub mod learning;
pub mod semantic_completeness;
// Python bindings - only compile if pyo3 is available
// Commented out until PyO3 is set up
// #[cfg(feature = "python-bindings")]
// pub mod python_bindings;

// Re-export Hypergraph module
#[path = "../Hypergraph/mod.rs"]
pub mod hypergraph;

// Re-export WorldState module
#[path = "../WorldState/mod.rs"]
pub mod world_state;

// Re-export KnowledgeBase module
#[path = "../KnowledgeBase/mod.rs"]
pub mod knowledge_base;

// Database module for PostgreSQL
pub mod db;
