//! Execution Module - Hybrid execution layer with pluggable engines
//! 
//! This module implements the execution architecture described in the design document:
//! - ExecutionEngine trait for pluggable engines
//! - QueryProfile for query characterization
//! - ExecutionRouter for intelligent engine selection
//! - Engine implementations (DuckDB, Trino, Polars)

pub mod engine;
pub mod profile;
pub mod result;
pub mod router;

// Engine implementations
pub mod duckdb_engine;
pub mod trino_engine;
pub mod polars_engine;

// Agent decision logic
pub mod agent_decision;

pub use engine::{EngineCapabilities, EngineSelection, EngineSuggestion, ExecutionContext, ExecutionEngine, UserContext};
pub use profile::{DataSource, QueryProfile, SourceType};
pub use result::QueryResult;
pub use router::ExecutionRouter;
pub use agent_decision::{AgentDecision, AgentDecisionContext, AgentDecisionMaker, EnginePreferences, agent_decide_engine, agent_select_engine};

