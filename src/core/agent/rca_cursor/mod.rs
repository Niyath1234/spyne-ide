//! RcaCursor Module
//! 
//! Grain-driven, agentic, deterministic RCA Cursor implementation.
//! 
//! This module contains the core components for the new RcaCursor architecture:
//! - Entity graph for understanding relationships
//! - Grain resolution for finding paths to target grains
//! - Validation, planning, execution, and attribution

pub mod entity_graph;
pub mod grain_resolver;
pub mod validator;
pub mod logical_plan;
pub mod planner;
pub mod executor;
pub mod diff;
pub mod attribution;
pub mod confidence;
pub mod cursor;

// Old cursor implementation (for backward compatibility during migration)
pub mod cursor_old;

// Re-export key types
pub use entity_graph::{EntityGraph, EntityRelationship, JoinStep};
pub use grain_resolver::{GrainResolver, GrainPlan};
pub use validator::{TaskValidator, ValidatedTask, RcaTask, Filter, TimeRange, ExecutionMode};
pub use logical_plan::LogicalPlanBuilder;
pub use planner::{ExecutionPlanner, ExecutionPlan, ExecutionNode, StopConditions};
pub use executor::{ExecutionEngine, ExecutionResult, ExecutionMetadata};
pub use diff::{GrainDiffEngine, GrainDiffResult};
pub use attribution::GrainAttributionEngine;
pub use confidence::ConfidenceModel;
pub use cursor::RcaCursor;
pub use cursor_old::{RcaCursorResult, RcaSummary};

