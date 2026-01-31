//! Lineage Tracing Module
//! 
//! Tracks the execution path for each row through joins, filters, and rules.
//! This enables detailed root cause attribution.

pub mod join_trace;
pub mod filter_trace;
pub mod rule_trace;

pub use join_trace::{JoinTracer, JoinTrace, JoinTraceCollection};
pub use filter_trace::{FilterTracer, FilterDecision, FilterTraceCollection};
pub use rule_trace::{RuleTracer, RuleExecution, RuleTraceCollection};





