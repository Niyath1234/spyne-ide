// RCA-ENGINE Library
// Root Cause Analysis Engine for Data Reconciliation

pub mod ambiguity;
pub mod diff;
pub mod drilldown;
pub mod error;
pub mod explain;
pub mod graph;
pub mod identity;
pub mod llm;
pub mod metadata;
pub mod operators;
pub mod rca;
pub mod rule_compiler;
pub mod time;

pub use error::{RcaError, Result};
pub use rca::{RcaEngine, RcaResult};

