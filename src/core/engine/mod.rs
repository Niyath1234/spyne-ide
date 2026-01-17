//! Core Engine Module
//! 
//! Contains the row materialization and canonicalization engines.

pub mod materialize;
pub mod canonicalize;
pub mod row_diff;
pub mod aggregate_reconcile;
pub mod logical_plan;
pub mod storage;

pub use materialize::RowMaterializationEngine;
pub use canonicalize::CanonicalMapper;
pub use row_diff::{RowDiffEngine, RowDiffResult, DiffSummary};
pub use aggregate_reconcile::{AggregateReconciliationEngine, AggregateReconciliation, ReconciliationBreakdown};

