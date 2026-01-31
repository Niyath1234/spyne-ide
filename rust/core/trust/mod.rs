//! Trust Layer
//! 
//! Provides trust and verification capabilities:
//! - Evidence storage for auditability
//! - Deterministic replay for reproducibility
//! - Aggregate proof verification

pub mod evidence;
pub mod replay;
pub mod verification;

pub use evidence::{EvidenceStore, EvidenceRecord, ExecutionInputs, ExecutionOutputs, OutputSummary};
pub use replay::{ReplayEngine, ReplayConfig};
pub use verification::{VerificationEngine, VerificationResult};

