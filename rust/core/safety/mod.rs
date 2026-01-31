//! Safety & Guardrails Module
//! 
//! Provides safety mechanisms for RCA execution:
//! - Resource limits and enforcement
//! - Failure recovery and retry logic

pub mod resource_limits;
pub mod failure_recovery;

pub use resource_limits::{ResourceLimits, ResourceUsage, ResourceEnforcer};
pub use failure_recovery::{RetryPolicy, FailureRecovery};





