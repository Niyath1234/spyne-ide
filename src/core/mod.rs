//! Core RCA Engine Modules
//! 
//! This module contains the foundational components for row-level RCA:
//! - Canonical entity models
//! - Metric normalization
//! - Row materialization
//! - Lineage tracing
//! - Root cause attribution

pub mod models;
pub mod metrics;
pub mod engine;
pub mod lineage;
pub mod rca;
pub mod agent;
pub mod llm;
pub mod performance;
pub mod trust;
pub mod observability;
pub mod safety;
pub mod identity;

