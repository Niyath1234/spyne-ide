//! Modular Components - Direct WorldState Integration
//! 
//! This module provides modular, reusable components that directly connect to WorldState,
//! implementing ingestion pipeline and contract registry functionality.

pub mod contract_registry;

pub use contract_registry::ContractRegistryModule;

