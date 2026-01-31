//! Database module for PostgreSQL connection and operations
//! 
//! This module provides database connectivity and CRUD operations for RCA Engine metadata

pub mod connection;
pub mod metadata_repo;
pub mod query_history;

pub use connection::{get_pool, init_pool, DbPool};
pub use metadata_repo::MetadataRepository;

