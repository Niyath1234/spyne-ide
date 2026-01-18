//! Database connection management using sqlx

use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::OnceLock;
use std::time::Duration;

pub type DbPool = PgPool;

static DB_POOL: OnceLock<PgPool> = OnceLock::new();

/// Initialize the database connection pool
pub async fn init_pool(database_url: &str) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(30))
        .connect(database_url)
        .await?;
    
    // Test the connection
    sqlx::query("SELECT 1")
        .execute(&pool)
        .await?;
    
    Ok(pool)
}

/// Get the global database pool (must be initialized first)
pub fn get_pool() -> Option<&'static PgPool> {
    DB_POOL.get()
}

/// Set the global database pool
pub fn set_pool(pool: PgPool) -> Result<(), PgPool> {
    DB_POOL.set(pool)
}

