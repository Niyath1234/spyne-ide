//! Columnar Writer - Write data in columnar format
//! Stub implementation for RCA-ENGINE

use anyhow::Result;
use serde_json::Value;

/// Columnar Writer
pub struct ColumnarWriter;

impl ColumnarWriter {
    pub fn new() -> Self {
        Self
    }
    
    pub fn write(&self, _table_name: &str, _payloads: &[Value]) -> Result<()> {
        // Stub implementation - RCA-ENGINE uses Polars for data handling
        Ok(())
    }
}

impl Default for ColumnarWriter {
    fn default() -> Self {
        Self::new()
    }
}

