//! Storage Abstraction Layer
//! 
//! Provides abstraction over different data sources (CSV, Parquet, Delta, Postgres, S3).
//! Automatically uses Parquet cache for CSV files when available.

use crate::error::{RcaError, Result};
use crate::core::performance::ParquetCache;
use polars::prelude::*;
use std::path::PathBuf;

/// Data source types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataSource {
    CSV(PathBuf),
    Parquet(PathBuf),
    Delta(PathBuf), // Future: Delta Lake support
    Postgres(String), // Future: Connection string
    S3(String), // Future: S3 path
}

impl DataSource {
    /// Detect data source type from path
    pub fn from_path(path: &PathBuf) -> Self {
        match path.extension().and_then(|e| e.to_str()) {
            Some("csv") => DataSource::CSV(path.clone()),
            Some("parquet") => DataSource::Parquet(path.clone()),
            _ => DataSource::CSV(path.clone()), // Default to CSV
        }
    }
    
    /// Get the path (for file-based sources)
    pub fn path(&self) -> Option<&PathBuf> {
        match self {
            DataSource::CSV(p) | DataSource::Parquet(p) | DataSource::Delta(p) => Some(p),
            _ => None,
        }
    }
}

/// Table reader trait
/// 
/// Abstracts reading from different data sources
pub trait TableReader {
    /// Read a table into a DataFrame
    fn read(&self, filters: Option<&[crate::core::engine::logical_plan::FilterExpr]>) -> Result<DataFrame>;
    
    /// Read a table as a LazyFrame (for streaming/chunked reading)
    fn read_lazy(&self, filters: Option<&[crate::core::engine::logical_plan::FilterExpr]>) -> Result<LazyFrame>;
    
    /// Get estimated row count (if available)
    fn estimated_row_count(&self) -> Option<usize>;
    
    /// Get file size in bytes (if applicable)
    fn file_size_bytes(&self) -> Option<u64>;
}

/// CSV table reader
pub struct CsvTableReader {
    path: PathBuf,
    infer_schema_length: Option<usize>,
    try_parse_dates: bool,
    parquet_cache: Option<ParquetCache>,
}

impl CsvTableReader {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            infer_schema_length: Some(10000),
            try_parse_dates: false,
            parquet_cache: Some(ParquetCache::new()),
        }
    }
    
    pub fn with_infer_schema_length(mut self, length: Option<usize>) -> Self {
        self.infer_schema_length = length;
        self
    }
    
    pub fn with_try_parse_dates(mut self, try_parse: bool) -> Self {
        self.try_parse_dates = try_parse;
        self
    }
    
    pub fn with_parquet_cache(mut self, cache: Option<ParquetCache>) -> Self {
        self.parquet_cache = cache;
        self
    }
    
    /// Get the actual file path to read (may be Parquet if cached)
    fn get_read_path(&self) -> Result<PathBuf> {
        // Check if we should use Parquet cache
        if let Some(ref cache) = self.parquet_cache {
            if let Ok(Some(parquet_path)) = cache.get_cached_parquet(&self.path) {
                return Ok(parquet_path);
            }
            
            // Try to get or create Parquet cache
            if let Ok(parquet_path) = cache.get_or_create_parquet(&self.path) {
                return Ok(parquet_path);
            }
        }
        
        // Fall back to CSV
        Ok(self.path.clone())
    }
}

impl TableReader for CsvTableReader {
    fn read(&self, filters: Option<&[crate::core::engine::logical_plan::FilterExpr]>) -> Result<DataFrame> {
        let read_path = self.get_read_path()?;
        
        // Check if we're reading Parquet (from cache) or CSV
        let mut lazy_df = if read_path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            // Read from Parquet cache
            LazyFrame::scan_parquet(&read_path, ScanArgsParquet::default())
                .map_err(|e| RcaError::Execution(format!("Failed to scan cached Parquet: {}", e)))?
        } else {
            // Read from CSV
            let mut reader = LazyCsvReader::new(&read_path)
                .with_try_parse_dates(self.try_parse_dates)
                .with_infer_schema_length(self.infer_schema_length);
            
            reader.finish()
                .map_err(|e| RcaError::Execution(format!("Failed to scan CSV: {}", e)))?
        };
        
        // Apply filters if provided (filter pushdown)
        if let Some(filters) = filters {
            for filter in filters {
                let expr = filter.to_polars_expr()?;
                lazy_df = lazy_df.filter(expr);
            }
        }
        
        lazy_df.collect()
            .map_err(|e| RcaError::Execution(format!("Failed to collect data: {}", e)))
    }
    
    fn read_lazy(&self, filters: Option<&[crate::core::engine::logical_plan::FilterExpr]>) -> Result<LazyFrame> {
        let read_path = self.get_read_path()?;
        
        // Check if we're reading Parquet (from cache) or CSV
        let mut lazy_df = if read_path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            // Read from Parquet cache
            LazyFrame::scan_parquet(&read_path, ScanArgsParquet::default())
                .map_err(|e| RcaError::Execution(format!("Failed to scan cached Parquet: {}", e)))?
        } else {
            // Read from CSV
            let mut reader = LazyCsvReader::new(&read_path)
                .with_try_parse_dates(self.try_parse_dates)
                .with_infer_schema_length(self.infer_schema_length);
            
            reader.finish()
                .map_err(|e| RcaError::Execution(format!("Failed to scan CSV: {}", e)))?
        };
        
        // Apply filters if provided (filter pushdown)
        if let Some(filters) = filters {
            for filter in filters {
                let expr = filter.to_polars_expr()?;
                lazy_df = lazy_df.filter(expr);
            }
        }
        
        Ok(lazy_df)
    }
    
    fn estimated_row_count(&self) -> Option<usize> {
        // Could estimate from file size, but for now return None
        None
    }
    
    fn file_size_bytes(&self) -> Option<u64> {
        std::fs::metadata(&self.path).ok().map(|m| m.len())
    }
}

/// Parquet table reader
pub struct ParquetTableReader {
    path: PathBuf,
}

impl ParquetTableReader {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl TableReader for ParquetTableReader {
    fn read(&self, filters: Option<&[crate::core::engine::logical_plan::FilterExpr]>) -> Result<DataFrame> {
        let mut lazy_df = LazyFrame::scan_parquet(&self.path, ScanArgsParquet::default())
            .map_err(|e| RcaError::Execution(format!("Failed to scan Parquet: {}", e)))?;
        
        // Apply filters if provided (filter pushdown)
        if let Some(filters) = filters {
            for filter in filters {
                let expr = filter.to_polars_expr()?;
                lazy_df = lazy_df.filter(expr);
            }
        }
        
        lazy_df.collect()
            .map_err(|e| RcaError::Execution(format!("Failed to collect Parquet: {}", e)))
    }
    
    fn read_lazy(&self, filters: Option<&[crate::core::engine::logical_plan::FilterExpr]>) -> Result<LazyFrame> {
        let mut lazy_df = LazyFrame::scan_parquet(&self.path, ScanArgsParquet::default())
            .map_err(|e| RcaError::Execution(format!("Failed to scan Parquet: {}", e)))?;
        
        if let Some(filters) = filters {
            for filter in filters {
                let expr = filter.to_polars_expr()?;
                lazy_df = lazy_df.filter(expr);
            }
        }
        
        Ok(lazy_df)
    }
    
    fn estimated_row_count(&self) -> Option<usize> {
        // Parquet files have metadata with row counts
        // For now, return None - could be enhanced to read Parquet metadata
        None
    }
    
    fn file_size_bytes(&self) -> Option<u64> {
        std::fs::metadata(&self.path).ok().map(|m| m.len())
    }
}

/// Create a table reader from a data source
pub fn create_table_reader(source: &DataSource) -> Result<Box<dyn TableReader>> {
    match source {
        DataSource::CSV(path) => {
            Ok(Box::new(CsvTableReader::new(path.clone())))
        }
        DataSource::Parquet(path) => {
            Ok(Box::new(ParquetTableReader::new(path.clone())))
        }
        DataSource::Delta(_) => {
            Err(RcaError::Execution("Delta Lake support not yet implemented".to_string()))
        }
        DataSource::Postgres(_) => {
            Err(RcaError::Execution("PostgreSQL support not yet implemented".to_string()))
        }
        DataSource::S3(_) => {
            Err(RcaError::Execution("S3 support not yet implemented".to_string()))
        }
    }
}

