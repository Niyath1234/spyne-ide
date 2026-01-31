//! Chunked Extraction
//! 
//! Extracts data in chunks to avoid loading entire datasets into memory.
//! Useful for large-scale RCA where datasets may not fit in memory.

use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Configuration for chunked extraction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkConfig {
    /// Number of rows per chunk
    pub chunk_size: usize,
    
    /// Maximum number of chunks to process (None = all)
    pub max_chunks: Option<usize>,
    
    /// Whether to parallelize chunk processing
    pub parallel: bool,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            chunk_size: 100_000,
            max_chunks: None,
            parallel: false,
        }
    }
}

/// Chunked data extractor
pub struct ChunkedExtractor {
    config: ChunkConfig,
}

impl ChunkedExtractor {
    /// Create a new chunked extractor
    pub fn new(config: ChunkConfig) -> Self {
        Self { config }
    }
    
    /// Extract data in chunks from a parquet file
    /// 
    /// Returns an iterator over chunks, allowing processing without loading
    /// the entire dataset into memory.
    pub fn extract_chunks(
        &self,
        file_path: &PathBuf,
    ) -> Result<ChunkIterator> {
        // Validate file exists
        if !file_path.exists() {
            return Err(RcaError::Execution(format!(
                "File not found: {}",
                file_path.display()
            )));
        }
        
        // Get total row count for chunking
        let total_rows = self.get_row_count(file_path)?;
        
        Ok(ChunkIterator {
            file_path: file_path.clone(),
            chunk_size: self.config.chunk_size,
            total_rows,
            current_offset: 0,
            max_chunks: self.config.max_chunks,
            chunks_processed: 0,
        })
    }
    
    /// Get total row count from parquet file
    fn get_row_count(&self, file_path: &PathBuf) -> Result<usize> {
        // Use lazy scan to get row count without loading data
        let df = LazyFrame::scan_parquet(file_path, ScanArgsParquet::default())
            .map_err(|e| RcaError::Execution(format!("Failed to scan parquet: {}", e)))?;
        
        // Count rows
        let count_df = df
            .select([col("*").count().alias("count")])
            .collect()
            .map_err(|e| RcaError::Execution(format!("Failed to count rows: {}", e)))?;
        
        if count_df.height() > 0 {
            if let Ok(count_series) = count_df.column("count") {
                if let Ok(count_u32) = count_series.u32() {
                    if let Some(count) = count_u32.get(0) {
                        return Ok(count as usize);
                    }
                }
            }
        }
        
        Ok(0)
    }
    
    /// Process chunks with a callback function
    /// 
    /// Processes each chunk and calls the callback with the chunk data.
    /// This allows streaming processing without storing all chunks.
    pub async fn process_chunks<F>(
        &self,
        file_path: &PathBuf,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(DataFrame) -> Result<()>,
    {
        let mut chunk_iter = self.extract_chunks(file_path)?;
        
        while let Some(chunk) = chunk_iter.next()? {
            callback(chunk)?;
        }
        
        Ok(())
    }
}

/// Iterator over chunks
pub struct ChunkIterator {
    file_path: PathBuf,
    chunk_size: usize,
    total_rows: usize,
    current_offset: usize,
    max_chunks: Option<usize>,
    chunks_processed: usize,
}

impl ChunkIterator {
    /// Get next chunk
    pub fn next(&mut self) -> Result<Option<DataFrame>> {
        // Check if we've reached max chunks
        if let Some(max) = self.max_chunks {
            if self.chunks_processed >= max {
                return Ok(None);
            }
        }
        
        // Check if we've processed all rows
        if self.current_offset >= self.total_rows {
            return Ok(None);
        }
        
        // Calculate chunk range
        let start = self.current_offset;
        let end = (start + self.chunk_size).min(self.total_rows);
        
        // Load chunk using row range
        let chunk = LazyFrame::scan_parquet(&self.file_path, ScanArgsParquet::default())
            .map_err(|e| RcaError::Execution(format!("Failed to scan parquet: {}", e)))?
            .slice(start as i64, (end - start) as u32)
            .collect()
            .map_err(|e| RcaError::Execution(format!("Failed to load chunk: {}", e)))?;
        
        // Update state
        self.current_offset = end;
        self.chunks_processed += 1;
        
        Ok(Some(chunk))
    }
    
    /// Get progress information
    pub fn progress(&self) -> (usize, usize) {
        (self.current_offset, self.total_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_chunk_config_default() {
        let config = ChunkConfig::default();
        assert_eq!(config.chunk_size, 100_000);
        assert_eq!(config.max_chunks, None);
        assert_eq!(config.parallel, false);
    }
}

