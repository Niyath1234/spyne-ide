//! Parquet Conversion Cache
//! 
//! Converts CSV files to Parquet format for faster subsequent reads.
//! Caches converted files and checks modification times for invalidation.

use crate::error::{RcaError, Result};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::SystemTime;

/// Parquet cache manager
pub struct ParquetCache {
    /// Cache directory (default: same directory as source file)
    cache_dir: Option<PathBuf>,
    /// Maximum cache size in bytes (default: 10GB)
    max_cache_size_bytes: u64,
}

impl ParquetCache {
    /// Create a new Parquet cache with default settings
    pub fn new() -> Self {
        Self {
            cache_dir: None,
            max_cache_size_bytes: 10 * 1024 * 1024 * 1024, // 10GB
        }
    }
    
    /// Create with custom cache directory
    pub fn with_cache_dir(mut self, cache_dir: PathBuf) -> Self {
        self.cache_dir = Some(cache_dir);
        self
    }
    
    /// Create with custom max cache size
    pub fn with_max_cache_size(mut self, max_bytes: u64) -> Self {
        self.max_cache_size_bytes = max_bytes;
        self
    }
    
    /// Get or create Parquet file for a CSV file
    /// 
    /// Returns the path to the Parquet file (either cached or newly created).
    /// If CSV is newer than Parquet, regenerates the Parquet file.
    pub fn get_or_create_parquet(&self, csv_path: &Path) -> Result<PathBuf> {
        // Determine Parquet path
        let parquet_path = self.get_parquet_path(csv_path)?;
        
        // Check if CSV file exists
        if !csv_path.exists() {
            return Err(RcaError::Execution(format!(
                "CSV file does not exist: {:?}",
                csv_path
            )));
        }
        
        // Check if Parquet file exists and is valid
        let should_regenerate = if parquet_path.exists() {
            // Check if CSV is newer than Parquet
            let csv_mtime = self.get_file_mtime(csv_path)?;
            let parquet_mtime = self.get_file_mtime(&parquet_path)?;
            
            csv_mtime > parquet_mtime
        } else {
            // Parquet doesn't exist, need to create it
            true
        };
        
        if should_regenerate {
            // Convert CSV to Parquet
            self.convert_csv_to_parquet(csv_path, &parquet_path)?;
        }
        
        Ok(parquet_path)
    }
    
    /// Get Parquet path for a CSV file
    fn get_parquet_path(&self, csv_path: &Path) -> Result<PathBuf> {
        let cache_dir = match &self.cache_dir {
            Some(dir) => dir.clone(),
            None => {
                // Use same directory as CSV file
                csv_path.parent()
                    .ok_or_else(|| RcaError::Execution(format!(
                        "Cannot determine parent directory for: {:?}",
                        csv_path
                    )))?
                    .to_path_buf()
            }
        };
        
        // Ensure cache directory exists
        fs::create_dir_all(&cache_dir)
            .map_err(|e| RcaError::Execution(format!(
                "Failed to create cache directory {:?}: {}",
                cache_dir, e
            )))?;
        
        // Generate Parquet filename from CSV filename
        let csv_filename = csv_path.file_name()
            .ok_or_else(|| RcaError::Execution(format!(
                "Cannot get filename from path: {:?}",
                csv_path
            )))?
            .to_string_lossy();
        
        let parquet_filename = if csv_filename.ends_with(".csv") {
            csv_filename.replace(".csv", ".parquet")
        } else {
            format!("{}.parquet", csv_filename)
        };
        
        Ok(cache_dir.join(parquet_filename))
    }
    
    /// Convert CSV file to Parquet format
    fn convert_csv_to_parquet(&self, csv_path: &Path, parquet_path: &Path) -> Result<()> {
        // Read CSV
        let df = LazyCsvReader::new(csv_path)
            .with_infer_schema_length(Some(10000))
            .finish()
            .map_err(|e| RcaError::Execution(format!(
                "Failed to read CSV file {:?}: {}",
                csv_path, e
            )))?
            .collect()
            .map_err(|e| RcaError::Execution(format!(
                "Failed to collect CSV data: {}",
                e
            )))?;
        
        // Write Parquet
        let parquet_dir = parquet_path.parent()
            .ok_or_else(|| RcaError::Execution(format!(
                "Cannot determine parent directory for: {:?}",
                parquet_path
            )))?;
        
        // Ensure directory exists
        fs::create_dir_all(parquet_dir)
            .map_err(|e| RcaError::Execution(format!(
                "Failed to create directory {:?}: {}",
                parquet_dir, e
            )))?;
        
        // Write Parquet file
        let mut file = fs::File::create(parquet_path)
            .map_err(|e| RcaError::Execution(format!(
                "Failed to create Parquet file {:?}: {}",
                parquet_path, e
            )))?;
        
        ParquetWriter::new(&mut file)
            .finish(&mut df.clone())
            .map_err(|e| RcaError::Execution(format!(
                "Failed to write Parquet file {:?}: {}",
                parquet_path, e
            )))?;
        
        Ok(())
    }
    
    /// Get file modification time
    fn get_file_mtime(&self, path: &Path) -> Result<SystemTime> {
        let metadata = fs::metadata(path)
            .map_err(|e| RcaError::Execution(format!(
                "Failed to get metadata for {:?}: {}",
                path, e
            )))?;
        
        metadata.modified()
            .map_err(|e| RcaError::Execution(format!(
                "Failed to get modification time for {:?}: {}",
                path, e
            )))
    }
    
    /// Check if a file should use Parquet cache
    /// 
    /// Returns true if:
    /// - File is CSV
    /// - Parquet cache exists and is valid
    pub fn should_use_parquet(&self, csv_path: &Path) -> Result<bool> {
        // Only cache CSV files
        if csv_path.extension().and_then(|e| e.to_str()) != Some("csv") {
            return Ok(false);
        }
        
        let parquet_path = self.get_parquet_path(csv_path)?;
        
        if !parquet_path.exists() {
            return Ok(false);
        }
        
        // Check if Parquet is newer or same age as CSV
        let csv_mtime = self.get_file_mtime(csv_path)?;
        let parquet_mtime = self.get_file_mtime(&parquet_path)?;
        
        Ok(parquet_mtime >= csv_mtime)
    }
    
    /// Get cached Parquet path if it exists and is valid
    pub fn get_cached_parquet(&self, csv_path: &Path) -> Result<Option<PathBuf>> {
        if self.should_use_parquet(csv_path)? {
            Ok(Some(self.get_parquet_path(csv_path)?))
        } else {
            Ok(None)
        }
    }
    
    /// Invalidate cache for a CSV file (delete corresponding Parquet file)
    pub fn invalidate(&self, csv_path: &Path) -> Result<()> {
        let parquet_path = self.get_parquet_path(csv_path)?;
        
        if parquet_path.exists() {
            fs::remove_file(&parquet_path)
                .map_err(|e| RcaError::Execution(format!(
                    "Failed to delete cached Parquet file {:?}: {}",
                    parquet_path, e
                )))?;
        }
        
        Ok(())
    }
    
    /// Clear entire cache directory
    pub fn clear_cache(&self) -> Result<()> {
        let cache_dir = match &self.cache_dir {
            Some(dir) => dir.clone(),
            None => {
                // Can't clear cache if no specific directory
                return Err(RcaError::Execution(
                    "Cannot clear cache: no cache directory specified".to_string()
                ));
            }
        };
        
        if cache_dir.exists() {
            fs::remove_dir_all(&cache_dir)
                .map_err(|e| RcaError::Execution(format!(
                    "Failed to clear cache directory {:?}: {}",
                    cache_dir, e
                )))?;
            
            // Recreate empty directory
            fs::create_dir_all(&cache_dir)
                .map_err(|e| RcaError::Execution(format!(
                    "Failed to recreate cache directory {:?}: {}",
                    cache_dir, e
                )))?;
        }
        
        Ok(())
    }
}

impl Default for ParquetCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parquet_path_generation() {
        let cache = ParquetCache::new();
        let csv_path = PathBuf::from("data/test.csv");
        
        let parquet_path = cache.get_parquet_path(&csv_path).unwrap();
        assert_eq!(parquet_path, PathBuf::from("data/test.parquet"));
    }
    
    #[test]
    fn test_parquet_path_with_custom_dir() {
        let cache_dir = PathBuf::from("/tmp/test_cache");
        let cache = ParquetCache::new()
            .with_cache_dir(cache_dir.clone());
        
        let csv_path = PathBuf::from("data/test.csv");
        let parquet_path = cache.get_parquet_path(&csv_path).unwrap();
        
        assert!(parquet_path.starts_with(&cache_dir));
        assert!(parquet_path.to_string_lossy().ends_with("test.parquet"));
    }
}

