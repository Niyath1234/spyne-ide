//! Types module for Hypergraph
//! Contains Value type and ColumnFragment interface needed by the hypergraph module

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Value type used in hypergraph statistics and metadata
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int64(i64),
    Int32(i32),
    Float64(f64),
    Float32(f32),
    String(String),
    Bool(bool),
    Vector(Vec<f32>),  // Dense vector for similarity search
    /// Array of values (homogeneous type)
    Array(Vec<Value>),
    /// Map/Dictionary: key-value pairs (keys are strings for now)
    Map(HashMap<String, Value>),
    /// JSON value (stored as string, parsed on demand)
    Json(String),
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int64(v) => write!(f, "{}", v),
            Value::Int32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Vector(v) => write!(f, "[{}]", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ")),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, val) in arr.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            },
            Value::Map(map) => {
                write!(f, "{{")?;
                let mut first = true;
                let mut entries: Vec<_> = map.iter().collect();
                entries.sort_by_key(|(k, _)| k.clone());
                for (k, v) in entries {
                    if !first { write!(f, ", ")?; }
                    first = false;
                    write!(f, "\"{}\": {}", k, v)?;
                }
                write!(f, "}}")
            },
            Value::Json(json_str) => write!(f, "{}", json_str),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// ColumnFragment interface - minimal interface for hypergraph nodes
/// This is a simplified version that can be implemented by external storage systems
#[derive(Clone, Debug)]
pub struct ColumnFragment {
    /// Fragment metadata
    pub metadata: FragmentMetadata,
}

/// Fragment metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FragmentMetadata {
    /// Number of rows in this fragment
    pub row_count: usize,
    
    /// Minimum value (for sorted fragments)
    pub min_value: Option<Value>,
    
    /// Maximum value (for sorted fragments)
    pub max_value: Option<Value>,
    
    /// Cardinality estimate
    pub cardinality: usize,
    
    /// Memory size in bytes
    pub memory_size: usize,
    
    /// Table name (for bitmap index building)
    #[serde(default)]
    pub table_name: Option<String>,
    
    /// Column name (for bitmap index building)
    #[serde(default)]
    pub column_name: Option<String>,
    
    /// Partition ID (for partition pruning optimizations)
    #[serde(default)]
    pub partition_id: Option<u64>,
    
    /// Additional metadata (key-value pairs)
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Default for FragmentMetadata {
    fn default() -> Self {
        Self {
            row_count: 0,
            min_value: None,
            max_value: None,
            cardinality: 0,
            memory_size: 0,
            table_name: None,
            column_name: None,
            partition_id: None,
            metadata: HashMap::new(),
        }
    }
}

impl ColumnFragment {
    /// Get the length (row count) of this fragment
    pub fn len(&self) -> usize {
        self.metadata.row_count
    }
}

