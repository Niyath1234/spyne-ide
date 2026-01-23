//! Semantic SQL Intent
//! 
//! Intent structure that uses semantic metrics and dimensions instead of raw SQL.
//! 
//! ## Key Design Principle
//! 
//! **LLMs decide *what the user means* (intent).
//! Compilers decide *how the database must behave* (join mechanics).**
//! 
//! The LLM only outputs dimension usage (Filter vs Select), never join types.
//! The compiler deterministically derives join types from intent + metadata.

use crate::error::Result;
use serde::{Deserialize, Serialize};

/// Time grain for metrics (re-exported for convenience)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeGrain {
    /// Real-time or no time grain
    None,
    /// Daily aggregation
    Day,
    /// Weekly aggregation
    Week,
    /// Monthly aggregation
    Month,
    /// Quarterly aggregation
    Quarter,
    /// Yearly aggregation
    Year,
}

impl TimeGrain {
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeGrain::None => "none",
            TimeGrain::Day => "day",
            TimeGrain::Week => "week",
            TimeGrain::Month => "month",
            TimeGrain::Quarter => "quarter",
            TimeGrain::Year => "year",
        }
    }
}

/// How a dimension is being used in the query
/// 
/// This is the ONLY semantic decision the LLM makes about dimensions.
/// The compiler uses this + metadata to deterministically choose join types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DimensionUsage {
    /// Dimension is used to augment/describe the result (appears in SELECT/GROUP BY)
    /// Example: "Show revenue by region" → region is Select
    Select,
    /// Dimension is used to filter/restrict the result (appears in WHERE)
    /// Example: "Show revenue for VIP customers" → customer_category is Filter
    Filter,
    /// Dimension is used both for filtering AND selection
    /// Example: "Show revenue by region for VIP customers" → region is Both, customer_category is Filter
    Both,
}

impl DimensionUsage {
    pub fn as_str(&self) -> &'static str {
        match self {
            DimensionUsage::Select => "select",
            DimensionUsage::Filter => "filter",
            DimensionUsage::Both => "both",
        }
    }
}

/// Dimension intent - how a dimension is being used
/// 
/// This replaces the ambiguous "dimension name" with explicit usage semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionIntent {
    /// Dimension name from semantic registry
    pub name: String,
    /// How this dimension is being used
    pub usage: DimensionUsage,
}

impl DimensionIntent {
    pub fn new(name: String, usage: DimensionUsage) -> Self {
        Self { name, usage }
    }
    
    pub fn select(name: String) -> Self {
        Self::new(name, DimensionUsage::Select)
    }
    
    pub fn filter(name: String) -> Self {
        Self::new(name, DimensionUsage::Filter)
    }
    
    pub fn both(name: String) -> Self {
        Self::new(name, DimensionUsage::Both)
    }
}

/// Semantic filter specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticFilter {
    pub dimension: String,
    pub operator: String, // "=", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "IS NULL", "IS NOT NULL"
    pub value: Option<serde_json::Value>,
    /// Relative date specification for date dimensions (e.g., "2_days_ago", "yesterday", "today")
    /// When specified, the filter will use date arithmetic instead of direct value comparison
    #[serde(default)]
    pub relative_date: Option<String>,
}

/// Time range specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: Option<String>, // ISO date string or relative like "start_of_year"
    pub end: Option<String>,   // ISO date string or relative like "end_of_year"
}

/// Semantic SQL Intent - uses metrics and dimensions instead of raw tables/columns
/// 
/// ## Design Philosophy
/// 
/// This intent structure separates concerns:
/// - **LLM responsibility**: Identify dimension usage (Filter vs Select)
/// - **Compiler responsibility**: Derive join types deterministically from usage + metadata
/// 
/// The LLM NEVER outputs join types. It only outputs dimension usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticSqlIntent {
    /// Metric names from semantic registry
    pub metrics: Vec<String>,
    
    /// Dimension intents - explicit usage semantics
    /// 
    /// If this field is present, use it. Otherwise, fall back to legacy `dimensions` field
    /// for backward compatibility.
    #[serde(default)]
    pub dimension_intents: Option<Vec<DimensionIntent>>,
    
    /// Dimension names from semantic registry (legacy field, for backward compatibility)
    /// 
    /// If `dimension_intents` is present, this is ignored.
    /// If not present, dimensions default to `Select` usage.
    #[serde(default)]
    pub dimensions: Vec<String>,
    
    /// Filters on dimensions
    pub filters: Vec<SemanticFilter>,
    
    /// Time grain for aggregation (optional, can be inferred from metric)
    pub time_grain: Option<TimeGrain>,
    
    /// Time range filter
    pub time_range: Option<TimeRange>,
    
    /// Limit number of rows
    pub limit: Option<u32>,
}

impl SemanticSqlIntent {
    pub fn new(metrics: Vec<String>) -> Self {
        Self {
            metrics,
            dimension_intents: None,
            dimensions: Vec::new(),
            filters: Vec::new(),
            time_grain: None,
            time_range: None,
            limit: None,
        }
    }

    /// Add dimension intents with explicit usage semantics
    pub fn with_dimension_intents(mut self, intents: Vec<DimensionIntent>) -> Self {
        self.dimension_intents = Some(intents);
        self
    }

    /// Add dimensions (legacy method - defaults to Select usage)
    pub fn with_dimensions(mut self, dimensions: Vec<String>) -> Self {
        self.dimensions = dimensions;
        self
    }

    pub fn with_filters(mut self, filters: Vec<SemanticFilter>) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_time_range(mut self, time_range: TimeRange) -> Self {
        self.time_range = Some(time_range);
        self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    
    /// Get dimension intents, converting legacy format if needed
    /// 
    /// This method provides a unified view:
    /// - If dimension_intents is present, use it
    /// - Otherwise, convert dimensions to DimensionIntent with Select usage
    /// - Also check filters to infer Filter usage
    pub fn get_dimension_intents(&self) -> Vec<DimensionIntent> {
        if let Some(ref intents) = self.dimension_intents {
            return intents.clone();
        }
        
        // Legacy: convert dimensions to intents
        // Check if dimension is used in filters to determine usage
        let filter_dimensions: std::collections::HashSet<&str> = self.filters
            .iter()
            .map(|f| f.dimension.as_str())
            .collect();
        
        self.dimensions
            .iter()
            .map(|dim_name| {
                let is_in_filter = filter_dimensions.contains(dim_name.as_str());
                let is_in_select = true; // If in dimensions list, it's selected
                
                let usage = if is_in_filter && is_in_select {
                    DimensionUsage::Both
                } else if is_in_filter {
                    DimensionUsage::Filter
                } else {
                    DimensionUsage::Select
                };
                
                DimensionIntent::new(dim_name.clone(), usage)
            })
            .collect()
    }
}

