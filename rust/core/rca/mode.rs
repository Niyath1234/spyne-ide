//! RCA Mode Configuration
//! 
//! Defines progressive RCA modes: Fast → Deep → Forensic
//! Each mode provides different levels of detail and performance characteristics.

use serde::{Deserialize, Serialize};

/// RCA execution mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RCAMode {
    /// Fast Mode (Triage)
    /// - Sampling only (1k-10k rows or top-N)
    /// - No full lineage tracing
    /// - Minimal joins
    /// - Hash-based diff
    /// - Heuristic contribution analysis
    Fast,
    
    /// Deep Mode (Investigation)
    /// - Full row diff
    /// - Limited lineage tracing (joins + filters)
    /// - Chunked processing
    /// - Deterministic diff
    Deep,
    
    /// Forensic Mode (Court-Proof)
    /// - Full lineage tracing
    /// - Rule tracing
    /// - Evidence storage
    /// - Deterministic replay
    Forensic,
}

/// Lineage tracing level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LineageLevel {
    /// No lineage tracing
    None,
    
    /// Trace joins and filters only
    JoinsAndFilters,
    
    /// Full lineage including rules
    Full,
}

/// RCA configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RCAConfig {
    /// Execution mode
    pub mode: RCAMode,
    
    /// Sampling configuration (for Fast mode)
    pub sampling: Option<SamplingConfig>,
    
    /// Lineage tracing level
    pub lineage_level: LineageLevel,
    
    /// Whether to store evidence (for Forensic mode)
    pub store_evidence: bool,
    
    /// Whether to enable deterministic replay
    pub enable_replay: bool,
    
    /// Confidence threshold for escalation (0.0-1.0)
    pub confidence_threshold: f64,
    
    /// Mismatch threshold for escalation
    pub mismatch_threshold: f64,
}

/// Sampling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// Sampling strategy
    pub strategy: RCASamplingStrategy,
    
    /// Sample size (for random sampling)
    pub sample_size: Option<usize>,
    
    /// Top-N count (for top-N sampling)
    pub top_n: Option<usize>,
    
    /// Order by column (for top-N sampling)
    pub order_by: Option<String>,
}

/// Sampling strategy for RCA modes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RCASamplingStrategy {
    /// Random sampling
    Random,
    
    /// Top-N by value
    TopN,
    
    /// Stratified sampling
    Stratified { column: String },
}

impl Default for RCAConfig {
    fn default() -> Self {
        Self {
            mode: RCAMode::Fast,
            sampling: Some(SamplingConfig {
                strategy: RCASamplingStrategy::Random,
                sample_size: Some(10000),
                top_n: None,
                order_by: None,
            }),
            lineage_level: LineageLevel::None,
            store_evidence: false,
            enable_replay: false,
            confidence_threshold: 0.6,
            mismatch_threshold: 1000.0,
        }
    }
}

impl RCAConfig {
    /// Create Fast mode configuration
    pub fn fast() -> Self {
        Self {
            mode: RCAMode::Fast,
            sampling: Some(SamplingConfig {
                strategy: RCASamplingStrategy::Random,
                sample_size: Some(10000),
                top_n: None,
                order_by: None,
            }),
            lineage_level: LineageLevel::None,
            store_evidence: false,
            enable_replay: false,
            confidence_threshold: 0.6,
            mismatch_threshold: 1000.0,
        }
    }
    
    /// Create Deep mode configuration
    pub fn deep() -> Self {
        Self {
            mode: RCAMode::Deep,
            sampling: None, // No sampling in Deep mode
            lineage_level: LineageLevel::JoinsAndFilters,
            store_evidence: false,
            enable_replay: false,
            confidence_threshold: 0.7,
            mismatch_threshold: 100.0,
        }
    }
    
    /// Create Forensic mode configuration
    pub fn forensic() -> Self {
        Self {
            mode: RCAMode::Forensic,
            sampling: None, // No sampling in Forensic mode
            lineage_level: LineageLevel::Full,
            store_evidence: true,
            enable_replay: true,
            confidence_threshold: 0.9,
            mismatch_threshold: 0.0, // Always investigate in Forensic mode
        }
    }
    
    /// Determine if lineage tracing should be enabled
    pub fn should_trace_joins(&self) -> bool {
        matches!(self.lineage_level, LineageLevel::JoinsAndFilters | LineageLevel::Full)
    }
    
    /// Determine if filter tracing should be enabled
    pub fn should_trace_filters(&self) -> bool {
        matches!(self.lineage_level, LineageLevel::JoinsAndFilters | LineageLevel::Full)
    }
    
    /// Determine if rule tracing should be enabled
    pub fn should_trace_rules(&self) -> bool {
        matches!(self.lineage_level, LineageLevel::Full)
    }
    
    /// Determine if sampling should be applied
    pub fn should_sample(&self) -> bool {
        self.sampling.is_some()
    }
    
    /// Determine if hash-based diff should be used
    pub fn use_hash_diff(&self) -> bool {
        matches!(self.mode, RCAMode::Fast)
    }
    
    /// Determine if deterministic diff should be used
    pub fn use_deterministic_diff(&self) -> bool {
        matches!(self.mode, RCAMode::Deep | RCAMode::Forensic)
    }
}

/// Mode selection logic
pub struct ModeSelector;

impl ModeSelector {
    /// Select mode based on user query
    pub fn select_from_query(query: &str) -> RCAMode {
        let query_lower = query.to_lowercase();
        
        // Check for explicit mode requests
        if query_lower.contains("prove") || 
           query_lower.contains("audit") || 
           query_lower.contains("regulator") ||
           query_lower.contains("court") {
            return RCAMode::Forensic;
        }
        
        if query_lower.contains("which rows") || 
           query_lower.contains("exact rows") ||
           query_lower.contains("show me") {
            return RCAMode::Deep;
        }
        
        // Default to Fast mode for general queries
        RCAMode::Fast
    }
    
    /// Determine if escalation is needed based on Fast mode results
    pub fn should_escalate(
        confidence: f64,
        mismatch_magnitude: f64,
        config: &RCAConfig,
    ) -> Option<RCAMode> {
        // Escalate to Deep if confidence is low
        if confidence < config.confidence_threshold {
            return Some(RCAMode::Deep);
        }
        
        // Escalate to Deep if mismatch is large
        if mismatch_magnitude > config.mismatch_threshold {
            return Some(RCAMode::Deep);
        }
        
        None
    }
    
    /// Determine if escalation to Forensic is needed
    pub fn should_escalate_to_forensic(
        deep_explanation_quality: f64,
        user_requested: bool,
    ) -> bool {
        user_requested || deep_explanation_quality < 0.7
    }
}

