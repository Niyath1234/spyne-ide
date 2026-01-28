//! Agent Decision Logic - Helps agents decide which execution engine to use
//! 
//! This module provides utilities for agents to make informed decisions about
//! which execution engine to use based on query characteristics and metadata.

use crate::error::{RcaError, Result};
use crate::execution::profile::{DataSource, QueryProfile};
use crate::execution::router::ExecutionRouter;
use crate::execution::engine::{EngineSelection, EngineSuggestion};
use crate::metadata::Metadata;
use serde::{Deserialize, Serialize};
use tracing::{info, debug};

/// Agent decision context for engine selection
#[derive(Clone)]
pub struct AgentDecisionContext {
    /// Query intent or SQL
    pub query: String,
    
    /// Query profile (if already computed)
    pub profile: Option<QueryProfile>,
    
    /// Metadata context (not serialized - contains HashMaps)
    // Note: Metadata cannot be serialized due to HashMap fields
    pub metadata: Option<Metadata>,
    
    /// User preferences (if any)
    pub user_preferences: Option<EnginePreferences>,
    
    /// Available engines
    pub available_engines: Vec<String>,
}

impl std::fmt::Debug for AgentDecisionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentDecisionContext")
            .field("query", &self.query)
            .field("profile", &self.profile)
            .field("metadata", &self.metadata.as_ref().map(|_| "..."))
            .field("user_preferences", &self.user_preferences)
            .field("available_engines", &self.available_engines)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnginePreferences {
    /// Preferred engine (if user specified)
    pub preferred_engine: Option<String>,
    
    /// Maximum execution time in ms
    pub max_execution_time_ms: Option<u64>,
    
    /// Whether to prefer speed over correctness
    pub prefer_speed: bool,
    
    /// Whether to allow preview engines
    pub allow_preview: bool,
}

/// Agent decision result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDecision {
    /// Selected engine
    pub engine: String,
    
    /// Reasoning for selection
    pub reasoning: Vec<String>,
    
    /// Confidence score (0-100)
    pub confidence: u8,
    
    /// Estimated execution time in ms
    pub estimated_time_ms: Option<u64>,
    
    /// Alternative engines considered
    pub alternatives: Vec<EngineAlternative>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineAlternative {
    pub engine: String,
    pub reason_not_selected: String,
}

/// Agent decision maker
pub struct AgentDecisionMaker<'a> {
    router: &'a ExecutionRouter,
}

impl<'a> AgentDecisionMaker<'a> {
    /// Create a new agent decision maker
    pub fn new(router: &'a ExecutionRouter) -> Self {
        Self { router }
    }
    
    /// Make engine selection decision based on context
    /// 
    /// This is the agent's chain of thought for engine selection.
    /// The agent must explicitly reason about which engine to use - no defaults.
    pub fn decide(&self, context: &AgentDecisionContext) -> Result<AgentDecision> {
        info!("Agent making engine selection decision (chain of thought)");
        
        // Step 1: Extract or compute query profile
        let profile = if let Some(ref p) = context.profile {
            p.clone()
        } else {
            // Compute from query
            QueryProfile::from_sql(&context.query)
        };
        
        // Step 2: Enhance profile with metadata if available
        let mut enhanced_profile = profile;
        if let Some(ref metadata) = context.metadata {
            self.enhance_profile_with_metadata(&mut enhanced_profile, metadata)?;
        }
        
        // Step 3: Get engine suggestions from router (for agent reasoning)
        // Include user preferences to get speed-optimized suggestions
        let suggestions = if let Some(ref prefs) = context.user_preferences {
            self.router.suggest_engine_with_preferences(&enhanced_profile, Some(prefs))?
        } else {
            self.router.suggest_engine(&enhanced_profile)?
        };
        
        // Step 4: Agent reasons about which engine to select
        // This is where the agent's chain of thought happens
        let (selected_engine, reasoning) = self.agent_reason_about_engine(
            &enhanced_profile,
            &suggestions,
            context.user_preferences.as_ref(),
        )?;
        
        // Step 5: Create engine selection (agent's explicit decision)
        let selection = EngineSelection {
            engine_name: selected_engine.clone(),
            reasoning: reasoning.clone(),
            fallback_available: self.router.has_fallback(&selected_engine),
        };
        
        // Step 6: Build decision with full reasoning chain
        // Calculate confidence based on profile completeness
        let confidence = self.calculate_confidence(&enhanced_profile);
        
        // Estimate execution time (heuristic)
        let estimated_time = self.estimate_execution_time(&enhanced_profile, &selection.engine_name);
        
        // Build alternatives (other engines considered)
        let alternatives = self.build_alternatives_from_suggestions(&suggestions, &selected_engine);
        
        Ok(AgentDecision {
            engine: selection.engine_name,
            reasoning, // Full chain of thought reasoning
            confidence,
            estimated_time_ms: estimated_time,
            alternatives,
        })
    }
    
    /// Agent's chain of thought reasoning about which engine to select
    /// 
    /// This is where the agent explicitly reasons about engine selection.
    /// Returns (engine_name, reasoning_chain)
    fn agent_reason_about_engine(
        &self,
        profile: &QueryProfile,
        suggestions: &[EngineSuggestion],
        user_preferences: Option<&EnginePreferences>,
    ) -> Result<(String, Vec<String>)> {
        let mut reasoning = Vec::new();
        
        // Start chain of thought
        reasoning.push("Analyzing query characteristics...".to_string());
        
        // Check user preferences first
        if let Some(prefs) = user_preferences {
            if let Some(ref preferred) = prefs.preferred_engine {
                reasoning.push(format!("User preference: {}", preferred));
                if suggestions.iter().any(|s| s.engine == *preferred && s.can_handle) {
                    return Ok((preferred.clone(), reasoning));
                } else {
                    reasoning.push(format!("Warning: User preferred engine '{}' cannot handle this query", preferred));
                }
            }
        }
        
        // Analyze query requirements
        if profile.requires_federation {
            reasoning.push("Query requires federation across multiple data sources".to_string());
            let trino_suggestion = suggestions.iter().find(|s| s.engine == "trino");
            if let Some(trino) = trino_suggestion {
                if trino.can_handle {
                    reasoning.push("Trino is the only engine that supports federated queries".to_string());
                    reasoning.push("Selecting Trino for federation requirement".to_string());
                    return Ok(("trino".to_string(), reasoning));
                } else {
                    return Err(RcaError::Execution(
                        "Query requires federation but Trino cannot handle it".to_string()
                    ));
                }
            } else {
                return Err(RcaError::Execution(
                    "Query requires federation but Trino is not available".to_string()
                ));
            }
        }
        
        // Check if speed optimization is preferred
        let prefer_speed = user_preferences.map(|p| p.prefer_speed).unwrap_or(false);
        if prefer_speed {
            reasoning.push("Speed optimization is preferred".to_string());
        }
        
        // Analyze scan size
        if let Some(scan_gb) = profile.estimated_scan_gb {
            reasoning.push(format!("Estimated data scan: {}GB", scan_gb));
            
            if scan_gb > 500 {
                reasoning.push("Large scan detected (>500GB)".to_string());
                let trino_suggestion = suggestions.iter().find(|s| s.engine == "trino");
                if let Some(trino) = trino_suggestion {
                    if trino.can_handle {
                        reasoning.push("Trino is best suited for large-scale distributed execution".to_string());
                        reasoning.push("Selecting Trino for large scan".to_string());
                        return Ok(("trino".to_string(), reasoning));
                    }
                }
            } else if scan_gb < 1 && prefer_speed {
                // Very small query: optimize for speed
                reasoning.push("Very small scan detected (<1GB) - optimizing for speed".to_string());
                let viable_engines: Vec<_> = suggestions.iter()
                    .filter(|s| s.can_handle)
                    .collect();
                
                if !viable_engines.is_empty() {
                    // Compare estimated execution times
                    let engine_times: Vec<(String, u64)> = viable_engines.iter()
                        .map(|s| {
                            let time = self.estimate_execution_time(profile, &s.engine).unwrap_or(u64::MAX);
                            (s.engine.clone(), time)
                        })
                        .collect();
                    
                    if let Some((fastest_engine, fastest_time)) = engine_times.iter().min_by_key(|(_, time)| time) {
                        reasoning.push(format!("Speed comparison: {} engines evaluated", engine_times.len()));
                        for (engine, time) in &engine_times {
                            reasoning.push(format!("  {}: {}ms estimated", engine, time));
                        }
                        reasoning.push(format!("Selecting {} (fastest: {}ms)", fastest_engine, fastest_time));
                        return Ok((fastest_engine.clone(), reasoning));
                    }
                }
            } else if scan_gb < 5 {
                reasoning.push("Small scan detected (<5GB)".to_string());
                // If speed is preferred, compare times; otherwise use Polars if available
                if prefer_speed {
                    let viable_engines: Vec<_> = suggestions.iter()
                        .filter(|s| s.can_handle)
                        .collect();
                    
                    if !viable_engines.is_empty() {
                        let engine_times: Vec<(String, u64)> = viable_engines.iter()
                            .map(|s| {
                                let time = self.estimate_execution_time(profile, &s.engine).unwrap_or(u64::MAX);
                                (s.engine.clone(), time)
                            })
                            .collect();
                        
                        if let Some((fastest_engine, fastest_time)) = engine_times.iter().min_by_key(|(_, time)| time) {
                            reasoning.push("Comparing engine speeds for small query".to_string());
                            for (engine, time) in &engine_times {
                                reasoning.push(format!("  {}: {}ms estimated", engine, time));
                            }
                            reasoning.push(format!("Selecting {} (fastest: {}ms)", fastest_engine, fastest_time));
                            return Ok((fastest_engine.clone(), reasoning));
                        }
                    }
                } else {
                    // Default behavior: prefer Polars for small queries
                    let polars_suggestion = suggestions.iter().find(|s| s.engine == "polars");
                    if let Some(polars) = polars_suggestion {
                        if polars.can_handle {
                            reasoning.push("Polars preview engine suitable for small queries".to_string());
                            reasoning.push("Selecting Polars for small scan".to_string());
                            return Ok(("polars".to_string(), reasoning));
                        }
                    }
                }
            }
        }
        
        // Analyze query complexity
        if profile.uses_ctes {
            reasoning.push("Query uses CTEs (Common Table Expressions)".to_string());
        }
        if profile.uses_window_functions {
            reasoning.push("Query uses window functions".to_string());
        }
        
        // Find viable engines (those that can handle the query)
        let viable_engines: Vec<_> = suggestions.iter()
            .filter(|s| s.can_handle)
            .collect();
        
        if viable_engines.is_empty() {
            let suggestions_str = suggestions.iter()
                .map(|s| format!("{} (can_handle: {}, score: {})", s.engine, s.can_handle, s.score))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(RcaError::Execution(
                format!("No engine can handle this query. Suggestions: {}", suggestions_str)
            ));
        }
        
        // If speed is preferred and multiple engines can handle, compare speeds
        if prefer_speed && viable_engines.len() > 1 {
            reasoning.push("Multiple engines can handle query - comparing speeds".to_string());
            
            let engine_times: Vec<(String, u64, i32)> = viable_engines.iter()
                .map(|s| {
                    let time = self.estimate_execution_time(profile, &s.engine).unwrap_or(u64::MAX);
                    (s.engine.clone(), time, s.score)
                })
                .collect();
            
            // Log all options
            for (engine, time, score) in &engine_times {
                reasoning.push(format!("  {}: {}ms estimated (capability score: {})", engine, time, score));
            }
            
            // Select fastest engine (with capability score as tiebreaker)
            if let Some((fastest_engine, fastest_time, _)) = engine_times.iter()
                .min_by_key(|(_, time, score)| (*time, -score))
            {
                reasoning.push(format!("Selecting {} (fastest: {}ms)", fastest_engine, fastest_time));
                return Ok((fastest_engine.clone(), reasoning));
            }
        }
        
        // Default: select best engine by capability score
        let best_engine = viable_engines.iter()
            .max_by_key(|s| s.score)
            .expect("At least one viable engine should exist");
        
        reasoning.push(format!("Evaluated {} engines", suggestions.len()));
        reasoning.push(format!("Best match: {} (score: {})", best_engine.engine, best_engine.score));
        reasoning.extend(best_engine.reasons.clone());
        reasoning.push(format!("Selecting {} based on query characteristics", best_engine.engine));
        
        Ok((best_engine.engine.clone(), reasoning))
    }
    
    /// Build alternatives from suggestions
    fn build_alternatives_from_suggestions(
        &self,
        suggestions: &[EngineSuggestion],
        selected_engine: &str,
    ) -> Vec<EngineAlternative> {
        suggestions.iter()
            .filter(|s| s.engine != selected_engine)
            .map(|s| {
                let reason = if !s.can_handle {
                    format!("Cannot handle query: {}", s.reasons.join(", "))
                } else {
                    format!("Score {} vs selected engine", s.score)
                };
                EngineAlternative {
                    engine: s.engine.clone(),
                    reason_not_selected: reason,
                }
            })
            .collect()
    }
    
    /// Enhance query profile with metadata information
    fn enhance_profile_with_metadata(
        &self,
        profile: &mut QueryProfile,
        metadata: &Metadata,
    ) -> Result<()> {
        // Enhance data sources with metadata
        for source in &mut profile.data_sources {
            // Try to find table in metadata
            if let Some(table) = metadata.tables.iter().find(|t| t.name == source.name) {
                // Update backend if known
                source.backend = table.system.clone();
                
                // Estimate size if available
                // (would need table statistics in metadata)
            }
        }
        
        // Check for federation
        let backends: std::collections::HashSet<String> = profile.data_sources.iter()
            .map(|ds| ds.backend.clone())
            .collect();
        
        profile.requires_federation = backends.len() > 1;
        
        // Calculate scan size
        profile.calculate_scan_size();
        
        Ok(())
    }
    
    /// Calculate confidence score for the decision
    fn calculate_confidence(&self, profile: &QueryProfile) -> u8 {
        let mut confidence = 50; // Base confidence
        
        // Higher confidence if we have scan size estimates
        if profile.estimated_scan_gb.is_some() {
            confidence += 20;
        }
        
        // Higher confidence if we know data sources
        if !profile.data_sources.is_empty() {
            confidence += 15;
        }
        
        // Higher confidence if federation requirement is clear
        if profile.requires_federation {
            confidence += 15;
        }
        
        confidence.min(100)
    }
    
    /// Estimate execution time (heuristic)
    /// 
    /// Accounts for:
    /// - Engine startup overhead (network latency for Trino, in-process for others)
    /// - Scan size (data volume to process)
    /// - Query complexity (CTEs, window functions, joins)
    fn estimate_execution_time(&self, profile: &QueryProfile, engine: &str) -> Option<u64> {
        // Startup overhead (one-time cost)
        let startup_overhead = match engine {
            "trino" => 3000,  // Network round-trip + coordinator overhead (~3s)
            "duckdb" => 50,    // In-process, minimal overhead (~50ms)
            "polars" => 10,    // In-process, minimal overhead (~10ms)
            _ => 1000,
        };
        
        // Base query processing time (per engine characteristics)
        let base_processing_time = match engine {
            "trino" => 2000,   // Distributed processing overhead
            "duckdb" => 500,   // Efficient in-process processing
            "polars" => 200,   // Very fast for simple queries
            _ => 1000,
        };
        
        // Scan size factor (time to read/process data)
        // Different engines have different scan speeds
        let scan_factor = match engine {
            "trino" => {
                // Trino: distributed, good for large scans
                profile.estimated_scan_gb.unwrap_or(0) as u64 * 50  // 50ms per GB (distributed)
            },
            "duckdb" => {
                // DuckDB: efficient columnar processing
                profile.estimated_scan_gb.unwrap_or(0) as u64 * 80  // 80ms per GB
            },
            "polars" => {
                // Polars: very fast for small scans, but limited for large
                profile.estimated_scan_gb.unwrap_or(0) as u64 * 60  // 60ms per GB (but limited to <10GB)
            },
            _ => profile.estimated_scan_gb.unwrap_or(0) as u64 * 100,
        };
        
        // Complexity factor (query features that add processing time)
        let complexity_factor = match engine {
            "trino" => {
                // Trino handles complexity well but with overhead
                profile.complexity_score as u64 * 15  // 15ms per complexity point
            },
            "duckdb" => {
                // DuckDB handles complexity efficiently
                profile.complexity_score as u64 * 8   // 8ms per complexity point
            },
            "polars" => {
                // Polars struggles with complex queries
                let base_complexity = profile.complexity_score as u64 * 10;
                // Penalty for features Polars doesn't support well
                let penalty = if profile.uses_ctes { 500 } else { 0 };
                base_complexity + penalty
            },
            _ => profile.complexity_score as u64 * 10,
        };
        
        // Join factor (joins add processing time)
        let join_factor = match engine {
            "trino" => profile.join_count as u64 * 200,  // Distributed join overhead
            "duckdb" => profile.join_count as u64 * 100,  // Efficient join processing
            "polars" => profile.join_count as u64 * 150,  // Moderate join performance
            _ => profile.join_count as u64 * 150,
        };
        
        let total_time = startup_overhead + base_processing_time + scan_factor + complexity_factor + join_factor;
        
        Some(total_time)
    }
    
    /// Build list of alternative engines and why they weren't selected
    fn build_alternatives(
        &self,
        profile: &QueryProfile,
        selected_engine: &str,
    ) -> Vec<EngineAlternative> {
        let mut alternatives = Vec::new();
        
        // Check DuckDB
        if selected_engine != "duckdb" {
            if profile.requires_federation {
                alternatives.push(EngineAlternative {
                    engine: "duckdb".to_string(),
                    reason_not_selected: "DuckDB does not support federated queries".to_string(),
                });
            } else if let Some(scan_gb) = profile.estimated_scan_gb {
                if scan_gb > 100 {
                    alternatives.push(EngineAlternative {
                        engine: "duckdb".to_string(),
                        reason_not_selected: format!("Scan size {}GB exceeds DuckDB limit", scan_gb),
                    });
                }
            }
        }
        
        // Check Trino
        if selected_engine != "trino" {
            if !profile.requires_federation {
                if let Some(scan_gb) = profile.estimated_scan_gb {
                    if scan_gb < 500 {
                        alternatives.push(EngineAlternative {
                            engine: "trino".to_string(),
                            reason_not_selected: format!(
                                "Query does not require Trino (scan size {}GB < 500GB threshold)",
                                scan_gb
                            ),
                        });
                    }
                } else {
                    alternatives.push(EngineAlternative {
                        engine: "trino".to_string(),
                        reason_not_selected: "Query does not require distributed execution".to_string(),
                    });
                }
            }
        }
        
        // Check Polars
        if selected_engine != "polars" {
            if profile.uses_ctes {
                alternatives.push(EngineAlternative {
                    engine: "polars".to_string(),
                    reason_not_selected: "Polars does not support CTEs".to_string(),
                });
            } else if let Some(scan_gb) = profile.estimated_scan_gb {
                if scan_gb > 5 {
                    alternatives.push(EngineAlternative {
                        engine: "polars".to_string(),
                        reason_not_selected: format!("Scan size {}GB exceeds Polars preview limit", scan_gb),
                    });
                }
            }
        }
        
        alternatives
    }
}

/// Helper function for agents to decide engine (returns full decision)
pub fn agent_decide_engine(
    router: &ExecutionRouter,
    query: &str,
    metadata: Option<&Metadata>,
) -> Result<AgentDecision> {
    let context = AgentDecisionContext {
        query: query.to_string(),
        profile: None,
        metadata: metadata.cloned(),
        user_preferences: None,
        available_engines: router.available_engines(),
    };
    
    let decision_maker = AgentDecisionMaker::new(router);
    decision_maker.decide(&context)
}

/// Helper function for agents to get engine selection (for direct use in execution)
/// 
/// This returns an EngineSelection that can be passed directly to router.execute()
pub fn agent_select_engine(
    router: &ExecutionRouter,
    query: &str,
    metadata: Option<&Metadata>,
) -> Result<EngineSelection> {
    let decision = agent_decide_engine(router, query, metadata)?;
    let engine_name = decision.engine.clone();
    
    Ok(EngineSelection {
        engine_name: decision.engine,
        reasoning: decision.reasoning,
        fallback_available: router.has_fallback(&engine_name),
    })
}

