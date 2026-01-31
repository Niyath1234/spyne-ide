//! Execution Router - Selects appropriate execution engine based on query profile
//! 
//! This is the "brain" that decides which engine to use for each query.
//! It implements the routing logic described in the design document.

use crate::error::{RcaError, Result};
use crate::execution::agent_decision::EnginePreferences;
use crate::execution::engine::{EngineCapabilities, EngineSelection, EngineSuggestion, ExecutionContext, ExecutionEngine};
use crate::execution::profile::QueryProfile;
use crate::execution::result::QueryResult;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn, debug};

/// Execution router - executes queries with explicitly selected engines
/// 
/// NOTE: No default engine - the Agent must explicitly decide which engine to use.
/// This decision should be part of the agent's chain of thought.
pub struct ExecutionRouter {
    engines: Vec<Arc<dyn ExecutionEngine>>,
    enable_trino: bool,
    enable_polars: bool,
}

impl ExecutionRouter {
    /// Create a new execution router
    /// 
    /// No default engine - agent must explicitly select engine for each query.
    pub fn new(engines: Vec<Arc<dyn ExecutionEngine>>) -> Self {
        let enable_trino = engines.iter().any(|e| e.name() == "trino");
        let enable_polars = engines.iter().any(|e| e.name() == "polars");
        
        Self {
            engines,
            enable_trino,
            enable_polars,
        }
    }
    
    /// Get available engines
    pub fn available_engines(&self) -> Vec<String> {
        self.engines.iter().map(|e| e.name().to_string()).collect()
    }
    
    /// Get engine capabilities for agent reasoning
    pub fn get_engine_capabilities(&self, engine_name: &str) -> Option<&EngineCapabilities> {
        self.find_engine(engine_name).map(|e| e.capabilities())
    }
    
    /// Suggest engine based on query profile (for agent reasoning)
    /// 
    /// This provides suggestions to help the agent decide, but the agent
    /// must make the final decision as part of its chain of thought.
    /// 
    /// Optionally accepts user preferences to include speed optimization in scoring.
    pub fn suggest_engine(
        &self,
        profile: &QueryProfile,
    ) -> Result<Vec<EngineSuggestion>> {
        self.suggest_engine_with_preferences(profile, None)
    }
    
    /// Suggest engine with user preferences (for speed optimization)
    pub fn suggest_engine_with_preferences(
        &self,
        profile: &QueryProfile,
        user_preferences: Option<&EnginePreferences>,
    ) -> Result<Vec<EngineSuggestion>> {
        let prefer_speed = user_preferences.map(|p| p.prefer_speed).unwrap_or(false);
        let mut suggestions = Vec::new();
        
        // Analyze each available engine
        for engine in &self.engines {
            let engine_name = engine.name();
            let capabilities = engine.capabilities();
            
            // Check if engine can handle this query
            let can_handle = engine.validate(profile).is_ok();
            
            // Build reasoning
            let mut reasons = Vec::new();
            let mut score = 0;
            
            // Federation check
            if profile.requires_federation {
                if capabilities.supports_federated_sources {
                    reasons.push("Supports federated queries".to_string());
                    score += 30;
                } else {
                    reasons.push("Does not support federated queries".to_string());
                    score -= 50;
                }
            }
            
            // Scan size check
            if let Some(scan_gb) = profile.estimated_scan_gb {
                if let Some(max_gb) = capabilities.max_data_scan_gb {
                    if scan_gb <= max_gb {
                        reasons.push(format!("Scan size {}GB within limit ({}GB)", scan_gb, max_gb));
                        score += 20;
                    } else {
                        reasons.push(format!("Scan size {}GB exceeds limit ({}GB)", scan_gb, max_gb));
                        score -= 50;
                    }
                } else {
                    reasons.push("No scan size limit".to_string());
                    score += 10;
                }
            }
            
            // CTE support
            if profile.uses_ctes {
                if capabilities.supports_ctes {
                    reasons.push("Supports CTEs".to_string());
                    score += 15;
                } else {
                    reasons.push("Does not support CTEs".to_string());
                    score -= 30;
                }
            }
            
            // Window function support
            if profile.uses_window_functions {
                if capabilities.supports_window_functions {
                    reasons.push("Supports window functions".to_string());
                    score += 15;
                } else {
                    reasons.push("Does not support window functions".to_string());
                    score -= 30;
                }
            }
            
            // Complexity consideration
            if profile.complexity_score > 50 {
                if engine_name == "duckdb" || engine_name == "trino" {
                    reasons.push("Good for complex queries".to_string());
                    score += 10;
                }
            }
            
            // Speed optimization: add speed bonus when speed is preferred
            if prefer_speed && can_handle {
                let speed_bonus = match engine_name {
                    "polars" => {
                        // Polars is fastest for small, simple queries
                        if let Some(scan_gb) = profile.estimated_scan_gb {
                            if scan_gb < 5 && !profile.uses_ctes {
                                25  // High bonus for small, simple queries
                            } else {
                                10  // Moderate bonus otherwise
                            }
                        } else {
                            15  // Default bonus
                        }
                    },
                    "duckdb" => {
                        // DuckDB is fast for medium queries
                        if let Some(scan_gb) = profile.estimated_scan_gb {
                            if scan_gb < 100 {
                                20  // High bonus for medium queries
                            } else {
                                10  // Moderate bonus for larger queries
                            }
                        } else {
                            15  // Default bonus
                        }
                    },
                    "trino" => {
                        // Trino has network overhead, but good for large scans
                        if let Some(scan_gb) = profile.estimated_scan_gb {
                            if scan_gb > 100 {
                                15  // Bonus for large scans where Trino excels
                            } else {
                                5   // Small bonus for smaller queries (overhead penalty)
                            }
                        } else {
                            5   // Small bonus (network overhead)
                        }
                    },
                    _ => 0,
                };
                score += speed_bonus;
                if speed_bonus > 0 {
                    reasons.push(format!("Speed bonus: +{} (speed optimization)", speed_bonus));
                }
            }
            
            // Estimate execution time (heuristic)
            let estimated_time_ms = self.estimate_execution_time(profile, engine_name);
            
            suggestions.push(EngineSuggestion {
                engine: engine_name.to_string(),
                can_handle,
                score,
                reasons,
                capabilities: capabilities.clone(),
                estimated_time_ms,
            });
        }
        
        // Sort by score (highest first)
        suggestions.sort_by(|a, b| {
            // If speed is preferred and both can handle, also consider estimated time
            if prefer_speed && a.can_handle && b.can_handle {
                match (a.estimated_time_ms, b.estimated_time_ms) {
                    (Some(time_a), Some(time_b)) => {
                        // Compare by score first, then by time (faster is better)
                        b.score.cmp(&a.score).then(time_a.cmp(&time_b))
                    },
                    _ => b.score.cmp(&a.score),
                }
            } else {
                b.score.cmp(&a.score)
            }
        });
        
        Ok(suggestions)
    }
    
    /// Estimate execution time for an engine (heuristic)
    /// 
    /// This is a simplified version - the full estimation is in AgentDecisionMaker
    fn estimate_execution_time(&self, profile: &QueryProfile, engine_name: &str) -> Option<u64> {
        // Startup overhead
        let startup_overhead = match engine_name {
            "trino" => 3000,
            "duckdb" => 50,
            "polars" => 10,
            _ => 1000,
        };
        
        // Base processing time
        let base_time = match engine_name {
            "trino" => 2000,
            "duckdb" => 500,
            "polars" => 200,
            _ => 1000,
        };
        
        // Scan size factor
        let scan_factor = match engine_name {
            "trino" => profile.estimated_scan_gb.unwrap_or(0) as u64 * 50,
            "duckdb" => profile.estimated_scan_gb.unwrap_or(0) as u64 * 80,
            "polars" => profile.estimated_scan_gb.unwrap_or(0) as u64 * 60,
            _ => profile.estimated_scan_gb.unwrap_or(0) as u64 * 100,
        };
        
        // Complexity factor
        let complexity_factor = match engine_name {
            "trino" => profile.complexity_score as u64 * 15,
            "duckdb" => profile.complexity_score as u64 * 8,
            "polars" => {
                let base = profile.complexity_score as u64 * 10;
                let penalty = if profile.uses_ctes { 500 } else { 0 };
                base + penalty
            },
            _ => profile.complexity_score as u64 * 10,
        };
        
        // Join factor
        let join_factor = match engine_name {
            "trino" => profile.join_count as u64 * 200,
            "duckdb" => profile.join_count as u64 * 100,
            "polars" => profile.join_count as u64 * 150,
            _ => profile.join_count as u64 * 150,
        };
        
        Some(startup_overhead + base_time + scan_factor + complexity_factor + join_factor)
    }
    
    /// Execute query with explicitly selected engine
    /// 
    /// The engine must be explicitly selected by the agent as part of its chain of thought.
    /// Use `suggest_engine()` to get suggestions for agent reasoning.
    pub async fn execute(
        &self,
        sql: &str,
        profile: &QueryProfile,
        ctx: &ExecutionContext,
        engine_selection: &EngineSelection, // Agent's explicit decision
    ) -> Result<QueryResult> {
        info!("Executing with agent-selected engine: {} - Reasoning: {:?}", 
            engine_selection.engine_name, engine_selection.reasoning);
        
        let engine = self.find_engine(&engine_selection.engine_name)
            .ok_or_else(|| RcaError::Execution(
                format!("Agent selected engine '{}' not available. Available engines: {:?}", 
                    engine_selection.engine_name, 
                    self.available_engines())
            ))?;
        
        // Validate engine can handle this query
        if let Err(e) = engine.validate(profile) {
            return Err(RcaError::Execution(
                format!("Agent selected engine '{}' cannot handle this query: {}. Agent reasoning: {:?}", 
                    engine_selection.engine_name, e, engine_selection.reasoning)
            ));
        }
        
        // Execute
        let start_time = std::time::Instant::now();
        match engine.execute(sql, ctx).await {
            Ok(mut result) => {
                result.execution_time_ms = start_time.elapsed().as_millis() as u64;
                result.engine_metadata.insert(
                    "agent_reasoning".to_string(),
                    serde_json::Value::Array(
                        engine_selection.reasoning.iter()
                            .map(|r| serde_json::Value::String(r.clone()))
                            .collect()
                    ),
                );
                result.engine_metadata.insert(
                    "agent_selected_engine".to_string(),
                    serde_json::Value::String(engine_selection.engine_name.clone()),
                );
                Ok(result)
            }
            Err(e) => {
                // Try fallback on failure (if agent indicated fallback available)
                if engine_selection.fallback_available {
                    warn!("Engine execution failed: {}, attempting fallback", e);
                    return self.execute_with_fallback(sql, profile, ctx, &engine_selection.engine_name).await;
                }
                Err(RcaError::Execution(
                    format!("Agent selected engine '{}' execution failed: {}. Agent reasoning: {:?}", 
                        engine_selection.engine_name, e, engine_selection.reasoning)
                ))
            }
        }
    }
    
    /// Execute with fallback to DuckDB
    async fn execute_with_fallback(
        &self,
        sql: &str,
        profile: &QueryProfile,
        ctx: &ExecutionContext,
        failed_engine: &str,
    ) -> Result<QueryResult> {
        info!("Attempting fallback from {} to DuckDB", failed_engine);
        
        // Fallback rules:
        // - Polars fails → DuckDB retry
        // - DuckDB fails → no fallback
        // - Trino fails → DuckDB retry (only if single-source)
        
        if failed_engine == "duckdb" {
            return Err(RcaError::Execution(
                "DuckDB execution failed and no fallback available".to_string()
            ));
        }
        
        // Trino can only fallback if not federated
        if failed_engine == "trino" && profile.requires_federation {
            return Err(RcaError::Execution(
                "Trino failed on federated query, no fallback available".to_string()
            ));
        }
        
        // Fallback to DuckDB
        let duckdb_engine = self.find_engine("duckdb")
            .ok_or_else(|| RcaError::Execution("DuckDB fallback not available".to_string()))?;
        
        let start_time = std::time::Instant::now();
        match duckdb_engine.execute(sql, ctx).await {
            Ok(mut result) => {
                result.execution_time_ms = start_time.elapsed().as_millis() as u64;
                result.warnings.push(format!("Executed via fallback from {}", failed_engine));
                Ok(result)
            }
            Err(e) => Err(e)
        }
    }
    
    /// Find engine by name
    fn find_engine(&self, name: &str) -> Option<&Arc<dyn ExecutionEngine>> {
        self.engines.iter().find(|e| e.name() == name)
    }
    
    /// Check if fallback is available for an engine
    pub fn has_fallback(&self, engine_name: &str) -> bool {
        match engine_name {
            "polars" => self.find_engine("duckdb").is_some(),
            "trino" => self.find_engine("duckdb").is_some(),
            "duckdb" => false, // DuckDB is the final fallback
            _ => false,
        }
    }
}

