//! Resource Limits
//! 
//! Implements resource limits and enforcement for RCA execution:
//! - Max rows scanned
//! - Max memory usage
//! - Max nodes executed
//! - Timeout per phase
//! - Cost budget

use crate::error::{RcaError, Result};
use std::time::{Duration, Instant};

/// Resource limits configuration
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum rows to scan (default: 10M)
    pub max_rows_scanned: usize,
    /// Maximum memory in MB (default: 8GB = 8192 MB)
    pub max_memory_mb: usize,
    /// Maximum nodes to execute (default: 100)
    pub max_nodes: usize,
    /// Timeout per phase (default: 5 minutes)
    pub timeout_per_phase: Duration,
    /// Cost budget (default: 1000.0)
    pub cost_budget: f64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_rows_scanned: 10_000_000, // 10M rows
            max_memory_mb: 8192,          // 8GB
            max_nodes: 100,
            timeout_per_phase: Duration::from_secs(5 * 60), // 5 minutes
            cost_budget: 1000.0,
        }
    }
}

impl ResourceLimits {
    /// Create with custom limits
    pub fn new(
        max_rows_scanned: usize,
        max_memory_mb: usize,
        max_nodes: usize,
        timeout_per_phase: Duration,
        cost_budget: f64,
    ) -> Self {
        Self {
            max_rows_scanned,
            max_memory_mb,
            max_nodes,
            timeout_per_phase,
            cost_budget,
        }
    }
    
    /// Create with conservative limits (for Fast mode)
    pub fn conservative() -> Self {
        Self {
            max_rows_scanned: 1_000_000,  // 1M rows
            max_memory_mb: 2048,          // 2GB
            max_nodes: 50,
            timeout_per_phase: Duration::from_secs(2 * 60), // 2 minutes
            cost_budget: 500.0,
        }
    }
    
    /// Create with aggressive limits (for Deep/Forensic mode)
    pub fn aggressive() -> Self {
        Self {
            max_rows_scanned: 50_000_000, // 50M rows
            max_memory_mb: 16384,         // 16GB
            max_nodes: 200,
            timeout_per_phase: Duration::from_secs(10 * 60), // 10 minutes
            cost_budget: 5000.0,
        }
    }
}

/// Resource usage tracker
#[derive(Debug, Clone)]
pub struct ResourceUsage {
    /// Rows scanned so far
    pub rows_scanned: usize,
    /// Memory used in MB (estimated)
    pub memory_mb: f64,
    /// Nodes executed so far
    pub nodes_executed: usize,
    /// Cost consumed so far
    pub cost_consumed: f64,
    /// Phase start time
    pub phase_start_time: Option<Instant>,
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            rows_scanned: 0,
            memory_mb: 0.0,
            nodes_executed: 0,
            cost_consumed: 0.0,
            phase_start_time: None,
        }
    }
}

impl ResourceUsage {
    /// Create new resource usage tracker
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Start a new phase
    pub fn start_phase(&mut self) {
        self.phase_start_time = Some(Instant::now());
    }
    
    /// End current phase
    pub fn end_phase(&mut self) {
        self.phase_start_time = None;
    }
    
    /// Add rows scanned
    pub fn add_rows_scanned(&mut self, rows: usize) {
        self.rows_scanned += rows;
    }
    
    /// Add memory usage (in MB)
    pub fn add_memory(&mut self, memory_mb: f64) {
        self.memory_mb += memory_mb;
    }
    
    /// Increment nodes executed
    pub fn increment_nodes(&mut self) {
        self.nodes_executed += 1;
    }
    
    /// Add cost
    pub fn add_cost(&mut self, cost: f64) {
        self.cost_consumed += cost;
    }
    
    /// Get elapsed time for current phase
    pub fn phase_elapsed(&self) -> Option<Duration> {
        self.phase_start_time.map(|start| start.elapsed())
    }
}

/// Resource limit enforcer
pub struct ResourceEnforcer {
    limits: ResourceLimits,
    usage: ResourceUsage,
}

impl ResourceEnforcer {
    /// Create a new resource enforcer with default limits
    pub fn new() -> Self {
        Self {
            limits: ResourceLimits::default(),
            usage: ResourceUsage::new(),
        }
    }
    
    /// Create with custom limits
    pub fn with_limits(limits: ResourceLimits) -> Self {
        Self {
            limits,
            usage: ResourceUsage::new(),
        }
    }
    
    /// Check if resource limits are exceeded
    /// 
    /// Returns Ok(()) if within limits, Err if exceeded
    pub fn check_limits(&self) -> Result<()> {
        // Check rows scanned
        if self.usage.rows_scanned > self.limits.max_rows_scanned {
            return Err(RcaError::Execution(format!(
                "Resource limit exceeded: rows_scanned ({}) > max_rows_scanned ({})",
                self.usage.rows_scanned, self.limits.max_rows_scanned
            )));
        }
        
        // Check memory
        if self.usage.memory_mb > self.limits.max_memory_mb as f64 {
            return Err(RcaError::Execution(format!(
                "Resource limit exceeded: memory_mb ({:.2}) > max_memory_mb ({})",
                self.usage.memory_mb, self.limits.max_memory_mb
            )));
        }
        
        // Check nodes
        if self.usage.nodes_executed > self.limits.max_nodes {
            return Err(RcaError::Execution(format!(
                "Resource limit exceeded: nodes_executed ({}) > max_nodes ({})",
                self.usage.nodes_executed, self.limits.max_nodes
            )));
        }
        
        // Check cost budget
        if self.usage.cost_consumed > self.limits.cost_budget {
            return Err(RcaError::Execution(format!(
                "Resource limit exceeded: cost_consumed ({:.2}) > cost_budget ({:.2})",
                self.usage.cost_consumed, self.limits.cost_budget
            )));
        }
        
        // Check phase timeout
        if let Some(elapsed) = self.usage.phase_elapsed() {
            if elapsed > self.limits.timeout_per_phase {
                return Err(RcaError::Execution(format!(
                    "Resource limit exceeded: phase_elapsed ({:?}) > timeout_per_phase ({:?})",
                    elapsed, self.limits.timeout_per_phase
                )));
            }
        }
        
        Ok(())
    }
    
    /// Check if we can execute a node with given resource requirements
    pub fn can_execute_node(&self, estimated_rows: usize, estimated_memory_mb: f64, estimated_cost: f64) -> Result<()> {
        // Check if adding this node would exceed limits
        let new_rows = self.usage.rows_scanned + estimated_rows;
        let new_memory = self.usage.memory_mb + estimated_memory_mb;
        let new_nodes = self.usage.nodes_executed + 1;
        let new_cost = self.usage.cost_consumed + estimated_cost;
        
        if new_rows > self.limits.max_rows_scanned {
            return Err(RcaError::Execution(format!(
                "Cannot execute node: would exceed max_rows_scanned ({} + {} > {})",
                self.usage.rows_scanned, estimated_rows, self.limits.max_rows_scanned
            )));
        }
        
        if new_memory > self.limits.max_memory_mb as f64 {
            return Err(RcaError::Execution(format!(
                "Cannot execute node: would exceed max_memory_mb ({:.2} + {:.2} > {})",
                self.usage.memory_mb, estimated_memory_mb, self.limits.max_memory_mb
            )));
        }
        
        if new_nodes > self.limits.max_nodes {
            return Err(RcaError::Execution(format!(
                "Cannot execute node: would exceed max_nodes ({} + 1 > {})",
                self.usage.nodes_executed, self.limits.max_nodes
            )));
        }
        
        if new_cost > self.limits.cost_budget {
            return Err(RcaError::Execution(format!(
                "Cannot execute node: would exceed cost_budget ({:.2} + {:.2} > {:.2})",
                self.usage.cost_consumed, estimated_cost, self.limits.cost_budget
            )));
        }
        
        Ok(())
    }
    
    /// Get current usage
    pub fn usage(&self) -> &ResourceUsage {
        &self.usage
    }
    
    /// Get mutable usage (for updating)
    pub fn usage_mut(&mut self) -> &mut ResourceUsage {
        &mut self.usage
    }
    
    /// Get limits
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }
    
    /// Update limits
    pub fn set_limits(&mut self, limits: ResourceLimits) {
        self.limits = limits;
    }
}

impl Default for ResourceEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_rows_scanned, 10_000_000);
        assert_eq!(limits.max_memory_mb, 8192);
        assert_eq!(limits.max_nodes, 100);
    }
    
    #[test]
    fn test_resource_enforcer_check() {
        let mut enforcer = ResourceEnforcer::new();
        
        // Should pass initially
        assert!(enforcer.check_limits().is_ok());
        
        // Exceed rows limit
        enforcer.usage_mut().rows_scanned = 11_000_000;
        assert!(enforcer.check_limits().is_err());
    }
    
    #[test]
    fn test_resource_enforcer_can_execute() {
        let enforcer = ResourceEnforcer::new();
        
        // Should be able to execute small node
        assert!(enforcer.can_execute_node(1000, 10.0, 10.0).is_ok());
        
        // Should fail for very large node
        assert!(enforcer.can_execute_node(20_000_000, 10.0, 10.0).is_err());
    }
}





