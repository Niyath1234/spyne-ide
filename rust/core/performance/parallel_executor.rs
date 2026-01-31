//! Parallel Execution Module
//! 
//! Provides parallel execution capabilities:
//! - Parallel node execution respecting dependencies (using tokio for async tasks)
//! - Parallel system execution using tokio (for async I/O)

use crate::error::{RcaError, Result};
use std::sync::Arc;
use std::collections::HashMap;
use std::hash::Hash;

/// Dependency graph for parallel execution
/// 
/// Tracks which nodes depend on which other nodes
pub struct DependencyGraph<T> 
where
    T: Hash + Eq + Clone + Send + Sync,
{
    /// Map from node to its dependencies (nodes that must complete first)
    dependencies: HashMap<T, Vec<T>>,
    /// Map from node to nodes that depend on it
    dependents: HashMap<T, Vec<T>>,
}

impl<T> DependencyGraph<T>
where
    T: Hash + Eq + Clone + Send + Sync,
{
    /// Create a new dependency graph
    pub fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
        }
    }
    
    /// Add a node with its dependencies
    pub fn add_node(&mut self, node: T, deps: Vec<T>) {
        // Record dependencies
        self.dependencies.insert(node.clone(), deps.clone());
        
        // Record reverse dependencies (dependents)
        for dep in deps {
            self.dependents.entry(dep).or_insert_with(Vec::new).push(node.clone());
        }
        
        // Ensure node exists in dependents map (even if no one depends on it)
        self.dependents.entry(node.clone()).or_insert_with(Vec::new);
    }
    
    /// Get dependencies for a node
    pub fn dependencies(&self, node: &T) -> Vec<T> {
        self.dependencies.get(node).cloned().unwrap_or_default()
    }
    
    /// Get nodes that depend on this node
    pub fn dependents(&self, node: &T) -> Vec<T> {
        self.dependents.get(node).cloned().unwrap_or_default()
    }
    
    /// Get all nodes with no dependencies (ready to execute)
    pub fn ready_nodes(&self, completed: &std::collections::HashSet<T>) -> Vec<T> {
        self.dependencies
            .iter()
            .filter(|(node, deps)| {
                // Node is ready if all its dependencies are completed
                deps.iter().all(|dep| completed.contains(dep))
            })
            .map(|(node, _)| node.clone())
            .collect()
    }
    
    /// Perform topological sort to get execution order
    /// 
    /// Returns nodes in order such that dependencies come before dependents
    pub fn topological_sort(&self) -> Result<Vec<T>> {
        let mut result = Vec::new();
        let mut in_degree: HashMap<T, usize> = HashMap::new();
        
        // Initialize in-degree for all nodes
        for node in self.dependencies.keys() {
            in_degree.insert(node.clone(), self.dependencies(node).len());
        }
        
        // Find nodes with no dependencies
        let mut queue: Vec<T> = in_degree
            .iter()
            .filter(|(_, &degree)| degree == 0)
            .map(|(node, _)| node.clone())
            .collect();
        
        // Process nodes
        while let Some(node) = queue.pop() {
            result.push(node.clone());
            
            // Decrease in-degree for dependents
            if let Some(deps) = self.dependents.get(&node) {
                for dependent in deps {
                    if let Some(degree) = in_degree.get_mut(dependent) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push(dependent.clone());
                        }
                    }
                }
            }
        }
        
        // Check for cycles
        if result.len() != self.dependencies.len() {
            return Err(RcaError::Execution(
                "Dependency graph contains cycles - cannot perform topological sort".to_string()
            ));
        }
        
        Ok(result)
    }
}

impl<T> Default for DependencyGraph<T>
where
    T: Hash + Eq + Clone + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Parallel executor for async tasks using tokio
/// 
/// Executes independent tasks in parallel while respecting dependencies.
/// Note: For node-level parallelism, use DependencyGraph to determine execution order,
/// then execute independent nodes at each level using AsyncParallelExecutor.
pub struct ParallelExecutor;

impl ParallelExecutor {
    /// Group nodes by execution level (nodes that can run in parallel)
    /// 
    /// Given a dependency graph and execution order, groups nodes into levels
    /// where all nodes in a level can execute in parallel.
    pub fn group_by_level<T>(
        graph: &DependencyGraph<T>,
        execution_order: &[T],
    ) -> Vec<Vec<T>>
    where
        T: Hash + Eq + Clone + Send + Sync,
    {
        let mut levels = Vec::new();
        let mut node_to_level: std::collections::HashMap<T, usize> = std::collections::HashMap::new();
        
        for node in execution_order {
            let deps = graph.dependencies(node);
            
            // Find the maximum level of all dependencies
            let max_dep_level = deps.iter()
                .filter_map(|dep| node_to_level.get(dep))
                .max()
                .copied();
            
            // Node's level is max(dependency levels) + 1, or 0 if no dependencies
            let node_level = max_dep_level.map(|l| l + 1).unwrap_or(0);
            
            // Ensure we have enough levels
            while levels.len() <= node_level {
                levels.push(Vec::new());
            }
            
            // Add node to its level
            levels[node_level].push(node.clone());
            node_to_level.insert(node.clone(), node_level);
        }
        
        levels
    }
}

/// Helper for parallel async execution using tokio
pub struct AsyncParallelExecutor;

impl AsyncParallelExecutor {
    /// Execute async tasks in parallel using tokio
    /// 
    /// All tasks are spawned concurrently and awaited together.
    /// 
    /// # Arguments
    /// * `tasks` - Vector of async tasks (futures)
    /// 
    /// # Returns
    /// Vector of results in the same order as input tasks
    pub async fn execute_parallel<F, R>(tasks: Vec<F>) -> Vec<Result<R>>
    where
        F: std::future::Future<Output = Result<R>> + Send + 'static,
        R: Send + 'static,
    {
        // Spawn all tasks concurrently
        let handles: Vec<_> = tasks
            .into_iter()
            .map(|task| tokio::spawn(task))
            .collect();
        
        // Await all tasks
        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(RcaError::Execution(format!(
                    "Task execution failed: {}",
                    e
                )))),
            }
        }
        
        results
    }
    
    /// Execute two async tasks in parallel
    /// 
    /// Convenience method for executing two systems in parallel
    pub async fn execute_pair<F1, F2, R1, R2>(
        task1: F1,
        task2: F2,
    ) -> Result<(R1, R2)>
    where
        F1: std::future::Future<Output = Result<R1>> + Send + 'static,
        F2: std::future::Future<Output = Result<R2>> + Send + 'static,
        R1: Send + 'static,
        R2: Send + 'static,
    {
        let (result1, result2) = tokio::join!(task1, task2);
        Ok((result1?, result2?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_dependency_graph_topological_sort() {
        let mut graph = DependencyGraph::new();
        
        // A -> B -> C
        graph.add_node("A".to_string(), vec![]);
        graph.add_node("B".to_string(), vec!["A".to_string()]);
        graph.add_node("C".to_string(), vec!["B".to_string()]);
        
        let order = graph.topological_sort().unwrap();
        assert_eq!(order, vec!["A", "B", "C"]);
    }
    
    #[test]
    fn test_dependency_graph_parallel_levels() {
        let mut graph = DependencyGraph::new();
        
        // A -> B, C
        // B -> D
        // C -> D
        graph.add_node("A".to_string(), vec![]);
        graph.add_node("B".to_string(), vec!["A".to_string()]);
        graph.add_node("C".to_string(), vec!["A".to_string()]);
        graph.add_node("D".to_string(), vec!["B".to_string(), "C".to_string()]);
        
        let order = graph.topological_sort().unwrap();
        let levels = ParallelExecutor::group_by_level(&graph, &order);
        
        // Level 0: A
        // Level 1: B, C (can run in parallel)
        // Level 2: D
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec!["A"]);
        assert!(levels[1].contains(&"B".to_string()));
        assert!(levels[1].contains(&"C".to_string()));
        assert_eq!(levels[2], vec!["D"]);
    }
}

