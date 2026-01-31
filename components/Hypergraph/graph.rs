use super::node::{HyperNode, NodeId};
use super::edge::{HyperEdge, EdgeId};
use super::path::{HyperPath, PathId, PathSignature};
use super::types::ColumnFragment;
use dashmap::DashMap;
use std::sync::Arc;
use std::collections::HashMap;

/// The main hypergraph structure
/// Stores all nodes, edges, and paths
pub struct HyperGraph {
    /// Map from node ID to node
    nodes: DashMap<NodeId, Arc<HyperNode>>,
    
    /// Map from edge ID to edge
    edges: DashMap<EdgeId, Arc<HyperEdge>>,
    
    /// Adjacency list: node_id -> list of outgoing edge IDs
    adjacency: DashMap<NodeId, Vec<EdgeId>>,
    
    /// Reverse adjacency: node_id -> list of incoming edge IDs
    reverse_adjacency: DashMap<NodeId, Vec<EdgeId>>,
    
    /// Path cache for reusing paths
    path_cache: Arc<dashmap::DashMap<PathSignature, HyperPath>>,
    
    /// Optimized shortest path cache (all-pairs, O(1) lookup)
    shortest_path_cache: Arc<dashmap::DashMap<(NodeId, NodeId), Vec<EdgeId>>>,
    
    /// Distance cache: (from, to) -> distance
    shortest_distance_cache: Arc<dashmap::DashMap<(NodeId, NodeId), usize>>,
    
    /// Graph version (increments when structure changes)
    graph_version: std::sync::atomic::AtomicU64,
    
    /// Map from (schema, table, column) to node ID
    table_column_map: DashMap<(String, String, String), NodeId>,
    
    /// Map from (schema, table) to table node ID (for O(1) table lookups)
    pub(crate) table_index: DashMap<(String, String), NodeId>,
    
    /// Hot fragment statistics: (node_id, fragment_idx) -> stats
    fragment_stats: DashMap<(NodeId, usize), FragmentStats>,
    
    /// Next node ID
    next_node_id: std::sync::atomic::AtomicU64,
    
    /// Next edge ID
    next_edge_id: std::sync::atomic::AtomicU64,
    
    /// Next path ID
    next_path_id: std::sync::atomic::AtomicU64,
}

/// Hot fragment statistics
#[derive(Clone, Debug)]
pub struct FragmentStats {
    /// Number of times this fragment has been accessed
    pub access_count: u64,
    /// Last access time (nanoseconds since UNIX epoch)
    pub last_access_ns: u64,
    /// Approximate size in bytes
    pub bytes: usize,
}

impl HyperGraph {
    pub fn new() -> Self {
        Self {
            nodes: DashMap::new(),
            edges: DashMap::new(),
            adjacency: DashMap::new(),
            reverse_adjacency: DashMap::new(),
            path_cache: Arc::new(dashmap::DashMap::new()),
            shortest_path_cache: Arc::new(dashmap::DashMap::new()),
            shortest_distance_cache: Arc::new(dashmap::DashMap::new()),
            table_column_map: DashMap::new(),
            table_index: DashMap::new(),
            fragment_stats: DashMap::new(),
            next_node_id: std::sync::atomic::AtomicU64::new(1),
            next_edge_id: std::sync::atomic::AtomicU64::new(1),
            next_path_id: std::sync::atomic::AtomicU64::new(1),
            graph_version: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    /// Add a node to the graph
    /// Schema is required - must be provided when creating the node
    pub fn add_node(&self, node: HyperNode) -> NodeId {
        let id = node.id;
        let schema_name = &node.schema_name; // Schema is now always present
        
        // Index by (schema, table) for table nodes
        if matches!(node.node_type, super::node::NodeType::Table) {
            if let Some(table) = &node.table_name {
                let key = (schema_name.to_lowercase(), table.to_lowercase());
                self.table_index.insert(key, id);
            }
        }
        
        // Index by (schema, table, column) for column nodes  
        if let (Some(table), Some(col)) = (node.table_name.clone(), node.column_name.clone()) {
            let key = (schema_name.to_lowercase(), table.to_lowercase(), col.to_lowercase());
            self.table_column_map.insert(key, id);
        }
        
        self.nodes.insert(id, Arc::new(node));
        id
    }
    
    /// Get table node by table name (searches in "main" schema if not qualified)
    /// Supports both "table" (defaults to "main") and "schema.table" formats
    /// Table name is normalized to lowercase for case-insensitive lookups
    pub fn get_table_node(&self, table_name: &str) -> Option<Arc<HyperNode>> {
        self.get_table_node_by_schema(None, table_name)
    }
    
    /// Get table node by schema and table name
    /// If schema is None, defaults to "main"
    /// Index-only lookup - no fallback iteration needed (because we normalize at add_node)
    pub fn get_table_node_by_schema(&self, schema_name: Option<&str>, table_name: &str) -> Option<Arc<HyperNode>> {
        // Normalize: None schema defaults to "main" (for callers, not storage)
        let schema = schema_name.unwrap_or("main").to_lowercase();
        let table = table_name.to_lowercase();
        let key = (schema, table);
        
        // Index-only lookup - no fallback iteration needed
        // If it's not indexed, it doesn't exist (because we normalize at add_node)
        if let Some(node_id) = self.table_index.get(&key) {
            return self.get_node(*node_id.value());
        }
        
        None
    }
    
    /// Get table node by table name, searching all schemas
    /// This is used when schema is not specified and we need to search across all schemas
    /// Returns the first matching table found (prefers "main" schema if multiple exist)
    /// Eliminates circular calls and handles ambiguity deterministically
    pub fn get_table_node_any_schema(&self, table_name: &str) -> Option<Arc<HyperNode>> {
        let table = table_name.to_lowercase();
        
        // Search index for all schemas containing this table name
        // Both t and table are already lowercase (normalized at insertion and start of function)
        let matches: Vec<_> = self.table_index
            .iter()
            .filter(|entry| {
                let ((_schema, t), _) = (entry.key(), entry.value());
                t == &table
            })
            .map(|entry| *entry.value())
            .collect();
        
        match matches.len() {
            0 => None,
            1 => self.get_node(matches[0]),
            _ => {
                // Ambiguous - multiple schemas have same table name
                // Prefer "main" schema if it exists, otherwise return first match
                // This matches ANSI SQL behavior
                if let Some(main_id) = self.table_index.get(&("main".to_string(), table.clone())) {
                    self.get_node(*main_id.value())
                } else {
                    self.get_node(matches[0])
                }
            }
        }
    }
    
    /// Rebuild the table_index by scanning all nodes
    /// This is useful after loading nodes from persistent storage
    /// Schema is required on all nodes
    pub fn rebuild_table_index(&self) {
        self.table_index.clear();
        self.table_column_map.clear();
        
        for (_, node) in self.iter_nodes() {
            // Schema is always present (required)
            let schema_name = node.schema_name.to_lowercase();
            
            if matches!(node.node_type, super::node::NodeType::Table) {
                if let Some(table) = &node.table_name {
                    let key = (schema_name.clone(), table.to_lowercase());
                    self.table_index.insert(key, node.id);
                }
            }
            
            // Rebuild column map too
            if let (Some(table), Some(col)) = (node.table_name.clone(), node.column_name.clone()) {
                let key = (schema_name.clone(), table.to_lowercase(), col.to_lowercase());
                self.table_column_map.insert(key, node.id);
            }
        }
    }
    
    /// Get column nodes for a table (searches in "main" schema if not qualified)
    pub fn get_column_nodes(&self, table_name: &str) -> Vec<Arc<HyperNode>> {
        self.get_column_nodes_by_schema(None, table_name)
    }
    
    /// Get column nodes for a table by schema
    pub fn get_column_nodes_by_schema(&self, schema_name: Option<&str>, table_name: &str) -> Vec<Arc<HyperNode>> {
        let mut result = Vec::new();
        let schema = schema_name.unwrap_or("main").to_lowercase();
        let normalized_table = table_name.to_lowercase();
        // Use table_column_map to find all columns for this table
        for entry in self.table_column_map.iter() {
            let ((s, table, _), node_id) = (entry.key(), entry.value());
            // Normalize comparison for case-insensitive matching
            if s.to_lowercase() == schema && table.to_lowercase() == normalized_table {
                if let Some(node) = self.get_node(*node_id) {
                    if matches!(node.node_type, super::node::NodeType::Column) {
                        result.push(node);
                    }
                }
            }
        }
        result
    }
    
    /// Get a node by ID
    pub fn get_node(&self, id: NodeId) -> Option<Arc<HyperNode>> {
        self.nodes.get(&id).map(|entry| entry.clone())
    }
    
    /// Get node by table and column name (searches in "main" schema if not qualified)
    pub fn get_node_by_table_column(&self, table: &str, column: &str) -> Option<Arc<HyperNode>> {
        self.get_node_by_schema_table_column(None, table, column)
    }
    
    /// Get node by schema, table and column name
    pub fn get_node_by_schema_table_column(&self, schema_name: Option<&str>, table: &str, column: &str) -> Option<Arc<HyperNode>> {
        let schema = schema_name.unwrap_or("main").to_lowercase();
        let key = (schema, table.to_lowercase(), column.to_lowercase());
        let id = self.table_column_map.get(&key)?;
        self.get_node(*id.value())
    }
    
    /// Update table_column_map entries for a renamed table
    /// Returns list of (old_key, new_key, node_id) tuples that were updated
    pub fn update_table_column_map_for_rename(
        &self,
        old_schema: &str,
        old_table: &str,
        new_schema: &str,
        new_table: &str,
    ) -> Vec<((String, String, String), (String, String, String), NodeId)> {
        let old_schema_lower = old_schema.to_lowercase();
        let old_table_lower = old_table.to_lowercase();
        let new_schema_lower = new_schema.to_lowercase();
        let new_table_lower = new_table.to_lowercase();
        
        let mut updates = Vec::new();
        let mut entries_to_update = Vec::new();
        
        // Collect entries to update
        for entry in self.table_column_map.iter() {
            let ((s, t, c), node_id) = (entry.key(), entry.value());
            if s.to_lowercase() == old_schema_lower && t.to_lowercase() == old_table_lower {
                entries_to_update.push((c.clone(), *node_id));
            }
        }
        
        // Remove old entries and add new entries
        for (col, node_id) in entries_to_update {
            let old_key = (old_schema_lower.clone(), old_table_lower.clone(), col.to_lowercase());
            let new_key = (new_schema_lower.clone(), new_table_lower.clone(), col.to_lowercase());
            if let Some(_) = self.table_column_map.remove(&old_key) {
                self.table_column_map.insert(new_key.clone(), node_id);
                updates.push((old_key, new_key, node_id));
            }
        }
        
        updates
    }
    
    /// Record access to a fragment (for hot-fragment statistics)
    pub fn record_fragment_access(&self, node_id: NodeId, fragment_idx: usize, bytes: usize) {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let key = (node_id, fragment_idx);
        self.fragment_stats
            .entry(key)
            .and_modify(|stats| {
                stats.access_count += 1;
                stats.last_access_ns = now_ns;
                stats.bytes = bytes;
            })
            .or_insert(FragmentStats {
                access_count: 1,
                last_access_ns: now_ns,
                bytes,
            });
    }
    
    /// Get statistics for a specific fragment
    pub fn get_fragment_stats(&self, node_id: NodeId, fragment_idx: usize) -> Option<FragmentStats> {
        self.fragment_stats.get(&(node_id, fragment_idx)).map(|e| e.clone())
    }
    
    /// Get top-N hottest fragments by access_count
    pub fn hot_fragments(&self, top_n: usize) -> Vec<((NodeId, usize), FragmentStats)> {
        let mut entries: Vec<_> = self.fragment_stats.iter().map(|e| (*e.key(), e.value().clone())).collect();
        entries.sort_by_key(|(_, stats)| std::cmp::Reverse(stats.access_count));
        entries.truncate(top_n);
        entries
    }
    
    /// Add an edge to the graph
    pub fn add_edge(&self, edge: HyperEdge) -> EdgeId {
        let id = edge.id;
        let source = edge.source;
        let target = edge.target;
        
        self.edges.insert(id, Arc::new(edge));
        
        // Invalidate shortest path cache (graph structure changed)
        self.invalidate_shortest_paths();
        
        // Update adjacency lists
        self.adjacency
            .entry(source)
            .or_insert_with(Vec::new)
            .push(id);
        
        self.reverse_adjacency
            .entry(target)
            .or_insert_with(Vec::new)
            .push(id);
        
        id
    }
    
    /// Get an edge by ID
    pub fn get_edge(&self, id: EdgeId) -> Option<Arc<HyperEdge>> {
        self.edges.get(&id).map(|entry| entry.clone())
    }
    
    /// Get outgoing edges from a node
    pub fn get_outgoing_edges(&self, node_id: NodeId) -> Vec<Arc<HyperEdge>> {
        self.adjacency
            .get(&node_id)
            .map(|entry| {
                entry
                    .iter()
                    .filter_map(|edge_id| self.get_edge(*edge_id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get incoming edges to a node
    pub fn get_incoming_edges(&self, node_id: NodeId) -> Vec<Arc<HyperEdge>> {
        self.reverse_adjacency
            .get(&node_id)
            .map(|entry| {
                entry
                    .iter()
                    .filter_map(|edge_id| self.get_edge(*edge_id))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Find shortest path between nodes
    /// Uses pre-computed cache for O(1) lookup if available, otherwise falls back to BFS
    pub fn find_path(&self, start: NodeId, end: NodeId) -> Option<Vec<EdgeId>> {
        // Try O(1) lookup from cache first
        if let Some(entry) = self.shortest_path_cache.get(&(start, end)) {
            return Some(entry.value().clone());
        }
        
        // Fallback to BFS if cache not computed
        self.find_path_bfs(start, end)
    }
    
    /// Find path using BFS (fallback when cache not available)
    fn find_path_bfs(&self, start: NodeId, end: NodeId) -> Option<Vec<EdgeId>> {
        use std::collections::{VecDeque, HashSet};
        
        let mut queue = VecDeque::new();
        let mut visited = HashSet::new();
        let mut parent = HashMap::new();
        
        queue.push_back(start);
        visited.insert(start);
        
        while let Some(current) = queue.pop_front() {
            if current == end {
                // Reconstruct path
                let mut path = vec![];
                let mut node = end;
                while let Some((prev_node, edge_id)) = parent.get(&node) {
                    path.push(*edge_id);
                    node = *prev_node;
                    if node == start {
                        break;
                    }
                }
                path.reverse();
                return Some(path);
            }
            
            for edge in self.get_outgoing_edges(current) {
                let next = edge.target;
                if !visited.contains(&next) {
                    visited.insert(next);
                    parent.insert(next, (current, edge.id));
                    queue.push_back(next);
                }
            }
        }
        
        None
    }
    
    /// Get shortest path distance (O(1) lookup)
    pub fn get_path_distance(&self, from: NodeId, to: NodeId) -> Option<usize> {
        if from == to {
            return Some(0);
        }
        self.shortest_distance_cache.get(&(from, to))
            .map(|entry| *entry.value())
    }
    
    /// Pre-compute all-pairs shortest paths for O(1) lookups
    /// Call this after graph construction or significant changes
    /// Uses BFS from each node (efficient for sparse graphs)
    pub fn compute_all_shortest_paths(&self) {
        use std::collections::{VecDeque, HashSet};
        
        // Clear existing cache
        self.shortest_path_cache.clear();
        self.shortest_distance_cache.clear();
        
        // Collect all node IDs
        let nodes: Vec<NodeId> = self.nodes.iter()
            .map(|entry| *entry.key())
            .collect();
        
        // For each node, run BFS to find shortest paths to all reachable nodes
        for &start in &nodes {
            let mut queue = VecDeque::new();
            let mut visited = HashSet::new();
            let mut parent_edge = HashMap::new(); // node -> (parent_node, edge_id)
            let mut distances = HashMap::new();
            
            queue.push_back(start);
            visited.insert(start);
            distances.insert(start, 0);
            
            while let Some(current) = queue.pop_front() {
                let current_dist = distances[&current];
                
                // Explore neighbors
                for edge in self.get_outgoing_edges(current) {
                    let neighbor = edge.target;
                    
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        distances.insert(neighbor, current_dist + 1);
                        parent_edge.insert(neighbor, (current, edge.id));
                        queue.push_back(neighbor);
                    } else if let Some(&existing_dist) = distances.get(&neighbor) {
                        // Check if we found a shorter path (shouldn't happen in BFS, but just in case)
                        if current_dist + 1 < existing_dist {
                            distances.insert(neighbor, current_dist + 1);
                            parent_edge.insert(neighbor, (current, edge.id));
                        }
                    }
                }
            }
            
            // Reconstruct and store paths for all reachable nodes
            for &target in &visited {
                if target == start {
                    continue; // Skip self
                }
                
                // Reconstruct path
                let mut path = Vec::new();
                let mut node = target;
                
                while let Some((parent, edge_id)) = parent_edge.get(&node) {
                    path.push(*edge_id);
                    if *parent == start {
                        break;
                    }
                    node = *parent;
                }
                
                path.reverse();
                
                // Store path (O(1) insertion)
                self.shortest_path_cache.insert((start, target), path);
                
                // Store distance (O(1) insertion)
                if let Some(&dist) = distances.get(&target) {
                    self.shortest_distance_cache.insert((start, target), dist);
                }
            }
        }
        
        // Increment graph version
        self.graph_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Invalidate shortest path cache (call when graph structure changes)
    pub fn invalidate_shortest_paths(&self) {
        self.shortest_path_cache.clear();
        self.shortest_distance_cache.clear();
        self.graph_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Get graph version (for cache invalidation)
    pub fn get_version(&self) -> u64 {
        self.graph_version.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    /// Add a path to the cache
    pub fn cache_path(&self, signature: PathSignature, path: HyperPath) {
        self.path_cache.insert(signature, path);
    }
    
    /// Get a cached path
    pub fn get_cached_path(&self, signature: &PathSignature) -> Option<HyperPath> {
        self.path_cache.get(signature).map(|entry| entry.clone())
    }
    
    /// Get all edges in the graph (for introspection)
    pub fn get_all_edges(&self) -> Vec<Arc<HyperEdge>> {
        self.edges.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    /// Get all cached paths (for introspection)
    pub fn get_all_paths(&self) -> Vec<(PathSignature, HyperPath)> {
        self.path_cache.iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }
    
    /// Clean up old path cache entries
    /// Removes paths that haven't been used in max_age_seconds
    pub fn cleanup_path_cache(&self, max_age_seconds: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Collect expired paths
        let expired_signatures: Vec<PathSignature> = self.path_cache
            .iter()
            .filter(|entry| {
                let path = entry.value();
                let age = now.saturating_sub(path.last_used);
                age > max_age_seconds
            })
            .map(|entry| entry.key().clone())
            .collect();
        
        // Remove expired paths
        for signature in expired_signatures {
            self.path_cache.remove(&signature);
        }
    }
    
    /// Generate next node ID
    pub fn next_node_id(&self) -> NodeId {
        NodeId(self.next_node_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
    
    /// Generate next edge ID
    pub fn next_edge_id(&self) -> EdgeId {
        EdgeId(self.next_edge_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
    
    /// Generate next path ID
    pub fn next_path_id(&self) -> PathId {
        PathId(self.next_path_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
    
    /// Update a node's fragments (for incremental updates)
    pub fn update_node_fragments(&self, node_id: NodeId, fragments: Vec<ColumnFragment>) {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            let mut new_node = (**node).clone();
            new_node.fragments = fragments;
            new_node.update_stats();
            *node = Arc::new(new_node);
        }
    }
    
    /// Update a node's metadata (for schema and alias storage)
    pub fn update_node_metadata(&self, node_id: NodeId, metadata_updates: HashMap<String, String>) {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            let mut new_node = (**node).clone();
            for (key, value) in metadata_updates {
                new_node.metadata.insert(key, value);
            }
            *node = Arc::new(new_node);
        }
    }
    
    /// Update an edge's metadata (for pattern insights)
    pub fn update_edge_metadata(&self, edge_id: EdgeId, metadata_updates: HashMap<String, String>) {
        if let Some(mut edge) = self.edges.get_mut(&edge_id) {
            let mut new_edge = (**edge).clone();
            for (key, value) in metadata_updates {
                new_edge.metadata.insert(key, value);
            }
            *edge = Arc::new(new_edge);
        }
    }
    
    /// Find edge between two nodes (bidirectional)
    pub fn find_edge_between(&self, node1: NodeId, node2: NodeId) -> Option<EdgeId> {
        // Check outgoing edges from node1
        if let Some(adj_list) = self.adjacency.get(&node1) {
            for &edge_id in adj_list.value() {
                if let Some(edge) = self.edges.get(&edge_id) {
                    if edge.target == node2 {
                        return Some(edge_id);
                    }
                }
            }
        }
        
        // Check outgoing edges from node2 (bidirectional)
        if let Some(adj_list) = self.adjacency.get(&node2) {
            for &edge_id in adj_list.value() {
                if let Some(edge) = self.edges.get(&edge_id) {
                    if edge.target == node1 {
                        return Some(edge_id);
                    }
                }
            }
        }
        
        None
    }
    
    /// Get table aliases from node metadata (alias -> table name)
    pub fn get_table_aliases_from_metadata(&self, table_name: &str) -> HashMap<String, String> {
        if let Some(table_node) = self.get_table_node(table_name) {
            table_node.metadata.get("table_aliases")
                .and_then(|s| serde_json::from_str::<HashMap<String, String>>(s).ok())
                .unwrap_or_default()
        } else {
            HashMap::new()
        }
    }
    
    /// Get alias names for a table from node metadata
    pub fn get_alias_names_from_metadata(&self, table_name: &str) -> Vec<String> {
        if let Some(table_node) = self.get_table_node(table_name) {
            table_node.metadata.get("alias_names")
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    }
    
    /// Resolve alias to actual table name using stored metadata
    pub fn resolve_alias_to_table(&self, alias: &str) -> Option<String> {
        // Try to find the table node that has this alias in its metadata
        for node_entry in self.nodes.iter() {
            let node = node_entry.value();
            if matches!(node.node_type, super::node::NodeType::Table) {
                if let Some(aliases_json) = node.metadata.get("table_aliases") {
                    if let Ok(aliases) = serde_json::from_str::<HashMap<String, String>>(aliases_json) {
                        if aliases.contains_key(alias) {
                            return aliases.get(alias).cloned();
                        }
                    }
                }
            }
        }
        None
    }
    
    /// Record access to a node (for access pattern tracking)
    pub fn record_node_access(&self, node_id: NodeId, is_write: bool, query_time_ms: f64) {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            let mut new_node = (**node).clone();
            new_node.stats.record_access(is_write, query_time_ms);
            *node = Arc::new(new_node);
        }
    }
    
    /// Record access to a table by name
    pub fn record_table_access(&self, table_name: &str, is_write: bool, query_time_ms: f64) {
        if let Some(table_node) = self.get_table_node(table_name) {
            self.record_node_access(table_node.id, is_write, query_time_ms);
        }
    }
    
    /// Update filter selectivity for a column node
    pub fn update_filter_selectivity(&self, table_name: &str, column_name: &str, selectivity: f64) {
        if let Some(column_node) = self.get_node_by_table_column(table_name, column_name) {
            if let Some(mut node) = self.nodes.get_mut(&column_node.id) {
                let mut new_node = (**node).clone();
                new_node.stats.update_filter_selectivity(selectivity);
                *node = Arc::new(new_node);
            }
        }
    }
    
    /// Update join selectivity for a column node
    pub fn update_join_selectivity(&self, table_name: &str, column_name: &str, selectivity: f64) {
        if let Some(column_node) = self.get_node_by_table_column(table_name, column_name) {
            if let Some(mut node) = self.nodes.get_mut(&column_node.id) {
                let mut new_node = (**node).clone();
                new_node.stats.update_join_selectivity(selectivity);
                *node = Arc::new(new_node);
            }
        }
    }
    
    /// Record join execution and update edge statistics
    pub fn record_join_execution(&self, edge_id: EdgeId, actual_cost_ms: f64, actual_cardinality: usize, result_size: usize) {
        if let Some(mut edge) = self.edges.get_mut(&edge_id) {
            let mut new_edge = (**edge).clone();
            new_edge.record_join_execution(actual_cost_ms, actual_cardinality, result_size);
            *edge = Arc::new(new_edge);
        }
    }
    
    /// Find edge by predicate and record join execution
    pub fn record_join_execution_by_predicate(&self, left_table: &str, left_col: &str, right_table: &str, right_col: &str, actual_cost_ms: f64, actual_cardinality: usize, result_size: usize) {
        for edge_entry in self.edges.iter() {
            let edge = edge_entry.value();
            if edge.matches_predicate(left_table, left_col, right_table, right_col) {
                self.record_join_execution(edge.id, actual_cost_ms, actual_cardinality, result_size);
                break;
            }
        }
    }
    
    /// Remove a node from the graph (for DROP TABLE)
    /// Also removes all related edges and updates indexes
    pub fn remove_node(&self, node_id: NodeId) -> anyhow::Result<()> {
        // Get the node first to extract metadata
        let node = if let Some(n) = self.nodes.get(&node_id) {
            n.clone()
        } else {
            return Ok(()); // Node doesn't exist, nothing to remove
        };
        
        // Remove from table_index if it's a table node (schema is required)
        if matches!(node.node_type, super::node::NodeType::Table) {
            if let Some(table_name) = &node.table_name {
                let schema_name = node.schema_name.to_lowercase();
                let table_key = (schema_name, table_name.to_lowercase());
                self.table_index.remove(&table_key);
            }
        }
        
        // Remove from table_column_map if it's a column node (schema is required)
        if let (Some(table_name), Some(column_name)) = (&node.table_name, &node.column_name) {
            let schema_name = node.schema_name.to_lowercase();
            let column_key = (schema_name, table_name.to_lowercase(), column_name.to_lowercase());
            self.table_column_map.remove(&column_key);
        }
        
        // Remove all edges connected to this node
        let outgoing_edges: Vec<EdgeId> = self.adjacency.get(&node_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        let incoming_edges: Vec<EdgeId> = self.reverse_adjacency.get(&node_id)
            .map(|entry| entry.value().clone())
            .unwrap_or_default();
        
        // Remove edges from edge map
        for edge_id in outgoing_edges.iter().chain(incoming_edges.iter()) {
            if let Some(edge) = self.edges.get(edge_id) {
                // Remove from adjacency lists of connected nodes
                if let Some(mut adj_list) = self.adjacency.get_mut(&edge.source) {
                    adj_list.retain(|&id| id != *edge_id);
                }
                if let Some(mut adj_list) = self.reverse_adjacency.get_mut(&edge.target) {
                    adj_list.retain(|&id| id != *edge_id);
                }
            }
            self.edges.remove(edge_id);
        }
        
        // Remove adjacency list entries for this node
        self.adjacency.remove(&node_id);
        self.reverse_adjacency.remove(&node_id);
        
        // Remove the node itself
        self.nodes.remove(&node_id);
        
        // If this is a table node, also remove all column nodes for this table
        if matches!(node.node_type, super::node::NodeType::Table) {
            if let Some(table_name) = &node.table_name {
                let column_nodes: Vec<NodeId> = self.get_column_nodes(table_name)
                    .iter()
                    .map(|n| n.id)
                    .collect();
                
                for col_node_id in column_nodes {
                    self.remove_node(col_node_id)?;
                }
            }
        }
        
        // Remove fragment statistics
        let fragment_keys: Vec<(NodeId, usize)> = self.fragment_stats.iter()
            .filter(|entry| entry.key().0 == node_id)
            .map(|entry| *entry.key())
            .collect();
        for key in fragment_keys {
            self.fragment_stats.remove(&key);
        }
        
        // Invalidate shortest path cache (graph structure changed)
        self.invalidate_shortest_paths();
        
        Ok(())
    }
    
    /// Iterate over all nodes (for DROP TABLE and other operations)
    pub fn iter_nodes(&self) -> impl Iterator<Item = (NodeId, Arc<HyperNode>)> + '_ {
        self.nodes.iter().map(|entry| (*entry.key(), entry.value().clone()))
    }
    
    /// Iterate over all edges (for coarsening/compression)
    pub fn iter_edges(&self) -> impl Iterator<Item = (EdgeId, Arc<HyperEdge>)> + '_ {
        self.edges.iter().map(|entry| (*entry.key(), entry.value().clone()))
    }
    
    /// Get node count
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
    
    /// Get edge count
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

impl Clone for HyperGraph {
    fn clone(&self) -> Self {
        // Create a new graph and copy all nodes and edges
        let mut new_graph = Self::new();
        
        // Copy nodes
        for entry in self.nodes.iter() {
            new_graph.nodes.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy edges
        for entry in self.edges.iter() {
            new_graph.edges.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy adjacency lists
        for entry in self.adjacency.iter() {
            new_graph.adjacency.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy reverse adjacency
        for entry in self.reverse_adjacency.iter() {
            new_graph.reverse_adjacency.insert(*entry.key(), entry.value().clone());
        }
        
        // Copy table column map
        for entry in self.table_column_map.iter() {
            new_graph.table_column_map.insert(entry.key().clone(), *entry.value());
        }
        
        // Copy table index
        for entry in self.table_index.iter() {
            new_graph.table_index.insert(entry.key().clone(), *entry.value());
        }
        
        // Share path cache
        new_graph.path_cache = self.path_cache.clone();
        
        // Copy atomic counters
        let node_id = self.next_node_id.load(std::sync::atomic::Ordering::SeqCst);
        new_graph.next_node_id.store(node_id, std::sync::atomic::Ordering::SeqCst);
        
        let edge_id = self.next_edge_id.load(std::sync::atomic::Ordering::SeqCst);
        new_graph.next_edge_id.store(edge_id, std::sync::atomic::Ordering::SeqCst);
        
        let path_id = self.next_path_id.load(std::sync::atomic::Ordering::SeqCst);
        new_graph.next_path_id.store(path_id, std::sync::atomic::Ordering::SeqCst);
        
        // Rebuild indexes to ensure consistency (though we've copied them above)
        // This is a safety measure in case nodes were added after indexes were built
        new_graph.rebuild_table_index();
        
        new_graph
    }
}

impl Default for HyperGraph {
    fn default() -> Self {
        Self::new()
    }
}

