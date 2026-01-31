//! # Hypergraph Module
//! 
//! A standalone hypergraph data structure module for representing database schemas,
//! tables, columns, and their relationships as a graph.
//! 
//! ## Features
//! 
//! - **Nodes**: Represent tables and columns with statistics and metadata
//! - **Edges**: Represent join relationships between tables/columns
//! - **Paths**: Precomputed multi-way joins for query optimization
//! - **Compression**: Compress hypergraph metadata to reduce memory usage
//! - **Coarsening**: Merge similar nodes/edges to reduce graph complexity
//! - **Shortest Path**: Optimized path finding with O(1) lookups
//! 
//! ## Usage
//! 
//! ```rust,no_run
//! use hypergraph::HyperGraph;
//! use hypergraph::node::{HyperNode, NodeId, NodeType};
//! use hypergraph::edge::{HyperEdge, EdgeId, JoinType, JoinPredicate, PredicateOperator};
//! 
//! // Create a new hypergraph
//! let graph = HyperGraph::new();
//! 
//! // Create a table node
//! let table_id = graph.next_node_id();
//! let table_node = HyperNode::new_table(table_id, "main".to_string(), "employees".to_string());
//! graph.add_node(table_node);
//! 
//! // Create a column node
//! let col_id = graph.next_node_id();
//! let col_node = HyperNode::new_column(col_id, "main".to_string(), "employees".to_string(), "id".to_string());
//! graph.add_node(col_node);
//! 
//! // Create an edge (join relationship)
//! let edge_id = graph.next_edge_id();
//! let predicate = JoinPredicate {
//!     left: ("employees".to_string(), "id".to_string()),
//!     right: ("departments".to_string(), "employee_id".to_string()),
//!     operator: PredicateOperator::Equals,
//! };
//! let edge = HyperEdge::new(edge_id, table_id, col_id, JoinType::Inner, predicate);
//! graph.add_edge(edge);
//! ```

pub mod types;
pub mod node;
pub mod edge;
pub mod path;
pub mod graph;
pub mod compression;
pub mod coarsening;
pub mod shortest_path;

// Re-export main types for convenience
pub use graph::HyperGraph;
pub use graph::FragmentStats;
pub use node::{HyperNode, NodeId, NodeType, NodeStatistics, Percentiles, TopNValue};
pub use edge::{HyperEdge, EdgeId, JoinType, JoinPredicate, PredicateOperator, EdgeStatistics, JoinAlgorithmHint, MaterializedJoin};
pub use path::{HyperPath, PathId, PathSignature, PathStatistics, PathCache};
pub use compression::{HypergraphCompressor, CompressedHyperGraph, DictionaryCompressor, FragmentCompressor, DeltaCompressor, DeltaEntry};
pub use coarsening::{HypergraphCoarsener, CoarsenedHyperGraph};
pub use shortest_path::{ShortestPathCache, ShortestPathComputer, OptimizedPathFinder};
pub use types::{Value, ColumnFragment, FragmentMetadata};

