///! Node Registry Module - Unified registration system for RCA Engine
///! 
///! Architecture:
///! - Node: Registered entity with a reference ID
///! - Knowledge Register: Human-readable information stored in pages (reference ID = page ID)
///! - Metadata Register: Technical metadata stored in pages (reference ID = page ID)
///! 
///! Each entity (column description, etc.) is stored in reserved segments
///! Example: Column descriptions lie between child_ref_id_1 and child_ref_id_2
///! 
///! LLM Search Flow:
///! 1. Grep search term in Knowledge Register
///! 2. Get matching pages (reference IDs)
///! 3. These reference IDs map to Nodes and Metadata
///! 4. This narrows down the universe for RCA analysis

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use crate::optimized_search::OptimizedSearch;

/// Reserved segment IDs for different entity types
/// These define ranges where specific information is stored
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReservedSegment {
    /// Column descriptions: 1000-1999
    ColumnDescriptions,
    /// Table descriptions: 2000-2999
    TableDescriptions,
    /// Business rules: 3000-3999
    BusinessRules,
    /// Relationships: 4000-4999
    Relationships,
    /// Join paths: 5000-5999
    JoinPaths,
    /// Statistics: 6000-6999
    Statistics,
    /// Custom segment: 7000+
    Custom(u64),
}

impl ReservedSegment {
    /// Get the start ID for this segment
    pub fn start_id(&self) -> u64 {
        match self {
            ReservedSegment::ColumnDescriptions => 1000,
            ReservedSegment::TableDescriptions => 2000,
            ReservedSegment::BusinessRules => 3000,
            ReservedSegment::Relationships => 4000,
            ReservedSegment::JoinPaths => 5000,
            ReservedSegment::Statistics => 6000,
            ReservedSegment::Custom(start) => *start,
        }
    }
    
    /// Get the end ID for this segment
    pub fn end_id(&self) -> u64 {
        match self {
            ReservedSegment::ColumnDescriptions => 1999,
            ReservedSegment::TableDescriptions => 2999,
            ReservedSegment::BusinessRules => 3999,
            ReservedSegment::Relationships => 4999,
            ReservedSegment::JoinPaths => 5999,
            ReservedSegment::Statistics => 6999,
            ReservedSegment::Custom(start) => start + 999,
        }
    }
    
    /// Check if a child ref ID falls within this segment
    pub fn contains(&self, child_ref_id: u64) -> bool {
        child_ref_id >= self.start_id() && child_ref_id <= self.end_id()
    }
}

/// A Node represents a registered entity (table, metric, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique reference ID (also used as page ID in both registers)
    pub ref_id: String,
    
    /// Node type (table, metric, entity, etc.)
    pub node_type: String,
    
    /// Node name
    pub name: String,
    
    /// Timestamp when node was created
    pub created_at: String,
    
    /// Additional node metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

/// A page in the Knowledge Register (human-readable information)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgePage {
    /// Page ID (same as Node ref_id)
    pub page_id: String,
    
    /// Node reference ID
    pub node_ref_id: String,
    
    /// Content segments organized by reserved segment ranges
    /// Key: (start_child_ref_id, end_child_ref_id)
    /// Value: Content stored in that segment
    pub segments: HashMap<String, KnowledgeSegment>,
    
    /// Full text content for search (all segments combined)
    pub full_text: String,
    
    /// Keywords extracted for search
    pub keywords: Vec<String>,
}

/// A segment within a Knowledge Page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeSegment {
    /// Segment identifier (e.g., "column_descriptions_1000_1999")
    pub segment_id: String,
    
    /// Reserved segment type
    pub segment_type: ReservedSegment,
    
    /// Start child ref ID
    pub start_child_ref_id: u64,
    
    /// End child ref ID
    pub end_child_ref_id: u64,
    
    /// Content stored in this segment
    pub content: HashMap<String, serde_json::Value>,
    
    /// Text content for this segment
    pub text_content: String,
}

/// A page in the Metadata Register (technical metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataPage {
    /// Page ID (same as Node ref_id)
    pub page_id: String,
    
    /// Node reference ID
    pub node_ref_id: String,
    
    /// Content segments organized by reserved segment ranges
    pub segments: HashMap<String, MetadataSegment>,
    
    /// Technical metadata (stats, join paths, etc.)
    pub technical_data: HashMap<String, serde_json::Value>,
}

/// A segment within a Metadata Page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSegment {
    /// Segment identifier
    pub segment_id: String,
    
    /// Reserved segment type
    pub segment_type: ReservedSegment,
    
    /// Start child ref ID
    pub start_child_ref_id: u64,
    
    /// End child ref ID
    pub end_child_ref_id: u64,
    
    /// Technical data stored in this segment
    pub data: HashMap<String, serde_json::Value>,
}

/// Knowledge Register - stores human-readable information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeRegister {
    /// All pages indexed by page_id (which equals node ref_id)
    pub pages: HashMap<String, KnowledgePage>,
    
    /// Full-text search index: keyword -> list of page_ids
    pub search_index: HashMap<String, Vec<String>>,
}

/// Metadata Register - stores technical metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataRegister {
    /// All pages indexed by page_id (which equals node ref_id)
    pub pages: HashMap<String, MetadataPage>,
}

/// Node Registry - manages Nodes, Knowledge Register, and Metadata Register
#[derive(Debug)]
pub struct NodeRegistry {
    /// All registered nodes
    pub nodes: HashMap<String, Node>,
    
    /// Knowledge Register
    pub knowledge_register: KnowledgeRegister,
    
    /// Metadata Register
    pub metadata_register: MetadataRegister,
    
    /// Counter for generating child ref IDs within segments
    segment_counters: HashMap<ReservedSegment, u64>,
    
    /// Optimized search engine for fast text search
    search_engine: Arc<Mutex<OptimizedSearch>>,
}

impl NodeRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        let mut registry = Self {
            nodes: HashMap::new(),
            knowledge_register: KnowledgeRegister {
                pages: HashMap::new(),
                search_index: HashMap::new(),
            },
            metadata_register: MetadataRegister {
                pages: HashMap::new(),
            },
            segment_counters: HashMap::new(),
            search_engine: Arc::new(Mutex::new(OptimizedSearch::new())),
        };
        
        // Index existing pages
        registry.rebuild_search_index();
        registry
    }
    
    /// Load registry from JSON files
    pub fn load(base_path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let base_path = base_path.as_ref();
        
        // Load nodes
        let nodes_path = base_path.join("nodes.json");
        let nodes: HashMap<String, Node> = if nodes_path.exists() {
            let content = std::fs::read_to_string(&nodes_path)?;
            serde_json::from_str(&content)?
        } else {
            HashMap::new()
        };
        
        // Load knowledge register
        let knowledge_path = base_path.join("knowledge_register.json");
        let knowledge_register: KnowledgeRegister = if knowledge_path.exists() {
            let content = std::fs::read_to_string(&knowledge_path)?;
            serde_json::from_str(&content)?
        } else {
            KnowledgeRegister {
                pages: HashMap::new(),
                search_index: HashMap::new(),
            }
        };
        
        // Load metadata register
        let metadata_path = base_path.join("metadata_register.json");
        let metadata_register: MetadataRegister = if metadata_path.exists() {
            let content = std::fs::read_to_string(&metadata_path)?;
            serde_json::from_str(&content)?
        } else {
            MetadataRegister {
                pages: HashMap::new(),
            }
        };
        
        // Initialize segment counters based on existing data
        let mut segment_counters = HashMap::new();
        for page in knowledge_register.pages.values() {
            for segment in page.segments.values() {
                let counter = segment_counters
                    .entry(segment.segment_type)
                    .or_insert(segment.start_child_ref_id);
                *counter = (*counter).max(segment.end_child_ref_id);
            }
        }
        
        let mut registry = Self {
            nodes,
            knowledge_register,
            metadata_register,
            segment_counters,
            search_engine: Arc::new(Mutex::new(OptimizedSearch::new())),
        };
        
        // Index all pages in optimized search engine
        registry.rebuild_search_index();
        
        Ok(registry)
    }
    
    /// Save registry to JSON files
    pub fn save(&self, base_path: impl AsRef<Path>) -> Result<(), Box<dyn std::error::Error>> {
        let base_path = base_path.as_ref();
        std::fs::create_dir_all(base_path)?;
        
        // Save nodes
        let nodes_path = base_path.join("nodes.json");
        let nodes_json = serde_json::to_string_pretty(&self.nodes)?;
        std::fs::write(&nodes_path, nodes_json)?;
        
        // Save knowledge register
        let knowledge_path = base_path.join("knowledge_register.json");
        let knowledge_json = serde_json::to_string_pretty(&self.knowledge_register)?;
        std::fs::write(&knowledge_path, knowledge_json)?;
        
        // Save metadata register
        let metadata_path = base_path.join("metadata_register.json");
        let metadata_json = serde_json::to_string_pretty(&self.metadata_register)?;
        std::fs::write(&metadata_path, metadata_json)?;
        
        Ok(())
    }
    
    /// Register a new table (creates Node and pages in both registers)
    pub fn register_table(
        &mut self,
        table_name: String,
        csv_path: PathBuf,
        primary_keys: Vec<String>,
        column_descriptions: HashMap<String, String>,
        table_description: Option<String>,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Generate reference ID for the node
        let ref_id = Uuid::new_v4().to_string();
        
        // Create Node
        let node = Node {
            ref_id: ref_id.clone(),
            node_type: "table".to_string(),
            name: table_name.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("csv_path".to_string(), serde_json::Value::String(csv_path.to_string_lossy().to_string()));
                meta.insert("primary_keys".to_string(), serde_json::to_value(primary_keys.clone())?);
                meta
            },
        };
        
        // Create Knowledge Page
        let knowledge_page = self.create_knowledge_page_for_table(
            &ref_id,
            &table_name,
            &column_descriptions,
            table_description.as_deref(),
        )?;
        
        // Create Metadata Page
        let metadata_page = self.create_metadata_page_for_table(
            &ref_id,
            &table_name,
            &csv_path,
            &primary_keys,
        )?;
        
        // Store everything
        self.nodes.insert(ref_id.clone(), node);
        self.knowledge_register.pages.insert(ref_id.clone(), knowledge_page.clone());
        self.metadata_register.pages.insert(ref_id.clone(), metadata_page);
        
        // Update search index
        let content = format!("{} {}", knowledge_page.full_text, knowledge_page.keywords.join(" "));
        let search_engine = self.search_engine.lock().unwrap();
        search_engine.index_document(ref_id.clone(), content);
        
        Ok(ref_id)
    }
    
    /// Create a Knowledge Page for a table
    fn create_knowledge_page_for_table(
        &mut self,
        ref_id: &str,
        table_name: &str,
        column_descriptions: &HashMap<String, String>,
        table_description: Option<&str>,
    ) -> Result<KnowledgePage, Box<dyn std::error::Error>> {
        let mut segments = HashMap::new();
        let mut full_text_parts = Vec::new();
        let mut keywords = Vec::new();
        
        // Add table name to keywords
        keywords.push(table_name.to_lowercase());
        
        // Segment 1: Table Description (2000-2999)
        if let Some(desc) = table_description {
            let segment_id = format!("table_description_{}", ReservedSegment::TableDescriptions.start_id());
            let start_id = self.get_next_child_ref_id(ReservedSegment::TableDescriptions);
            let end_id = start_id;
            
            let mut content = HashMap::new();
            content.insert("description".to_string(), serde_json::Value::String(desc.to_string()));
            
            segments.insert(segment_id.clone(), KnowledgeSegment {
                segment_id,
                segment_type: ReservedSegment::TableDescriptions,
                start_child_ref_id: start_id,
                end_child_ref_id: end_id,
                content,
                text_content: desc.to_string(),
            });
            
            full_text_parts.push(format!("Table: {} - {}", table_name, desc));
            keywords.extend(extract_keywords(desc));
        }
        
        // Segment 2: Column Descriptions (1000-1999)
        let mut column_segments = HashMap::new();
        let mut column_texts = Vec::new();
        
        for (col_name, col_desc) in column_descriptions {
            let segment_id = format!("column_{}_{}", col_name, ReservedSegment::ColumnDescriptions.start_id());
            let start_id = self.get_next_child_ref_id(ReservedSegment::ColumnDescriptions);
            let end_id = start_id;
            
            let mut content = HashMap::new();
            content.insert("column_name".to_string(), serde_json::Value::String(col_name.clone()));
            content.insert("description".to_string(), serde_json::Value::String(col_desc.clone()));
            
            column_segments.insert(segment_id.clone(), KnowledgeSegment {
                segment_id,
                segment_type: ReservedSegment::ColumnDescriptions,
                start_child_ref_id: start_id,
                end_child_ref_id: end_id,
                content,
                text_content: format!("Column {}: {}", col_name, col_desc),
            });
            
            column_texts.push(format!("Column {}: {}", col_name, col_desc));
            keywords.push(col_name.to_lowercase());
            keywords.extend(extract_keywords(&col_desc));
        }
        
        segments.extend(column_segments);
        full_text_parts.extend(column_texts);
        
        let full_text = full_text_parts.join("\n");
        
        // Update search index
        let mut page = KnowledgePage {
            page_id: ref_id.to_string(),
            node_ref_id: ref_id.to_string(),
            segments,
            full_text: full_text.clone(),
            keywords: keywords.clone(),
        };
        
        // Index keywords
        for keyword in &keywords {
            self.knowledge_register.search_index
                .entry(keyword.clone())
                .or_insert_with(Vec::new)
                .push(ref_id.to_string());
        }
        
        Ok(page)
    }
    
    /// Create a Metadata Page for a table
    fn create_metadata_page_for_table(
        &mut self,
        ref_id: &str,
        table_name: &str,
        csv_path: &PathBuf,
        primary_keys: &[String],
    ) -> Result<MetadataPage, Box<dyn std::error::Error>> {
        let mut segments = HashMap::new();
        let mut technical_data = HashMap::new();
        
        // Store basic table metadata
        technical_data.insert("table_name".to_string(), serde_json::Value::String(table_name.to_string()));
        technical_data.insert("csv_path".to_string(), serde_json::Value::String(csv_path.to_string_lossy().to_string()));
        technical_data.insert("primary_keys".to_string(), serde_json::to_value(primary_keys)?);
        
        // Segment: Statistics (6000-6999) - placeholder for future stats
        let segment_id = format!("statistics_{}", ReservedSegment::Statistics.start_id());
        let start_id = self.get_next_child_ref_id(ReservedSegment::Statistics);
        let end_id = start_id;
        
        let mut stats_data = HashMap::new();
        stats_data.insert("primary_keys".to_string(), serde_json::to_value(primary_keys)?);
        
        segments.insert(segment_id.clone(), MetadataSegment {
            segment_id,
            segment_type: ReservedSegment::Statistics,
            start_child_ref_id: start_id,
            end_child_ref_id: end_id,
            data: stats_data,
        });
        
        Ok(MetadataPage {
            page_id: ref_id.to_string(),
            node_ref_id: ref_id.to_string(),
            segments,
            technical_data,
        })
    }
    
    /// Get next child ref ID within a segment
    fn get_next_child_ref_id(&mut self, segment: ReservedSegment) -> u64 {
        let counter = self.segment_counters.entry(segment).or_insert(segment.start_id());
        let current = *counter;
        
        // Check if we're within bounds
        if current < segment.end_id() {
            *counter += 1;
            current
        } else {
            // Reset to start if we've exceeded (shouldn't happen in practice)
            *counter = segment.start_id();
            segment.start_id()
        }
    }
    
    /// Search Knowledge Register for a term (returns matching page IDs / node ref IDs)
    /// Uses optimized search engine for fast lookup
    pub fn search_knowledge(&self, search_term: &str) -> Vec<String> {
        let search_term_lower = search_term.to_lowercase();
        let mut matching_page_ids = HashSet::new();
        
        // Fast path: Search in search index (exact keyword matches) - O(1)
        if let Some(page_ids) = self.knowledge_register.search_index.get(&search_term_lower) {
            matching_page_ids.extend(page_ids.iter().cloned());
        }
        
        // Optimized search: Use search engine for full-text search
        let search_engine = self.search_engine.lock().unwrap();
        let search_results = search_engine.search(&search_term_lower);
        
        // Extract page IDs from search results
        for result in search_results {
            if let Some(doc_id) = result.metadata.get("doc_id") {
                matching_page_ids.insert(doc_id.clone());
            }
        }
        
        // Fallback: If optimized search didn't find anything, do linear scan
        // (This should rarely happen if index is properly maintained)
        if matching_page_ids.is_empty() {
            for (page_id, page) in &self.knowledge_register.pages {
                // Check if search term appears in full text
                if page.full_text.to_lowercase().contains(&search_term_lower) {
                    matching_page_ids.insert(page_id.clone());
                }
                
                // Check keywords
                for keyword in &page.keywords {
                    if keyword.contains(&search_term_lower) || search_term_lower.contains(keyword) {
                        matching_page_ids.insert(page_id.clone());
                    }
                }
            }
        }
        
        matching_page_ids.into_iter().collect()
    }
    
    /// Rebuild search index (call after bulk updates)
    fn rebuild_search_index(&mut self) {
        let mut search_engine = self.search_engine.lock().unwrap();
        
        // Index all pages
        for (page_id, page) in &self.knowledge_register.pages {
            // Combine full text and keywords for indexing
            let content = format!("{} {}", page.full_text, page.keywords.join(" "));
            search_engine.index_document(page_id.clone(), content);
        }
    }
    
    /// Get Node by reference ID
    pub fn get_node(&self, ref_id: &str) -> Option<&Node> {
        self.nodes.get(ref_id)
    }
    
    /// Get Knowledge Page by reference ID
    pub fn get_knowledge_page(&self, ref_id: &str) -> Option<&KnowledgePage> {
        self.knowledge_register.pages.get(ref_id)
    }
    
    /// Get Metadata Page by reference ID
    pub fn get_metadata_page(&self, ref_id: &str) -> Option<&MetadataPage> {
        self.metadata_register.pages.get(ref_id)
    }
    
    /// Get all nodes matching a search term (LLM search flow)
    /// Returns: (nodes, knowledge_pages, metadata_pages)
    pub fn search_all(&self, search_term: &str) -> (Vec<&Node>, Vec<&KnowledgePage>, Vec<&MetadataPage>) {
        // Step 1: Search Knowledge Register
        let matching_ref_ids = self.search_knowledge(search_term);
        
        // Step 2: Get corresponding Nodes and Metadata
        let mut nodes = Vec::new();
        let mut knowledge_pages = Vec::new();
        let mut metadata_pages = Vec::new();
        
        for ref_id in &matching_ref_ids {
            if let Some(node) = self.get_node(ref_id) {
                nodes.push(node);
            }
            if let Some(knowledge_page) = self.get_knowledge_page(ref_id) {
                knowledge_pages.push(knowledge_page);
            }
            if let Some(metadata_page) = self.get_metadata_page(ref_id) {
                metadata_pages.push(metadata_page);
            }
        }
        
        (nodes, knowledge_pages, metadata_pages)
    }
}

/// Extract keywords from text (simple implementation)
fn extract_keywords(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split_whitespace()
        .filter(|word| word.len() > 3) // Filter out short words
        .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_reserved_segment_ranges() {
        assert_eq!(ReservedSegment::ColumnDescriptions.start_id(), 1000);
        assert_eq!(ReservedSegment::ColumnDescriptions.end_id(), 1999);
        assert!(ReservedSegment::ColumnDescriptions.contains(1500));
        assert!(!ReservedSegment::ColumnDescriptions.contains(2000));
    }
    
    #[test]
    fn test_register_table() {
        let mut registry = NodeRegistry::new();
        
        let mut column_descriptions = HashMap::new();
        column_descriptions.insert("customer_id".to_string(), "Unique identifier for customer".to_string());
        column_descriptions.insert("name".to_string(), "Customer name".to_string());
        
        let ref_id = registry.register_table(
            "khatabook_customers".to_string(),
            PathBuf::from("data/khatabook_customers.csv"),
            vec!["customer_id".to_string()],
            column_descriptions,
            Some("Customer master table from Khatabook system".to_string()),
        ).unwrap();
        
        assert!(!ref_id.is_empty());
        assert!(registry.nodes.contains_key(&ref_id));
        assert!(registry.knowledge_register.pages.contains_key(&ref_id));
        assert!(registry.metadata_register.pages.contains_key(&ref_id));
    }
    
    #[test]
    fn test_search_knowledge() {
        let mut registry = NodeRegistry::new();
        
        let mut column_descriptions = HashMap::new();
        column_descriptions.insert("customer_id".to_string(), "Unique identifier for customer".to_string());
        
        let ref_id = registry.register_table(
            "khatabook_customers".to_string(),
            PathBuf::from("data/khatabook_customers.csv"),
            vec!["customer_id".to_string()],
            column_descriptions,
            Some("Customer master table from Khatabook system".to_string()),
        ).unwrap();
        
        // Search for "khatabook"
        let results = registry.search_knowledge("khatabook");
        assert!(results.contains(&ref_id));
        
        // Search for "customer"
        let results = registry.search_knowledge("customer");
        assert!(results.contains(&ref_id));
    }
}

