//! KnowledgeBase REST API Server
//! 
//! Main binary to run the REST API server for KnowledgeBase vector store

use knowledge_base::{KnowledgeBase, VectorStore, BusinessConcept, ConceptType};
use knowledge_base::api_server::{ApiState, start_server};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde_json;

/// Load concepts from knowledge_base.json if it exists
fn load_concepts_from_json(kb: &mut KnowledgeBase, vs: &mut VectorStore, json_path: &Path) {
    if !json_path.exists() {
        println!("⚠️  Knowledge base JSON not found at: {:?}", json_path);
        println!("   Starting with empty knowledge base.");
        return;
    }

    match std::fs::read_to_string(json_path) {
        Ok(content) => {
            match serde_json::from_str::<serde_json::Value>(&content) {
                Ok(json) => {
                    if let Some(terms) = json.get("terms").and_then(|t| t.as_object()) {
                        let mut count = 0;
                        for (term_name, term_def) in terms {
                            if let Some(def_obj) = term_def.as_object() {
                                let definition = def_obj
                                    .get("definition")
                                    .and_then(|d| d.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                
                                let mut concept = BusinessConcept::new(
                                    format!("term_{}", term_name),
                                    term_name.clone(),
                                    ConceptType::Entity,
                                    definition,
                                );
                                
                                // Add related tables if available
                                if let Some(tables) = def_obj.get("related_tables").and_then(|t| t.as_array()) {
                                    for table in tables {
                                        if let Some(table_name) = table.as_str() {
                                            concept.related_tables.push(table_name.to_string());
                                        }
                                    }
                                }
                                
                                kb_write.add_concept(concept.clone());
                                
                                // Add to vector store with a simple embedding (just zeros for now)
                                // In production, would generate proper embeddings
                                let embedding = vec![0.0f32; 384]; // Placeholder embedding
                                vs_write.add_concept(concept, embedding);
                                
                                count += 1;
                            }
                        }
                        println!("✅ Loaded {} business terms from knowledge base", count);
                    } else {
                        println!("⚠️  No 'terms' key found in knowledge base JSON");
                    }
                }
                Err(e) => {
                    eprintln!("❌ Failed to parse knowledge base JSON: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("❌ Failed to read knowledge base JSON: {}", e);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting KnowledgeBase REST API Server...");
    
    // Initialize KnowledgeBase and VectorStore
    let kb = Arc::new(RwLock::new(KnowledgeBase::new()));
    let vs = Arc::new(RwLock::new(VectorStore::new()));
    
    // Load concepts from JSON if available
    {
        let mut kb_write = kb.write().await;
        let mut vs_write = vs.write().await;
        
        // Try to load from metadata/knowledge_base.json
        let json_path = Path::new("metadata/knowledge_base.json");
        load_concepts_from_json(&mut kb_write, &mut vs_write, json_path);
        
        // Also try to load from semantic_registry.json for metrics/dimensions
        let semantic_path = Path::new("metadata/semantic_registry.json");
        if semantic_path.exists() {
            match std::fs::read_to_string(semantic_path) {
                Ok(content) => {
                    match serde_json::from_str::<serde_json::Value>(&content) {
                        Ok(json) => {
                            // Load metrics
                            if let Some(metrics) = json.get("metrics").and_then(|m| m.as_array()) {
                                let mut count = 0;
                                for metric in metrics {
                                    if let Some(name) = metric.get("name").and_then(|n| n.as_str()) {
                                        let description = metric
                                            .get("description")
                                            .and_then(|d| d.as_str())
                                            .unwrap_or("")
                                            .to_string();
                                        
                                        let concept = BusinessConcept::new(
                                            format!("metric_{}", name),
                                            name.to_string(),
                                            ConceptType::Metric,
                                            description,
                                        );
                                        
                                        kb_write.add_concept(concept.clone());
                                        
                                        let embedding = vec![0.0f32; 384];
                                        vs_write.add_concept(concept, embedding);
                                        count += 1;
                                    }
                                }
                                println!("✅ Loaded {} metrics from semantic registry", count);
                            }
                            
                            // Load dimensions
                            if let Some(dimensions) = json.get("dimensions").and_then(|d| d.as_array()) {
                                let mut count = 0;
                                for dim in dimensions {
                                    if let Some(name) = dim.get("name").and_then(|n| n.as_str()) {
                                        let description = dim
                                            .get("description")
                                            .and_then(|d| d.as_str())
                                            .unwrap_or("")
                                            .to_string();
                                        
                                        let concept = BusinessConcept::new(
                                            format!("dimension_{}", name),
                                            name.to_string(),
                                            ConceptType::Dimension,
                                            description,
                                        );
                                        
                                        kb_write.add_concept(concept.clone());
                                        
                                        let embedding = vec![0.0f32; 384];
                                        vs_write.add_concept(concept, embedding);
                                        count += 1;
                                    }
                                }
                                println!("✅ Loaded {} dimensions from semantic registry", count);
                            }
                        }
                        Err(e) => {
                            eprintln!("⚠️  Failed to parse semantic registry: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("⚠️  Failed to read semantic registry: {}", e);
                }
            }
        }
    }
    
    // Create API state
    let state = ApiState {
        knowledge_base: kb,
        vector_store: vs,
    };
    
    // Get host and port from environment or use defaults
    let host = std::env::var("KB_API_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("KB_API_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    
    // Start server
    start_server(&host, port, state).await?;
    
    Ok(())
}

