//! REST API Server for KnowledgeBase Vector Store
//! 
//! Exposes RAG retrieval endpoints for Python backend integration

use super::concepts::{KnowledgeBase, BusinessConcept};
use super::vector_store::{VectorStore, ConceptSearchResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use tower_http::cors::{CorsLayer, Any};
use std::collections::HashMap;

/// API State - Shared between handlers
#[derive(Clone)]
pub struct ApiState {
    pub knowledge_base: Arc<RwLock<KnowledgeBase>>,
    pub vector_store: Arc<RwLock<VectorStore>>,
}

/// Request for RAG retrieval
#[derive(Deserialize)]
pub struct RAGRequest {
    pub query: String,
    #[serde(default = "default_top_k")]
    pub top_k: usize,
}

fn default_top_k() -> usize {
    10
}

/// Response for RAG retrieval
#[derive(Serialize)]
pub struct RAGResponse {
    pub results: Vec<ConceptSearchResult>,
    pub context: String,
}

/// Response for health check
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub concepts_count: usize,
    pub vector_store_size: usize,
}

/// Health check endpoint
async fn health_check(State(state): State<ApiState>) -> Json<HealthResponse> {
    let kb = state.knowledge_base.read().await;
    let vs = state.vector_store.read().await;
    
    Json(HealthResponse {
        status: "healthy".to_string(),
        concepts_count: kb.list_all().len(),
        vector_store_size: vs.get_concept_count(),
    })
}

/// RAG retrieval endpoint
async fn rag_retrieve(
    State(state): State<ApiState>,
    Json(request): Json<RAGRequest>,
) -> Result<Json<RAGResponse>, StatusCode> {
    let vector_store = state.vector_store.read().await;
    
    // Retrieve relevant concepts
    let results = vector_store.rag_retrieve(&request.query, request.top_k);
    
    // Generate context string
    let context = vector_store.get_rag_context(&request.query, request.top_k);
    
    Ok(Json(RAGResponse {
        results,
        context,
    }))
}

/// Search concepts by text
async fn search_concepts(
    State(state): State<ApiState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Vec<ConceptSearchResult>>, StatusCode> {
    let query = params.get("q").ok_or(StatusCode::BAD_REQUEST)?;
    let top_k = params
        .get("top_k")
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    
    let vector_store = state.vector_store.read().await;
    let results = vector_store.search_by_text(query, top_k);
    
    Ok(Json(results))
}

/// Get all concepts (for debugging)
async fn list_concepts(State(state): State<ApiState>) -> Json<Vec<BusinessConcept>> {
    let kb = state.knowledge_base.read().await;
    Json(kb.list_all().into_iter().cloned().collect())
}

/// Create the API router
pub fn create_router(state: ApiState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    
    Router::new()
        .route("/health", get(health_check))
        .route("/rag", post(rag_retrieve))
        .route("/search", get(search_concepts))
        .route("/concepts", get(list_concepts))
        .layer(cors)
        .with_state(state)
}

/// Start the API server
pub async fn start_server(host: &str, port: u16, state: ApiState) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(state);
    
    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    
    println!(" KnowledgeBase API server listening on http://{}", addr);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}


