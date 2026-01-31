use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

/// Shared application state
/// In a real implementation, this would hold a reference to your hypergraph
type AppState = Arc<RwLock<GraphData>>;

/// Graph data structure matching the frontend expectations
#[derive(Clone, Debug)]
struct GraphData {
    nodes: Vec<Value>,
    edges: Vec<Value>,
}

impl GraphData {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Create sample data for demonstration
    fn sample() -> Self {
        Self {
            nodes: vec![
                json!({
                    "id": "table_1",
                    "label": "users",
                    "type": "table",
                    "row_count": 1000,
                    "columns": ["id", "name", "email", "created_at"]
                }),
                json!({
                    "id": "table_2",
                    "label": "orders",
                    "type": "table",
                    "row_count": 5000,
                    "columns": ["id", "user_id", "product_id", "amount", "created_at"]
                }),
                json!({
                    "id": "table_3",
                    "label": "products",
                    "type": "table",
                    "row_count": 500,
                    "columns": ["id", "name", "price", "category_id"]
                }),
                json!({
                    "id": "table_4",
                    "label": "categories",
                    "type": "table",
                    "row_count": 50,
                    "columns": ["id", "name", "description"]
                }),
            ],
            edges: vec![
                json!({
                    "id": "edge_1",
                    "from": "table_1",
                    "to": "table_2",
                    "label": "users.id = orders.user_id"
                }),
                json!({
                    "id": "edge_2",
                    "from": "table_2",
                    "to": "table_3",
                    "label": "orders.product_id = products.id"
                }),
                json!({
                    "id": "edge_3",
                    "from": "table_3",
                    "to": "table_4",
                    "label": "products.category_id = categories.id"
                }),
            ],
        }
    }
}

/// GET /api/graph - Returns the hypergraph visualization data
async fn get_graph(State(state): State<AppState>) -> Result<Json<Value>, StatusCode> {
    let data = state.read().await;
    
    let table_count = data.nodes.iter()
        .filter(|n| n["type"] == "table")
        .count();
    
    let column_count = data.nodes.iter()
        .filter(|n| n["type"] == "column")
        .count();
    
    Ok(Json(json!({
        "nodes": data.nodes,
        "edges": data.edges,
        "stats": {
            "total_nodes": data.nodes.len(),
            "total_edges": data.edges.len(),
            "table_count": table_count,
            "column_count": column_count,
        }
    })))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize with sample data
    // In a real implementation, you would load this from your hypergraph source
    let state: AppState = Arc::new(RwLock::new(GraphData::sample()));
    
    // Build the router
    let app = Router::new()
        .route("/api/graph", get(get_graph))
        .layer(CorsLayer::permissive()) // Allow all CORS for development
        .with_state(state);
    
    // Start the server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("🚀 Hypergraph Visualizer Backend running on http://localhost:3000");
    println!("📊 Graph API available at http://localhost:3000/api/graph");
    
    axum::serve(listener, app).await?;
    
    Ok(())
}

