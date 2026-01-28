//! NodeRegistry HTTP Server
//! 
//! Provides HTTP API for accessing NodeRegistry from Python and other clients.
//! Uses tokio directly (no axum dependency) to match existing server pattern.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{info, error};

use spyne_ide::node_registry::NodeRegistry;

/// Server state containing the NodeRegistry
struct AppState {
    registry: Arc<RwLock<NodeRegistry>>,
}

/// Health check response
#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
}

/// Search response
#[derive(Serialize)]
struct SearchResponse {
    nodes: Vec<spyne_ide::node_registry::Node>,
    knowledge_pages: Vec<spyne_ide::node_registry::KnowledgePage>,
    metadata_pages: Vec<spyne_ide::node_registry::MetadataPage>,
}

/// Error response
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    // Get registry path from environment or use default
    let registry_path = std::env::var("NODE_REGISTRY_PATH")
        .unwrap_or_else(|_| "./node_registry".to_string());
    
    info!("Loading NodeRegistry from: {}", registry_path);
    
    // Load NodeRegistry
    let registry = match NodeRegistry::load(&registry_path) {
        Ok(reg) => {
            info!("NodeRegistry loaded successfully");
            reg
        }
        Err(e) => {
            error!("Failed to load NodeRegistry: {}. Creating new registry.", e);
            NodeRegistry::new()
        }
    };
    
    let state = Arc::new(AppState {
        registry: Arc::new(RwLock::new(registry)),
    });
    
    // Get bind address from environment or use default
    let bind_addr = std::env::var("NODE_REGISTRY_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8081".to_string());
    
    info!("Starting NodeRegistry server on {}", bind_addr);
    
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Server listening on {}", bind_addr);
    
    loop {
        let (stream, addr) = listener.accept().await?;
        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state_clone).await {
                error!("Error handling connection from {}: {}", addr, e);
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::time::{timeout, Duration};
    
    // Read request with timeout
    let mut buffer = Vec::new();
    let mut temp_buf = [0; 8192];
    
    let read_result = timeout(Duration::from_secs(5), async {
        loop {
            match stream.read(&mut temp_buf).await {
                Ok(0) => break,
                Ok(n) => {
                    buffer.extend_from_slice(&temp_buf[..n]);
                    if let Ok(s) = std::str::from_utf8(&buffer) {
                        if s.contains("\r\n\r\n") {
                            break;
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(buffer)
    }).await;
    
    let request_bytes = match read_result {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => return Err(Box::new(e)),
        Err(_) => {
            send_error_response(&mut stream, 408, "Request timeout").await?;
            return Ok(());
        }
    };
    
    let request_str = String::from_utf8_lossy(&request_bytes);
    let request_lines: Vec<&str> = request_str.lines().collect();
    
    if request_lines.is_empty() {
        send_error_response(&mut stream, 400, "Empty request").await?;
        return Ok(());
    }
    
    // Parse request line
    let request_line = request_lines[0];
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        send_error_response(&mut stream, 400, "Invalid request line").await?;
        return Ok(());
    }
    
    let method = parts[0];
    let path = parts[1];
    
    // Parse path and query
    let (path, query_params) = if let Some(q_pos) = path.find('?') {
        let (p, q) = path.split_at(q_pos);
        let params: HashMap<String, String> = q[1..]
            .split('&')
            .filter_map(|pair| {
                let mut kv = pair.split('=');
                if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
                    Some((k.to_string(), url_decode(v)))
                } else {
                    None
                }
            })
            .collect();
        (p, params)
    } else {
        (path, HashMap::new())
    };
    
    // Handle routes
    let response = match (method, path) {
        ("GET", "/health") => {
            let health = HealthResponse {
                status: "ok".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            };
            create_json_response(200, &health)?
        }
        ("GET", "/search") => {
            let query = query_params.get("q").cloned().unwrap_or_default();
            if query.is_empty() {
                let error = ErrorResponse {
                    error: "Query parameter 'q' is required".to_string(),
                };
                create_json_response(400, &error)?
            } else {
                let registry = state.registry.read().await;
                let (nodes_ref, knowledge_pages_ref, metadata_pages_ref) = registry.search_all(&query);
                let response = SearchResponse {
                    nodes: nodes_ref.iter().map(|n| (*n).clone()).collect(),
                    knowledge_pages: knowledge_pages_ref.iter().map(|k| (*k).clone()).collect(),
                    metadata_pages: metadata_pages_ref.iter().map(|m| (*m).clone()).collect(),
                };
                create_json_response(200, &response)?
            }
        }
        (method, path) if path.starts_with("/node/") => {
            let ref_id = path.strip_prefix("/node/").unwrap_or("");
            if ref_id.is_empty() {
                let error = ErrorResponse {
                    error: "Missing ref_id".to_string(),
                };
                create_json_response(400, &error)?
            } else {
                let registry = state.registry.read().await;
                match registry.get_node(ref_id) {
                    Some(node) => create_json_response(200, &node)?,
                    None => {
                        let error = ErrorResponse {
                            error: format!("Node with ref_id '{}' not found", ref_id),
                        };
                        create_json_response(404, &error)?
                    }
                }
            }
        }
        ("OPTIONS", _) => {
            // CORS preflight
            create_cors_response(200)?
        }
        _ => {
            let error = ErrorResponse {
                error: format!("Not found: {} {}", method, path),
            };
            create_json_response(404, &error)?
        }
    };
    
    stream.write_all(&response).await?;
    stream.flush().await?;
    
    Ok(())
}

fn create_json_response<T: Serialize>(status_code: u16, body: &T) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let json_body = serde_json::to_string(body)?;
    let status_text = match status_code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        408 => "Request Timeout",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    
    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: application/json\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        status_code,
        status_text,
        json_body.len(),
        json_body
    );
    
    Ok(response.into_bytes())
}

fn create_cors_response(status_code: u16) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let status_text = match status_code {
        200 => "OK",
        _ => "Unknown",
    };
    
    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         Content-Length: 0\r\n\
         \r\n",
        status_code,
        status_text
    );
    
    Ok(response.into_bytes())
}

async fn send_error_response(
    stream: &mut TcpStream,
    status_code: u16,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let error = ErrorResponse {
        error: message.to_string(),
    };
    let response = create_json_response(status_code, &error)?;
    stream.write_all(&response).await?;
    stream.flush().await?;
    Ok(())
}

fn url_decode(s: &str) -> String {
    // Simple URL decoding - replace %20 with space, etc.
    s.replace("%20", " ")
        .replace("%21", "!")
        .replace("%22", "\"")
        .replace("%23", "#")
        .replace("%24", "$")
        .replace("%25", "%")
        .replace("%26", "&")
        .replace("%27", "'")
        .replace("%28", "(")
        .replace("%29", ")")
        .replace("%2B", "+")
        .replace("%2C", ",")
        .replace("%2F", "/")
        .replace("%3A", ":")
        .replace("%3B", ";")
        .replace("%3D", "=")
        .replace("%3F", "?")
        .replace("%40", "@")
}
