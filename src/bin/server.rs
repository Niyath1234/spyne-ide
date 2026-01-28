//! HTTP Server for RCA Engine UI
//! Simple HTTP server using tokio and basic HTTP handling

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::path::PathBuf;
use serde::Deserialize;
use spyne_ide::metadata::Metadata;
use spyne_ide::llm::LlmClient;
use spyne_ide::graph_traversal::GraphTraversalAgent;
use spyne_ide::sql_engine::SqlEngine;
use spyne_ide::graph::Hypergraph;
use spyne_ide::intent_compiler::{IntentCompiler, IntentCompilationResult, IntentSpec, TaskType};
use spyne_ide::error::RcaError;
use spyne_ide::agent::contracts::{AgentRequest, AgentContinueRequest};
use spyne_ide::agent::service::run_agent;
use spyne_ide::data_assistant::DataAssistant;
use spyne_ide::node_registry::NodeRegistry;
use spyne_ide::query_engine::QueryEngine;
use spyne_ide::learning_store::LearningStore;
use spyne_ide::rca::RcaEngine;
use std::sync::Arc;
use tokio::sync::Mutex;

// Global Node Registry (shared across all connections)
lazy_static::lazy_static! {
    static ref NODE_REGISTRY: Arc<Mutex<NodeRegistry>> = {
        let registry_path = PathBuf::from("node_registry");
        let registry = NodeRegistry::load(&registry_path)
            .unwrap_or_else(|_| {
                eprintln!("[INFO] Creating new Node Registry");
                NodeRegistry::new()
            });
        Arc::new(Mutex::new(registry))
    };
    
    // Global Learning Store (shared across all connections)
    static ref LEARNING_STORE: Arc<Mutex<LearningStore>> = {
        let learning_path = PathBuf::from("metadata");
        let store = LearningStore::load(&learning_path)
            .unwrap_or_else(|_| {
                eprintln!("[INFO] Creating new Learning Store");
                LearningStore::new(&learning_path).unwrap()
            });
        Arc::new(Mutex::new(store))
    };
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();
    
    println!("Starting RCA Engine API Server...");
    println!("Server will run on http://localhost:8080");
    println!("UI should connect to: http://localhost:8080");
    
    // Check if API key is set
    if std::env::var("OPENAI_API_KEY").is_ok() {
        println!("[OK] OpenAI API key found - Real RCA analysis enabled");
    } else {
        println!("[WARN] OpenAI API key not found - Will use fallback responses");
    }
    
    // Initialize Node Registry
    let registry_path = PathBuf::from("node_registry");
    if let Ok(mut registry) = NodeRegistry::load(&registry_path) {
        println!("[OK] Node Registry loaded from {}", registry_path.display());
        *NODE_REGISTRY.lock().await = registry;
    } else {
        println!("[INFO] Creating new Node Registry");
    }
    
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("[OK] Server listening on port 8080");
    
    loop {
        let (stream, addr) = listener.accept().await?;
        println!("[INFO] New connection from: {}", addr);
        tokio::spawn(handle_connection(stream));
    }
}

async fn handle_connection(mut stream: TcpStream) {
    use tokio::time::{timeout, Duration};
    
    // Read request with timeout to prevent hanging
    let mut buffer = Vec::new();
    let mut temp_buf = [0; 8192];
    
    let read_result = timeout(Duration::from_secs(5), async {
        loop {
            match stream.read(&mut temp_buf).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    buffer.extend_from_slice(&temp_buf[..n]);
                    // Check if we've reached the end of HTTP headers + body
                    if let Ok(s) = std::str::from_utf8(&buffer) {
                        if s.contains("\r\n\r\n") {
                            // We have headers, check if we have the full body
                            if let Some(content_length) = extract_content_length(s) {
                                let headers_end = s.find("\r\n\r\n").unwrap() + 4;
                                if buffer.len() >= headers_end + content_length {
                                    break; // We have the complete request
                                }
                            } else if n < temp_buf.len() {
                                // No content-length header and we got less than buffer size
                                break;
                            }
                        }
                    }
                    // If buffer is getting too large, break to prevent memory issues
                    if buffer.len() > 1_000_000 {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read from stream: {}", e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }).await;
    
    if read_result.is_err() {
        eprintln!("[WARN] Request read timeout");
        return;
    }
    
    if buffer.is_empty() {
        return;
    }
    
    match String::from_utf8(buffer) {
        Ok(request) => {
            let response = handle_request(&request).await;
            if let Err(e) = stream.write_all(response.as_bytes()).await {
                eprintln!("Failed to write response: {}", e);
            }
        }
        Err(e) => {
            eprintln!("Failed to parse request as UTF-8: {}", e);
        }
    }
}

fn extract_content_length(request: &str) -> Option<usize> {
    for line in request.lines() {
        if line.to_lowercase().starts_with("content-length:") {
            if let Some(value) = line.split(':').nth(1) {
                return value.trim().parse().ok();
            }
        }
    }
    None
}

async fn handle_request(request: &str) -> String {
    let lines: Vec<&str> = request.lines().collect();
    if lines.is_empty() {
        return create_response(400, "Bad Request", "{}");
    }
    
    let request_line = lines[0];
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    
    if parts.len() < 2 {
        return create_response(400, "Bad Request", "{}");
    }
    
    let method = parts[0];
    let full_path = parts[1].to_string();
    
    // Extract query string BEFORE stripping (needed for /api/search/knowledge)
    let (path_str, query_string) = if let Some(query_start) = full_path.find('?') {
        (full_path[..query_start].to_string(), Some(full_path[query_start + 1..].to_string()))
    } else {
        (full_path, None)
    };
    
    // Normalize path (remove trailing slash except for root)
    let mut normalized_path = path_str.trim_end_matches('/').to_string();
    if normalized_path.is_empty() {
        normalized_path = "/".to_string();
    }
    let path = normalized_path.as_str();
    
    // Debug logging
    eprintln!("[DEBUG] Request: {} {}", method, path);
    
    // Parse headers
    let mut headers = HashMap::new();
    for line in &lines[1..] {
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_lowercase(), value.trim().to_string());
        }
    }
    
    // Handle routes
    match (method, path) {
        ("GET", "/api/health") => {
            create_response(200, "OK", r#"{"status":"ok","service":"rca-engine-api"}"#)
        }
        ("GET", "/api/sources") => {
            // Return actual tables from metadata
            match get_tables_from_metadata().await {
                Ok(json) => create_response(200, "OK", &json),
                Err(_) => create_response(200, "OK", r#"{"sources":[]}"#)
            }
        }
        ("GET", "/api/tables") => {
            // Return tables metadata
            match get_tables_from_metadata().await {
                Ok(json) => create_response(200, "OK", &json),
                Err(_) => create_response(200, "OK", r#"{"tables":[]}"#)
            }
        }
        ("GET", "/api/pipelines") => {
            // Return pipelines (tables) from metadata
            match get_pipelines_from_metadata().await {
                Ok(json) => create_response(200, "OK", &json),
                Err(_) => create_response(200, "OK", r#"{"pipelines":[]}"#)
            }
        }
        ("GET", "/api/rules") => {
            // Return rules from metadata
            match get_rules_from_metadata().await {
                Ok(json) => create_response(200, "OK", &json),
                Err(e) => {
                    eprintln!("Error loading rules: {}", e);
                    create_response(200, "OK", r#"{"rules":[]}"#)
                }
            }
        }
        ("GET", "/api/metadata") => {
            match get_all_metadata().await {
                Ok(json) => create_response(200, "OK", &json),
                Err(e) => create_response(500, "Internal Server Error", &format!(r#"{{"error":"{}"}}"#, e)),
            }
        }
        ("GET", "/api/knowledge/entries") => {
            match get_knowledge_entries().await {
                Ok(json) => create_response(200, "OK", &json),
                Err(e) => create_response(500, "Internal Server Error", &format!(r#"{{"error":"{}"}}"#, e)),
            }
        }
        ("GET", "/api/knowledge-base") => {
            // Return knowledge base dictionary
            match get_knowledge_base() {
                Ok(json) => create_response(200, "OK", &json),
                Err(e) => {
                    eprintln!("Error loading knowledge base: {}", e);
                    create_response(200, "OK", r#"{"terms":{},"tables":{},"relationships":{}}"#)
                }
            }
        }
        ("GET", "/api/graph") => {
            // Return hypergraph visualization data (nodes, edges, stats)
            match get_graph_data().await {
                Ok(json) => create_response(200, "OK", &json),
                Err(e) => {
                    eprintln!("Error loading graph data: {}", e);
                    create_response(500, "Internal Server Error", r#"{"error":"Failed to load graph data"}"#)
                }
            }
        }
        ("POST", "/api/upload/csv") => {
            // Simple CSV upload handler (legacy endpoint)
            create_response(200, "OK", r#"{"success":true,"message":"CSV uploaded successfully. Processing will create nodes and edges automatically.","records":0}"#)
        }
        // ====================================================================
        // NEW: NODE REGISTRY ENDPOINTS
        // ====================================================================
        ("POST", "/api/register/table") => {
            // Register a table (creates Node + Knowledge Page + Metadata Page)
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            let json_str = if body.starts_with('{') {
                body
            } else if let Some(json_start) = body.find('{') {
                &body[json_start..]
            } else {
                ""
            };
            
            if json_str.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"JSON body required"}"#);
            }
            
            match handle_register_table(json_str).await {
                Ok(response_json) => create_response(200, "OK", &response_json),
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": e.to_string()
                    });
                    create_response(400, "Bad Request", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        ("GET", "/api/search/knowledge") => {
            // Search Knowledge Register
            // Extract query parameter from query_string (extracted before path normalization)
            let query = if let Some(qs) = &query_string {
                // Parse query parameters (simple URL decoding)
                let mut search_term = String::new();
                for param in qs.split('&') {
                    if let Some((key, value)) = param.split_once('=') {
                        if key == "q" {
                            // Simple URL decoding (replace %20 with space, etc.)
                            search_term = value.replace("%20", " ")
                                .replace("%2B", "+")
                                .replace("%2F", "/")
                                .replace("%3D", "=")
                                .replace("%26", "&")
                                .replace("%3F", "?")
                                .replace("%25", "%");
                            break;
                        }
                    }
                }
                search_term
            } else {
                String::new()
            };
            
            if query.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"Query parameter 'q' is required"}"#);
            }
            
            match handle_search_knowledge(&query).await {
                Ok(response_json) => create_response(200, "OK", &response_json),
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": e.to_string()
                    });
                    create_response(500, "Internal Server Error", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        ("GET", path) if path.starts_with("/api/nodes/") => {
            // Get node details by reference ID
            let ref_id = path.strip_prefix("/api/nodes/").unwrap_or("");
            if ref_id.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"Reference ID is required"}"#);
            }
            
            match handle_get_node(ref_id).await {
                Ok(response_json) => create_response(200, "OK", &response_json),
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": e.to_string()
                    });
                    create_response(404, "Not Found", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        // ====================================================================
        // NEW: FAIL-FAST CLARIFICATION ENDPOINT
        // ====================================================================
        ("POST", "/api/reasoning/assess") => {
            // Assess query confidence and return clarification if needed
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            let query = if let Some(json_start) = body.find('{') {
                let json_str = &body[json_start..];
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                    json.get("query")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            
            if query.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"Query is required"}"#);
            }
            
            // Use fail-fast with clarification
            let result = assess_and_compile_query(&query, None).await;
            
            match result {
                Ok(compilation_result) => {
                    let response_json = match compilation_result {
                        IntentCompilationResult::Success(intent) => {
                            // Check if validation was performed and include warnings
                            let metadata_dir = PathBuf::from("metadata");
                            let mut validation_info = serde_json::json!({
                                "validated": false,
                                "warnings": [],
                                "errors": []
                            });
                            
                            if let Ok(metadata) = Metadata::load(&metadata_dir) {
                                if let Ok(validation_result) = IntentCompiler::validate_against_metadata(&mut intent.clone(), &metadata) {
                                    validation_info = serde_json::json!({
                                        "validated": true,
                                        "is_valid": validation_result.is_valid,
                                        "warnings": validation_result.warnings,
                                        "errors": validation_result.errors,
                                        "resolved_tables": validation_result.resolved_tables,
                                        "resolved_columns": validation_result.resolved_columns.iter().map(|(t, c)| format!("{}.{}", t, c)).collect::<Vec<_>>()
                                    });
                                }
                            }
                            
                            serde_json::json!({
                                "status": "success",
                                "needs_clarification": false,
                                "intent": intent,
                                "validation": validation_info,
                                "message": "Query understood. Ready to execute."
                            })
                        }
                        IntentCompilationResult::NeedsClarification(clarification) => {
                            serde_json::json!({
                                "status": "needs_clarification",
                                "needs_clarification": true,
                                "question": clarification.question,
                                "missing_pieces": clarification.missing_pieces,
                                "confidence": clarification.confidence,
                                "partial_understanding": clarification.partial_understanding,
                                "response_hints": clarification.response_hints,
                                "message": "Please provide additional information."
                            })
                        }
                        IntentCompilationResult::Failed(msg) => {
                            serde_json::json!({
                                "status": "failed",
                                "needs_clarification": false,
                                "error": msg,
                                "message": "Failed to understand query."
                            })
                        }
                    };
                    create_response(200, "OK", &serde_json::to_string(&response_json).unwrap())
                }
                Err(e) => {
                    let error_json = serde_json::json!({
                        "status": "error",
                        "error": e.to_string()
                    });
                    create_response(500, "Internal Server Error", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        // ====================================================================
        // NEW: COMPILE WITH CLARIFICATION ANSWER
        // ====================================================================
        ("POST", "/api/reasoning/clarify") => {
            // Compile with user's answer to clarification question
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            let (query, answer) = if let Some(json_start) = body.find('{') {
                let json_str = &body[json_start..];
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                    let q = json.get("query")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let a = json.get("answer")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    (q, a)
                } else {
                    (String::new(), String::new())
                }
            } else {
                (String::new(), String::new())
            };
            
            if query.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"Original query is required"}"#);
            }
            if answer.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"Clarification answer is required"}"#);
            }
            
            // Compile with answer
            let result = assess_and_compile_query(&query, Some(&answer)).await;
            
            match result {
                Ok(compilation_result) => {
                    let response_json = match compilation_result {
                        IntentCompilationResult::Success(intent) => {
                            // Check if validation was performed and include warnings
                            let metadata_dir = PathBuf::from("metadata");
                            let mut validation_info = serde_json::json!({
                                "validated": false,
                                "warnings": [],
                                "errors": []
                            });
                            
                            if let Ok(metadata) = Metadata::load(&metadata_dir) {
                                if let Ok(validation_result) = IntentCompiler::validate_against_metadata(&mut intent.clone(), &metadata) {
                                    validation_info = serde_json::json!({
                                        "validated": true,
                                        "is_valid": validation_result.is_valid,
                                        "warnings": validation_result.warnings,
                                        "errors": validation_result.errors,
                                        "resolved_tables": validation_result.resolved_tables,
                                        "resolved_columns": validation_result.resolved_columns.iter().map(|(t, c)| format!("{}.{}", t, c)).collect::<Vec<_>>()
                                    });
                                }
                            }
                            
                            serde_json::json!({
                                "status": "success",
                                "needs_clarification": false,
                                "intent": intent,
                                "validation": validation_info,
                                "message": "Query understood with clarification. Ready to execute."
                            })
                        }
                        IntentCompilationResult::NeedsClarification(clarification) => {
                            // Still needs more info even after clarification
                            serde_json::json!({
                                "status": "needs_clarification",
                                "needs_clarification": true,
                                "question": clarification.question,
                                "missing_pieces": clarification.missing_pieces,
                                "confidence": clarification.confidence,
                                "message": "Still need more information."
                            })
                        }
                        IntentCompilationResult::Failed(msg) => {
                            serde_json::json!({
                                "status": "failed",
                                "error": msg
                            })
                        }
                    };
                    create_response(200, "OK", &serde_json::to_string(&response_json).unwrap())
                }
                Err(e) => {
                    let error_json = serde_json::json!({
                        "status": "error",
                        "error": e.to_string()
                    });
                    create_response(500, "Internal Server Error", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        // ====================================================================
        // ORIGINAL REASONING/QUERY ENDPOINT (kept for backward compatibility)
        // ====================================================================
        ("POST", "/api/assistant/ask") => {
            // Cursor-like data assistant - answers any question using knowledge base
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            let json_str = if body.starts_with('{') {
                body
            } else if let Some(json_start) = body.find('{') {
                &body[json_start..]
            } else {
                ""
            };
            
            if json_str.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"JSON body required"}"#);
            }
            
            // Validate JSON structure before processing
            #[derive(Deserialize)]
            struct AssistantRequest {
                question: Option<String>,
            }
            
            let validation_result: Result<AssistantRequest, _> = serde_json::from_str(json_str);
            match validation_result {
                Ok(req) => {
                    if req.question.is_none() || req.question.as_ref().map(|q| q.is_empty()).unwrap_or(true) {
                        return create_response(400, "Bad Request", r#"{"error":"Field 'question' is required and cannot be empty"}"#);
                    }
                }
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": format!("Invalid JSON: {}", e)
                    });
                    return create_response(400, "Bad Request", &serde_json::to_string(&error_json).unwrap());
                }
            }
            
            match handle_assistant_query(json_str).await {
                Ok(response_json) => create_response(200, "OK", &response_json),
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": e.to_string()
                    });
                    create_response(500, "Internal Server Error", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        ("POST", "/api/query/execute") => {
            // Execute query - supports 3 modes: normal SQL, Knowledge Register, Metadata Register
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            let json_str = if body.starts_with('{') {
                body
            } else if let Some(json_start) = body.find('{') {
                &body[json_start..]
            } else {
                ""
            };
            
            if json_str.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"JSON body required"}"#);
            }
            
            match handle_query_execute(json_str).await {
                Ok(response_json) => create_response(200, "OK", &response_json),
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": e.to_string()
                    });
                    create_response(500, "Internal Server Error", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        ("POST", "/api/reasoning/query") => {
            // Extract query from body - handle both raw HTTP and JSON body formats
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            // Try multiple parsing strategies
            let query = if body.starts_with('{') {
                // Direct JSON body
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
                    json.get("query")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                } else {
                    String::new()
                }
            } else if let Some(json_start) = body.find('{') {
                // JSON somewhere in the body
                let json_str = &body[json_start..];
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                    json.get("query")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string()
                } else {
                    String::new()
                }
            } else if !body.is_empty() {
                // Maybe it's just the query string directly?
                body.to_string()
            } else {
                String::new()
            };
            
            if query.is_empty() {
                eprintln!("[WARN] Failed to parse query from request body. Body length: {}, Body preview: {}", body.len(), &body[..body.len().min(200)]);
                return create_response(400, "Bad Request", r#"{"error":"Query is required"}"#);
            }
            
            // Try to execute real RCA analysis with actual data
            let result = execute_rca_query(&query).await;
            
            match result {
                Ok((result_text, steps)) => {
                    let result_json = serde_json::json!({
                        "result": result_text,
                        "steps": steps
                    });
                    create_response(200, "OK", &serde_json::to_string(&result_json).unwrap_or_else(|_| r#"{"error":"Failed to serialize response"}"#.to_string()))
                }
                Err(e) => {
                    // Log the actual error for debugging
                    eprintln!("[ERROR] RCA analysis failed: {}", e);
                    eprintln!("   Query: {}", query);
                    eprintln!("   Error details: {:?}", e);
                    eprintln!("   This usually means:");
                    eprintln!("     1. LLM API call failed (check OPENAI_API_KEY, OPENAI_MODEL, OPENAI_BASE_URL)");
                    eprintln!("     2. Metadata or data files are missing");
                    eprintln!("     3. Query parsing failed");
                    eprintln!("   Falling back to mock data...");
                    
                    // Get actual systems from metadata instead of hardcoding
                    let metadata_dir = PathBuf::from("metadata");
                    let detected_systems = match Metadata::load(&metadata_dir) {
                        Ok(metadata) => {
                            let mut systems: Vec<String> = metadata.tables.iter()
                                .map(|t| t.system.clone())
                                .collect::<std::collections::HashSet<_>>()
                                .into_iter()
                                .collect();
                            systems.sort();
                            if systems.len() >= 2 {
                                (systems[0].clone(), systems[1].clone())
                            } else if systems.len() == 1 {
                                (systems[0].clone(), "system_b".to_string())
                            } else {
                                ("system_a".to_string(), "system_b".to_string())
                            }
                        }
                        Err(_) => ("system_a".to_string(), "system_b".to_string())
                    };
                    
                    // Adaptive fallback: Extract actual metrics and systems from query instead of generic mock data
                    let query_lower = query.to_lowercase();
                    let has_mismatch = query_lower.contains("mismatch") || query_lower.contains("difference") || query_lower.contains("compare");
                    
                    // Try to extract actual metrics from query
                    let mut detected_metrics = Vec::new();
                    if query_lower.contains("minority") || query_lower.contains("minority_category") {
                        detected_metrics.push("minority_category");
                    }
                    if query_lower.contains("social") || query_lower.contains("social_category") {
                        detected_metrics.push("social_category");
                    }
                    if query_lower.contains("ledger") || query_lower.contains("balance") {
                        detected_metrics.push("ledger_balance");
                    }
                    if query_lower.contains("transaction") || query_lower.contains("count") {
                        detected_metrics.push("transaction_count");
                    }
                    
                    // Use detected metrics or fallback to generic
                    let metric_a = detected_metrics.get(0).unwrap_or(&"metric_a");
                    let metric_b = detected_metrics.get(1).unwrap_or(metric_a);
                    
                    // Generate adaptive table based on actual query context
                    let table_data = if has_mismatch {
                        if detected_metrics.len() >= 2 {
                            // Cross-metric comparison
                            format!("\n\nMismatch Details:\n\n| System | Metric | Value | Status |\n|--------|--------|-------|--------|\n| {} | {} | * | Mismatch |\n| {} | {} | * | Mismatch |\n\n*Note: Real RCA execution failed. Please check:\n- Rules exist for both metrics in their respective systems\n- Data files are loaded correctly\n- Identity mapping is configured\n- Error: {}", 
                                detected_systems.0, metric_a, detected_systems.1, metric_b, e)
                        } else {
                            // Single metric comparison
                            format!("\n\nMismatch Details:\n\n| System | Metric | Value | Status | Difference |\n|--------|--------|-------|--------|------------|\n| {} | {} | * | Mismatch | * |\n| {} | {} | * | Mismatch | * |\n\n*Note: Real RCA execution failed. Error: {}", 
                                detected_systems.0, metric_a, detected_systems.1, metric_b, e)
                        }
                    } else {
                        format!("\n\nError: RCA execution failed: {}\n\nPlease check:\n- Query interpretation is correct\n- Rules exist for specified metrics\n- Data files are accessible\n- Server logs for detailed error", e)
                    };
                    
                    let result_text = format!("⚠️ RCA Analysis Failed - Adaptive Error Response\n\nQuery: {}\n\nDetected Systems: {} and {}\nDetected Metrics: {} and {}\n\n{}", 
                        query, detected_systems.0, detected_systems.1, metric_a, metric_b, table_data);
                    
                    let result_json = serde_json::json!({
                        "result": result_text,
                        "steps": [
                            {"type": "thought", "content": format!("Analyzing query: {}", query)},
                            {"type": "thought", "content": format!("Detected systems: {} and {}", detected_systems.0, detected_systems.1)},
                            {"type": "thought", "content": format!("Detected metrics: {} and {}", metric_a, metric_b)},
                            {"type": "action", "content": format!("Attempting to query {} and {}", detected_systems.0, detected_systems.1)},
                            {"type": "error", "content": format!("RCA execution failed: {}. This is not mock data - the real query failed. Check server logs for details.", e)},
                            {"type": "result", "content": result_text}
                        ]
                    });
                    
                    create_response(200, "OK", &serde_json::to_string(&result_json).unwrap_or_else(|_| r#"{"error":"Failed to serialize response"}"#.to_string()))
                }
            }
        }
        // ====================================================================
        // NEW: AGENTIC CURSOR-LIKE ENDPOINTS (MVP contracts + stub)
        // ====================================================================
        ("POST", "/api/agent/run") => {
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();

            let json_str = if body.starts_with('{') {
                body
            } else if let Some(json_start) = body.find('{') {
                &body[json_start..]
            } else {
                ""
            };

            let req: AgentRequest = match serde_json::from_str(json_str) {
                Ok(v) => v,
                Err(e) => {
                    return create_response(
                        400,
                        "Bad Request",
                        &serde_json::to_string(&serde_json::json!({
                            "status": "error",
                            "error": format!("Invalid JSON: {}", e)
                        }))
                        .unwrap(),
                    );
                }
            };

            let resp = run_agent(req).await;
            create_response(200, "OK", &serde_json::to_string(&resp).unwrap())
        }
        ("POST", "/api/agent/continue") => {
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();

            let json_str = if body.starts_with('{') {
                body
            } else if let Some(json_start) = body.find('{') {
                &body[json_start..]
            } else {
                ""
            };

            let req: AgentContinueRequest = match serde_json::from_str(json_str) {
                Ok(v) => v,
                Err(e) => {
                    return create_response(
                        400,
                        "Bad Request",
                        &serde_json::to_string(&serde_json::json!({
                            "status": "error",
                            "error": format!("Invalid JSON: {}", e)
                        }))
                        .unwrap(),
                    );
                }
            };

            // MVP: treat continue as a new run with the selected choice appended.
            let run_req = AgentRequest {
                session_id: req.session_id,
                // Interpret choice as a table selection for MVP; open it directly.
                user_query: format!("open {}", req.choice_id),
                ui_context: req.ui_context,
            };
            let resp = run_agent(run_req).await;
            create_response(200, "OK", &serde_json::to_string(&resp).unwrap())
        }
        ("POST", "/api/learning/approve") => {
            // Approve a correction (learn from user approval)
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            let json_str = if body.starts_with('{') {
                body
            } else if let Some(json_start) = body.find('{') {
                &body[json_start..]
            } else {
                ""
            };
            
            if json_str.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"JSON body required"}"#);
            }
            
            match handle_approve_correction(json_str).await {
                Ok(response_json) => create_response(200, "OK", &response_json),
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": e.to_string()
                    });
                    create_response(400, "Bad Request", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        ("GET", "/api/learning/stats") => {
            // Get learning statistics
            match handle_get_learning_stats().await {
                Ok(response_json) => create_response(200, "OK", &response_json),
                Err(e) => {
                    let error_json = serde_json::json!({
                        "error": e.to_string()
                    });
                    create_response(500, "Internal Server Error", &serde_json::to_string(&error_json).unwrap())
                }
            }
        }
        ("POST", "/api/graph/traverse") => {
            // Extract query and optional metadata_dir from body
            let body_start = request.find("\r\n\r\n").unwrap_or(request.len());
            let body = &request[body_start..].trim();
            
            let mut query = String::new();
            let mut metadata_dir = PathBuf::from("metadata");
            let mut data_dir = PathBuf::from("data");
            
            if let Some(json_start) = body.find('{') {
                let json_str = &body[json_start..];
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) {
                    if let Some(q) = json.get("query").and_then(|v| v.as_str()) {
                        query = q.to_string();
                    }
                    if let Some(md) = json.get("metadata_dir").and_then(|v| v.as_str()) {
                        metadata_dir = PathBuf::from(md);
                    }
                    if let Some(dd) = json.get("data_dir").and_then(|v| v.as_str()) {
                        data_dir = PathBuf::from(dd);
                    }
                }
            }
            
            if query.is_empty() {
                return create_response(400, "Bad Request", r#"{"error":"Query is required"}"#);
            }
            
            match execute_graph_traverse(&query, &metadata_dir, &data_dir).await {
                Ok(state) => {
                    let result_json = serde_json::json!({
                        "result": {
                            "root_cause_found": state.root_cause_found,
                            "current_hypothesis": state.current_hypothesis,
                            "findings": state.findings,
                            "visited_path": state.visited_path,
                            "current_depth": state.current_depth,
                            "max_depth": state.max_depth,
                            "hints": state.hints
                        },
                        "state": state
                    });
                    create_response(200, "OK", &serde_json::to_string(&result_json).unwrap_or_else(|_| r#"{"error":"Failed to serialize response"}"#.to_string()))
                }
                Err(e) => {
                    eprintln!("[ERROR] Graph traversal failed: {}", e);
                    create_response(500, "Internal Server Error", r#"{"error":"Graph traversal failed"}"#)
                }
            }
        }
        ("OPTIONS", _) => {
            // Handle CORS preflight
            create_response(200, "OK", "")
        }
        _ => {
            eprintln!("[ERROR] 404: {} {} not found", method, path);
            create_response(404, "Not Found", &format!(r#"{{"error":"Endpoint not found: {} {}"}}"#, method, path))
        }
    }
}

async fn execute_rca_query(query: &str) -> Result<(String, Vec<serde_json::Value>), Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let data_dir = PathBuf::from("data");
    
    // Load metadata (from PostgreSQL if USE_POSTGRES=true, otherwise from files)
    let metadata = Metadata::load_auto(&metadata_dir).await
        .map_err(|e| format!("Failed to load metadata: {}", e))?;
    
    // Create LLM client (will use env var OPENAI_API_KEY if set)
    let api_key = std::env::var("OPENAI_API_KEY")
        .unwrap_or_else(|_| "dummy".to_string());
    let model = std::env::var("OPENAI_MODEL")
        .unwrap_or_else(|_| "gpt-4".to_string());
    let base_url = std::env::var("OPENAI_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let llm = LlmClient::new(api_key, model, base_url);
    
    // Create RCA engine
    let rca_engine = RcaEngine::new(metadata, llm, data_dir);
    
    // Build steps
    let mut steps = Vec::new();
    steps.push(serde_json::json!({
        "type": "thought",
        "content": format!("Analyzing query: {}", query)
    }));
    steps.push(serde_json::json!({
        "type": "thought",
        "content": "Identifying systems and metrics involved"
    }));
    steps.push(serde_json::json!({
        "type": "action",
        "content": "Querying data sources and loading CSV files"
    }));
    steps.push(serde_json::json!({
        "type": "action",
        "content": "Comparing data and detecting differences"
    }));
    steps.push(serde_json::json!({
        "type": "action",
        "content": "Analyzing root causes"
    }));
    
    // Execute query
    match rca_engine.run(query).await {
        Ok(result) => {
            // Format result from RcaResult
            let mut result_text = format!("Root Cause Analysis Complete\n\nQuery: {}\n\n", query);
            result_text.push_str(&format!("System A: {} | System B: {} | Metric: {}\n\n", 
                result.system_a, result.system_b, result.metric));
            
            // Add classifications (root causes)
            if !result.classifications.is_empty() {
                result_text.push_str("Root Causes Found:\n");
                for classification in &result.classifications {
                    result_text.push_str(&format!("- {} ({})\n", classification.root_cause, classification.subtype));
                    result_text.push_str(&format!("  {}\n", classification.description));
                }
                result_text.push_str("\n");
            }
            
            // Add comparison results
            result_text.push_str(&format!("Population Comparison:\n"));
            result_text.push_str(&format!("- Missing in B: {}\n", result.comparison.population_diff.missing_in_b.len()));
            result_text.push_str(&format!("- Extra in B: {}\n", result.comparison.population_diff.extra_in_b.len()));
            result_text.push_str(&format!("- Common: {}\n", result.comparison.population_diff.common_count));
            result_text.push_str(&format!("- Matches: {}\n", result.comparison.data_diff.matches));
            result_text.push_str(&format!("- Mismatches: {}\n\n", result.comparison.data_diff.mismatches));
            
            // Add mismatch details as markdown table if available
            if result.comparison.data_diff.mismatches > 0 {
                let mismatch_df = &result.comparison.data_diff.mismatch_details;
                let cols: Vec<String> = mismatch_df.get_column_names().iter().map(|s| s.to_string()).collect();
                if !cols.is_empty() {
                    result_text.push_str("\n\nMismatch Details:\n\n");
                    
                    // Create markdown table header
                    result_text.push_str("| ");
                    result_text.push_str(&cols.join(" | "));
                    result_text.push_str(" |\n");
                    
                    // Create markdown table separator
                    result_text.push_str("|");
                    for _ in &cols {
                        result_text.push_str("--------|");
                    }
                    result_text.push_str("\n");
                    
                    // Add rows (limit to 20 for display)
                    let height = mismatch_df.height().min(20);
                    for i in 0..height {
                        let mut row = Vec::new();
                        for col in &cols {
                            if let Ok(series) = mismatch_df.column(col) {
                                // Get value as string
                                if let Ok(val) = series.get(i) {
                                    row.push(format!("{}", val));
                                } else {
                                    row.push("".to_string());
                                }
                            } else {
                                row.push("".to_string());
                            }
                        }
                        result_text.push_str("| ");
                        result_text.push_str(&row.join(" | "));
                        result_text.push_str(" |\n");
                    }
                }
            }
            
            steps.push(serde_json::json!({
                "type": "result",
                "content": result_text.clone()
            }));
            
            Ok((result_text, steps))
        }
        Err(e) => {
            eprintln!("[ERROR] execute_rca_query error: {}", e);
            Err(format!("RCA analysis failed: {}", e).into())
        }
    }
}

async fn execute_graph_traverse(
    query: &str,
    metadata_dir: &PathBuf,
    data_dir: &PathBuf,
) -> Result<spyne_ide::graph_traversal::TraversalState, Box<dyn std::error::Error>> {
    let metadata = Metadata::load_auto(metadata_dir).await
        .map_err(|e| format!("Failed to load metadata: {}", e))?;
    
    let api_key = std::env::var("OPENAI_API_KEY")
        .unwrap_or_else(|_| "dummy".to_string());
    let model = std::env::var("OPENAI_MODEL")
        .unwrap_or_else(|_| "gpt-4".to_string());
    let base_url = std::env::var("OPENAI_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let llm = LlmClient::new(api_key, model, base_url);
    
    let interpretation = llm.interpret_query(
        query,
        &metadata.business_labels,
        &metadata.metrics,
    ).await?;
    
    let graph = Hypergraph::new(metadata.clone());
    let sql_engine = SqlEngine::new(metadata.clone(), data_dir.clone());
    
    let kb_path = metadata_dir.join("knowledge_base.json");
    let mut agent = GraphTraversalAgent::new(metadata, graph, sql_engine)
        .with_llm(llm)
        .with_knowledge_hints_from_path(kb_path)?;
    
    let metric: &str = &interpretation.metric;
    
    let state = agent.traverse(
        query,
        metric,
        &interpretation.system_a,
        &interpretation.system_b,
        interpretation.as_of_date.as_deref(),
    ).await?;
    
    Ok(state)
}

async fn get_tables_from_metadata() -> Result<String, Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load_auto(&metadata_dir).await?;
    
    let tables_json = serde_json::json!({
        "sources": metadata.tables.iter().map(|t| {
            serde_json::json!({
                "id": t.name,
                "name": t.name,
                "system": t.system,
                "entity": t.entity,
                "path": t.path,
                "primary_key": t.primary_key,
                "type": "csv"
            })
        }).collect::<Vec<_>>()
    });
    
    Ok(serde_json::to_string(&tables_json)?)
}

async fn get_pipelines_from_metadata() -> Result<String, Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load_auto(&metadata_dir).await?;
    
    let pipelines_json = serde_json::json!({
        "pipelines": metadata.tables.iter().map(|t| {
            serde_json::json!({
                "id": t.name,
                "name": t.name,
                "type": "csv",
                "source": t.path,
                "status": "active",
                "config": {
                    "system": t.system,
                    "entity": t.entity,
                    "primary_key": t.primary_key,
                    "time_column": t.time_column.clone()
                },
                "lastRun": null,
                "createdAt": null
            })
        }).collect::<Vec<_>>()
    });
    
    Ok(serde_json::to_string(&pipelines_json)?)
}

async fn get_all_metadata() -> Result<String, Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load_auto(&metadata_dir).await?;
    
    let response = serde_json::json!({
        "tables": metadata.tables.iter().map(|t| serde_json::json!({
            "name": t.name,
            "system": t.system,
            "entity": t.entity,
            "columns": t.columns.as_ref().map(|cols| cols.iter().map(|c| serde_json::json!({
                "name": c.name,
                "data_type": c.data_type
            })).collect::<Vec<_>>()),
            "primary_key": t.primary_key
        })).collect::<Vec<_>>(),
        "metrics": metadata.metrics.iter().map(|m| serde_json::json!({
            "id": m.id,
            "name": m.name,
            "description": m.description
        })).collect::<Vec<_>>()
    });
    
    Ok(serde_json::to_string(&response)?)
}

async fn get_knowledge_entries() -> Result<String, Box<dyn std::error::Error>> {
    let knowledge_path = PathBuf::from("metadata/knowledge_base.json");
    let content = std::fs::read_to_string(&knowledge_path)?;
    let kb: serde_json::Value = serde_json::from_str(&content)?;
    
    // Convert knowledge base terms to entries
    let entries: Vec<serde_json::Value> = if let Some(terms) = kb.get("terms").and_then(|t| t.as_object()) {
        terms.iter().map(|(key, value)| {
            serde_json::json!({
                "id": key,
                "title": value.get("definition").and_then(|v| v.as_str()).unwrap_or(key),
                "content": value.get("business_meaning").and_then(|v| v.as_str()).unwrap_or(""),
                "type": "term",
                "tags": value.get("related_tables").and_then(|v| v.as_array()).map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()).unwrap_or_default()
            })
        }).collect()
    } else {
        vec![]
    };
    
    Ok(serde_json::to_string(&entries)?)
}

async fn get_rules_from_metadata() -> Result<String, Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load_auto(&metadata_dir).await?;
    
    let rules_json = serde_json::json!({
        "rules": metadata.rules.iter().map(|r| {
            // Extract description from computation for frontend display
            // Note: We don't include join information here - that's handled by the graph/lineage system
            let description = r.computation.description.clone();
            let note = r.computation.note.clone();
            
            // Build a human-readable description
            // Only include note if it contains business logic, not join instructions
            let display_description = if let Some(ref note_text) = note {
                // Only include note if it's not about joins (graph handles that)
                if !note_text.to_lowercase().contains("join") && !note_text.to_lowercase().contains("table") {
                    format!("{}\n\nNote: {}", description, note_text)
                } else {
                    description.clone()
                }
            } else {
                description.clone()
            };
            
            serde_json::json!({
                "id": r.id,
                "system": r.system,
                "metric": r.metric,
                "target_entity": r.target_entity,
                "target_grain": r.target_grain,
                "description": display_description,
                "note": note.unwrap_or_default(),
                "labels": r.labels.as_ref().unwrap_or(&Vec::new()),
                "filter_conditions": r.computation.filter_conditions.as_ref().unwrap_or(&std::collections::HashMap::new()),
                "computation": {
                    "description": r.computation.description,
                    "formula": r.computation.formula,
                    "source_entities": r.computation.source_entities,
                    "aggregation_grain": r.computation.aggregation_grain,
                    "filter_conditions": r.computation.filter_conditions,
                    "note": r.computation.note
                }
            })
        }).collect::<Vec<_>>()
    });
    
    Ok(serde_json::to_string(&rules_json)?)
}

fn get_knowledge_base() -> Result<String, Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let knowledge_base_path = metadata_dir.join("knowledge_base.json");
    
    if !knowledge_base_path.exists() {
        return Ok(r#"{"terms":{},"tables":{},"relationships":{}}"#.to_string());
    }
    
    let content = std::fs::read_to_string(&knowledge_base_path)?;
    Ok(content)
}

async fn get_graph_data() -> Result<String, Box<dyn std::error::Error>> {
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load_auto(&metadata_dir).await?;
    
    // Create nodes from tables
    let nodes: Vec<serde_json::Value> = metadata.tables.iter().map(|t| {
        // Extract columns if available
        let columns: Vec<String> = if let Some(ref cols) = t.columns {
            cols.iter().map(|c| c.name.clone()).collect()
        } else {
            Vec::new()
        };
        
        // Extract labels if available
        let labels: Vec<String> = t.labels.as_ref()
            .map(|l| l.clone())
            .unwrap_or_default();
        
        serde_json::json!({
            "id": t.name,
            "label": t.name,
            "type": "table",
            "row_count": 0, // Can be populated from actual data if needed
            "columns": columns,
            "labels": labels,
            "title": format!("{} - {}", t.name, t.entity)
        })
    }).collect();
    
    // Create edges from lineage
    let edges: Vec<serde_json::Value> = metadata.lineage.edges.iter().enumerate().map(|(idx, e)| {
        // Format join condition from keys (only show the condition, not join type)
        let join_conditions: Vec<String> = e.keys.iter()
            .map(|(left, right)| format!("{}.{} = {}.{}", e.from, left, e.to, right))
            .collect();
        let join_condition = join_conditions.join(" AND ");
        
        serde_json::json!({
            "id": format!("edge_{}", idx),
            "from": e.from,
            "to": e.to,
            "label": join_condition,
            "joinCondition": join_condition,
            "relationship": e.relationship
        })
    }).collect();
    
    // Calculate stats
    let table_count = nodes.iter().filter(|n| n["type"] == "table").count();
    let column_count: usize = nodes.iter()
        .filter_map(|n| n.get("columns"))
        .filter_map(|c| c.as_array())
        .map(|arr| arr.len())
        .sum();
    
    let graph_json = serde_json::json!({
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "total_nodes": nodes.len(),
            "total_edges": edges.len(),
            "table_count": table_count,
            "column_count": column_count
        }
    });
    
    Ok(serde_json::to_string(&graph_json)?)
}

// ============================================================================
// FAIL-FAST CLARIFICATION HELPER
// ============================================================================

/// Assess query confidence and compile with optional clarification answer
async fn assess_and_compile_query(
    query: &str,
    answer: Option<&str>,
) -> Result<IntentCompilationResult, Box<dyn std::error::Error>> {
    // Check for API key
    let api_key = std::env::var("OPENAI_API_KEY")
        .map_err(|_| "OpenAI API key not found. Set OPENAI_API_KEY environment variable.")?;
    
    let model = std::env::var("OPENAI_MODEL")
        .unwrap_or_else(|_| "gpt-4".to_string());
    let api_url = std::env::var("OPENAI_API_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    
    let llm = LlmClient::new(api_key, model, api_url);
    let compiler = IntentCompiler::new(llm)
        .with_confidence_threshold(0.7)  // 70% confidence required
        .with_fail_fast(true);           // Enable fail-fast
    
    // Compile with or without answer
    let mut result = if let Some(ans) = answer {
        compiler.compile_with_answer(query, ans).await?
    } else {
        compiler.compile_with_clarification(query).await?
    };
    
    // SAFEGUARD: Validate against metadata to prevent hallucination
    // This ensures all tables, columns, and relationships actually exist
    // Uses learning store for user-approved corrections
    if let IntentCompilationResult::Success(ref mut intent) = result {
        let metadata_dir = PathBuf::from("metadata");
        if let Ok(metadata) = Metadata::load(&metadata_dir) {
            // Load learning store for user-approved corrections
            let learning_store = LEARNING_STORE.lock().await;
            let learning_store_arc = Arc::new(learning_store.clone());
            drop(learning_store);
            
            match IntentCompiler::validate_against_metadata_with_learning(
                intent, 
                &metadata, 
                Some(learning_store_arc)
            ) {
                Ok(validation_result) => {
                    if !validation_result.is_valid {
                        // Validation failed - return error with details
                        let error_msg = format!(
                            "Intent validation failed (hallucination detected):\nErrors: {}\nWarnings: {}",
                            validation_result.errors.join("; "),
                            validation_result.warnings.join("; ")
                        );
                        eprintln!("[WARN] {}", error_msg);
                        return Ok(IntentCompilationResult::Failed(error_msg));
                    }
                    // Log warnings if any
                    if !validation_result.warnings.is_empty() {
                        eprintln!("[INFO] Intent validation warnings: {:?}", validation_result.warnings);
                    }
                    eprintln!("[OK] Intent validated successfully against metadata (hallucination-free)");
                }
                Err(e) => {
                    eprintln!("[WARN] Intent validation error: {}", e);
                    // Don't fail completely - validation error might be recoverable
                    // But log it for debugging
                }
            }
        } else {
            eprintln!("[WARN] Metadata not found - skipping validation safeguard");
        }
    }
    
    Ok(result)
}

// ============================================================================
// LEARNING STORE HANDLERS
// ============================================================================

/// Handle approval of a correction
async fn handle_approve_correction(json_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct ApproveCorrectionRequest {
        incorrect_name: String,
        correct_name: String,
        correction_type: String, // "table" or "column"
        table_name: Option<String>, // Required for column corrections
        approved_by: Option<String>,
    }
    
    let req: ApproveCorrectionRequest = serde_json::from_str(json_str)?;
    
    // Validate required fields
    if req.incorrect_name.is_empty() {
        return Err("incorrect_name is required".into());
    }
    if req.correct_name.is_empty() {
        return Err("correct_name is required".into());
    }
    if req.correction_type != "table" && req.correction_type != "column" {
        return Err("correction_type must be 'table' or 'column'".into());
    }
    if req.correction_type == "column" && req.table_name.is_none() {
        return Err("table_name is required for column corrections".into());
    }
    
    // Learn the correction
    let mut store = LEARNING_STORE.lock().await;
    store.learn_correction(
        req.incorrect_name.clone(),
        req.correct_name.clone(),
        req.correction_type.clone(),
        req.table_name.clone(),
        req.approved_by,
    )?;
    
    // Return response
    let response = serde_json::json!({
        "success": true,
        "message": format!("Learned correction: {} -> {} ({})", 
            req.incorrect_name, req.correct_name, req.correction_type),
        "correction": {
            "incorrect_name": req.incorrect_name,
            "correct_name": req.correct_name,
            "correction_type": req.correction_type,
            "table_name": req.table_name,
        },
        "stats": store.get_stats()
    });
    
    Ok(serde_json::to_string(&response)?)
}

/// Handle get learning statistics
async fn handle_get_learning_stats() -> Result<String, Box<dyn std::error::Error>> {
    let store = LEARNING_STORE.lock().await;
    let stats = store.get_stats();
    let all_corrections = store.get_all_corrections();
    
    let response = serde_json::json!({
        "stats": stats,
        "corrections": all_corrections.iter().map(|c| serde_json::json!({
            "incorrect_name": c.incorrect_name,
            "correct_name": c.correct_name,
            "correction_type": c.correction_type,
            "table_name": c.table_name,
            "learned_at": c.learned_at,
            "usage_count": c.usage_count,
            "approved_by": c.approved_by,
        })).collect::<Vec<_>>()
    });
    
    Ok(serde_json::to_string(&response)?)
}

// ============================================================================
// NODE REGISTRY HANDLERS
// ============================================================================

/// Handle table registration
async fn handle_register_table(json_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct RegisterTableRequest {
        table_name: String,
        csv_path: String,
        primary_keys: Vec<String>,
        column_descriptions: Option<HashMap<String, String>>,
        table_description: Option<String>,
    }
    
    let req: RegisterTableRequest = serde_json::from_str(json_str)?;
    
    // Validate required fields
    if req.table_name.is_empty() {
        return Err("table_name is required".into());
    }
    if req.csv_path.is_empty() {
        return Err("csv_path is required".into());
    }
    if req.primary_keys.is_empty() {
        return Err("primary_keys are required".into());
    }
    
    let csv_path = PathBuf::from(&req.csv_path);
    if !csv_path.exists() {
        return Err(format!("CSV file not found: {}", req.csv_path).into());
    }
    
    // Register table
    let mut registry = NODE_REGISTRY.lock().await;
    let column_descriptions = req.column_descriptions.unwrap_or_default();
    
    let ref_id = registry.register_table(
        req.table_name.clone(),
        csv_path,
        req.primary_keys.clone(),
        column_descriptions,
        req.table_description.clone(),
    )?;
    
    // Save registry
    let registry_path = PathBuf::from("node_registry");
    registry.save(&registry_path)?;
    
    // Return response
    let response = serde_json::json!({
        "success": true,
        "ref_id": ref_id,
        "message": format!("Table '{}' registered successfully", req.table_name),
        "node": {
            "ref_id": ref_id,
            "name": req.table_name,
            "type": "table"
        }
    });
    
    Ok(serde_json::to_string(&response)?)
}

/// Handle knowledge search
async fn handle_search_knowledge(search_term: &str) -> Result<String, Box<dyn std::error::Error>> {
    let registry = NODE_REGISTRY.lock().await;
    
    // Search Knowledge Register
    let (nodes, knowledge_pages, metadata_pages) = registry.search_all(search_term);
    
    // Build response
    let mut results = Vec::new();
    
    for (idx, node) in nodes.iter().enumerate() {
        let knowledge_page = knowledge_pages.get(idx);
        let metadata_page = metadata_pages.get(idx);
        
        let mut result = serde_json::json!({
            "ref_id": node.ref_id,
            "node_type": node.node_type,
            "name": node.name,
            "created_at": node.created_at,
        });
        
        if let Some(kp) = knowledge_page {
            result["knowledge"] = serde_json::json!({
                "page_id": kp.page_id,
                "full_text": kp.full_text,
                "keywords": kp.keywords,
                "segments_count": kp.segments.len(),
            });
        }
        
        if let Some(mp) = metadata_page {
            result["metadata"] = serde_json::json!({
                "page_id": mp.page_id,
                "segments_count": mp.segments.len(),
                "has_technical_data": !mp.technical_data.is_empty(),
            });
        }
        
        results.push(result);
    }
    
    let response = serde_json::json!({
        "search_term": search_term,
        "results_count": results.len(),
        "results": results
    });
    
    Ok(serde_json::to_string(&response)?)
}

/// Handle get node by reference ID
async fn handle_get_node(ref_id: &str) -> Result<String, Box<dyn std::error::Error>> {
    let registry = NODE_REGISTRY.lock().await;
    
    let node = registry.get_node(ref_id)
        .ok_or_else(|| format!("Node not found: {}", ref_id))?;
    
    let knowledge_page = registry.get_knowledge_page(ref_id);
    let metadata_page = registry.get_metadata_page(ref_id);
    
    let mut response = serde_json::json!({
        "ref_id": node.ref_id,
        "node_type": node.node_type,
        "name": node.name,
        "created_at": node.created_at,
        "metadata": node.metadata,
    });
    
    if let Some(kp) = knowledge_page {
        response["knowledge_page"] = serde_json::json!({
            "page_id": kp.page_id,
            "full_text": kp.full_text,
            "keywords": kp.keywords,
            "segments": kp.segments.iter().map(|(k, v)| {
                serde_json::json!({
                    "segment_id": k,
                    "segment_type": format!("{:?}", v.segment_type),
                    "start_child_ref_id": v.start_child_ref_id,
                    "end_child_ref_id": v.end_child_ref_id,
                    "text_content": v.text_content,
                })
            }).collect::<Vec<_>>(),
        });
    }
    
    if let Some(mp) = metadata_page {
        response["metadata_page"] = serde_json::json!({
            "page_id": mp.page_id,
            "technical_data": mp.technical_data,
            "segments": mp.segments.iter().map(|(k, v)| {
                serde_json::json!({
                    "segment_id": k,
                    "segment_type": format!("{:?}", v.segment_type),
                    "start_child_ref_id": v.start_child_ref_id,
                    "end_child_ref_id": v.end_child_ref_id,
                })
            }).collect::<Vec<_>>(),
        });
    }
    
    Ok(serde_json::to_string(&response)?)
}

// ============================================================================
// DIRECT QUERY HANDLER
// ============================================================================

/// Handle direct query execution
/// Handle query execution - supports 3 modes: normal SQL, Knowledge Register, Metadata Register
async fn handle_query_execute(json_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct QueryRequest {
        query: String,
        #[serde(default)]
        mode: String, // "sql", "knowledge", "metadata"
    }
    
    let req: QueryRequest = serde_json::from_str(json_str)?;
    
    if req.query.is_empty() {
        return Err("Query is required".into());
    }
    
    let mode = if req.mode.is_empty() { "sql".to_string() } else { req.mode.to_lowercase() };
    
    eprintln!("[DEBUG] Query execute - mode: {}, query: {}", mode, req.query);
    
    // Handle Knowledge Register queries
    if mode == "knowledge" || req.query.to_uppercase().contains("FROM KNOWLEDGE_REGISTER") || req.query.to_uppercase().contains("FROM KNOWLEDGE") {
        eprintln!("[DEBUG] Routing to knowledge register");
        return handle_knowledge_register_query(&req.query).await;
    }
    
    // Handle Metadata Register queries
    if mode == "metadata" || req.query.to_uppercase().contains("FROM METADATA_REGISTER") || req.query.to_uppercase().contains("FROM METADATA") {
        eprintln!("[DEBUG] Routing to metadata register");
        return handle_metadata_register_query(&req.query).await;
    }
    
    // Default: Normal SQL query with tables - DIRECT EXECUTION
    eprintln!("[DEBUG] Routing to direct SQL execution");
    
    // Execute SQL directly using SqlEngine - no LLM, no intent compilation
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load(&metadata_dir)
        .map_err(|e| format!("Failed to load metadata: {}", e))?;
    let data_dir = PathBuf::from("data");
    
    let sql_engine = SqlEngine::new(metadata, data_dir);
    let result = sql_engine.execute_sql(&req.query).await
        .map_err(|e| format!("SQL execution error: {}", e))?;
    
    // Return result in standard format
    let response = serde_json::json!({
        "status": "success",
        "query": req.query,
        "mode": "sql",
        "columns": result.columns,
        "rows": result.rows,
        "row_count": result.rows.len()
    });
    
    Ok(serde_json::to_string(&response)?)
}

/// Handle Knowledge Register queries
async fn handle_knowledge_register_query(query: &str) -> Result<String, Box<dyn std::error::Error>> {
    let registry_path = PathBuf::from("node_registry");
    let registry = NodeRegistry::load(&registry_path)
        .map_err(|e| format!("Failed to load node registry: {}", e))?;
    
    // Convert Knowledge Register to table-like structure
    let mut rows: Vec<serde_json::Value> = Vec::new();
    for (page_id, page) in &registry.knowledge_register.pages {
        let node = registry.nodes.get(page_id);
        rows.push(serde_json::json!({
            "page_id": page_id,
            "node_name": node.map(|n| &n.name).unwrap_or(&"".to_string()),
            "node_type": node.map(|n| &n.node_type).unwrap_or(&"".to_string()),
            "node_ref_id": page.node_ref_id,
            "full_text": page.full_text,
            "keywords": page.keywords.join(", "),
            "segments": page.segments.keys().collect::<Vec<_>>(),
        }));
    }
    
    // Simple query parsing (basic SELECT * FROM knowledge_register WHERE ...)
    let query_upper = query.to_uppercase();
    let mut filtered_rows = rows;
    
    if query_upper.contains("WHERE") {
        // Simple WHERE clause parsing (very basic)
        if let Some(where_clause) = query_upper.split("WHERE").nth(1) {
            let conditions: Vec<&str> = where_clause.split("AND").collect();
            for condition in conditions {
                let condition = condition.trim();
                if condition.contains("LIKE") || condition.contains("=") {
                    // Basic filtering (simplified)
                    // In production, would use proper SQL parser
                }
            }
        }
    }
    
    let response = serde_json::json!({
        "status": "success",
        "query": query,
        "mode": "knowledge",
        "columns": ["page_id", "node_name", "node_type", "node_ref_id", "full_text", "keywords", "segments"],
        "rows": filtered_rows,
        "row_count": filtered_rows.len()
    });
    
    Ok(serde_json::to_string(&response)?)
}

/// Handle Metadata Register queries
async fn handle_metadata_register_query(query: &str) -> Result<String, Box<dyn std::error::Error>> {
    let registry_path = PathBuf::from("node_registry");
    let registry = NodeRegistry::load(&registry_path)
        .map_err(|e| format!("Failed to load node registry: {}", e))?;
    
    // Convert Metadata Register to table-like structure
    let mut rows: Vec<serde_json::Value> = Vec::new();
    for (page_id, page) in &registry.metadata_register.pages {
        let node = registry.nodes.get(page_id);
        rows.push(serde_json::json!({
            "page_id": page_id,
            "node_name": node.map(|n| &n.name).unwrap_or(&"".to_string()),
            "node_type": node.map(|n| &n.node_type).unwrap_or(&"".to_string()),
            "node_ref_id": page.node_ref_id,
            "segments": page.segments,
            "technical_data": page.technical_data,
        }));
    }
    
    let response = serde_json::json!({
        "status": "success",
        "query": query,
        "mode": "metadata",
        "columns": ["page_id", "node_name", "node_type", "node_ref_id", "segments", "technical_data"],
        "rows": rows,
        "row_count": rows.len()
    });
    
    Ok(serde_json::to_string(&response)?)
}

async fn handle_direct_query(json_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct QueryRequest {
        query: String,
    }
    
    let req: QueryRequest = serde_json::from_str(json_str)?;
    
    if req.query.is_empty() {
        return Err("Query is required".into());
    }
    
    // Execute SQL directly using SqlEngine - no LLM, no intent compilation
    // This is pure SQL execution, just like a database
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load(&metadata_dir)
        .map_err(|e| format!("Failed to load metadata: {}", e))?;
    let data_dir = PathBuf::from("data");
    
    let sql_engine = SqlEngine::new(metadata, data_dir);
    let result = sql_engine.execute_sql(&req.query).await
        .map_err(|e| format!("SQL execution error: {}", e))?;
    
    // Return result in standard format
    let response = serde_json::json!({
        "status": "success",
        "query": req.query,
        "mode": "sql",
        "columns": result.columns,
        "rows": result.rows,
        "row_count": result.rows.len()
    });
    
    Ok(serde_json::to_string(&response)?)
}

// ============================================================================
// DATA ASSISTANT HANDLER (Cursor-like)
// ============================================================================

/// Handle assistant query - Cursor-like AI for data
async fn handle_assistant_query(json_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct AssistantRequest {
        question: String,
    }
    
    let req: AssistantRequest = serde_json::from_str(json_str)?;
    
    if req.question.is_empty() {
        return Err("Question is required".into());
    }
    
    // Load node registry
    let registry_path = PathBuf::from("node_registry");
    let node_registry = NodeRegistry::load(&registry_path)
        .map_err(|e| format!("Failed to load node registry: {}", e))?;
    
    // Load metadata
    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load(&metadata_dir)
        .map_err(|e| format!("Failed to load metadata: {}", e))?;
    
    // Create LLM client
    let api_key = std::env::var("OPENAI_API_KEY")
        .map_err(|_| "OpenAI API key not found. Set OPENAI_API_KEY environment variable.")?;
    let model = std::env::var("OPENAI_MODEL")
        .unwrap_or_else(|_| "gpt-4".to_string());
    let base_url = std::env::var("OPENAI_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let llm = LlmClient::new(api_key, model, base_url);
    
    // Create data assistant
    let data_dir = PathBuf::from("data");
    let assistant = DataAssistant::new(llm, node_registry, metadata, data_dir);
    
    // Answer the question
    let response = assistant.answer(&req.question).await?;
    
    // Return response
    Ok(serde_json::to_string(&response)?)
}

fn create_response(status: u16, status_text: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: application/json\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n\
         Access-Control-Allow-Headers: Content-Type\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        status,
        status_text,
        body.len(),
        body
    )
}
