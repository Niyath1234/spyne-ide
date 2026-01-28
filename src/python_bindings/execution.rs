//! Python bindings for ExecutionRouter

#[cfg(feature = "python-bindings")]
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use crate::execution::router::ExecutionRouter;
use crate::execution::engine::{ExecutionEngine, EngineSelection};
use crate::execution::profile::QueryProfile;
use crate::execution::result::QueryResult;
use crate::execution::duckdb_engine::DuckDbEngine;
use crate::execution::trino_engine::TrinoEngine;
use crate::execution::polars_engine::PolarsEngine;
use std::path::PathBuf;
use crate::error::Result;
use std::sync::Arc;
use serde_json;

/// Python wrapper for ExecutionRouter
#[cfg(feature = "python-bindings")]
#[pyclass]
pub struct PythonExecutionRouter {
    router: ExecutionRouter,
}

#[pymethods]
impl PythonExecutionRouter {
    /// Create a new execution router with specified engines
    /// 
    /// Args:
    ///     engines: List of engine names (e.g., ["duckdb", "trino", "polars"])
    ///     data_dir: Optional data directory path (defaults to "./data")
    ///     trino_config: Optional dict with Trino config (coordinator_url, catalog, schema, user)
    #[new]
    #[args(data_dir = "None", trino_config = "None")]
    fn new(
        engines: Vec<String>,
        data_dir: Option<String>,
        trino_config: Option<&PyDict>,
    ) -> PyResult<Self> {
        let data_path = data_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data"));
        
        let mut rust_engines: Vec<Arc<dyn ExecutionEngine>> = Vec::new();
        
        for engine_name in engines {
            match engine_name.as_str() {
                "duckdb" => {
                    rust_engines.push(Arc::new(DuckDBEngine::new(data_path.clone())));
                }
                "trino" => {
                    // Extract Trino config from Python dict
                    let coordinator_url = trino_config
                        .and_then(|c| c.get_item("coordinator_url"))
                        .and_then(|v| v.extract::<String>().ok())
                        .unwrap_or_else(|| std::env::var("TRINO_COORDINATOR_URL")
                            .unwrap_or_else(|_| "http://localhost:8080".to_string()));
                    
                    let catalog = trino_config
                        .and_then(|c| c.get_item("catalog"))
                        .and_then(|v| v.extract::<String>().ok())
                        .unwrap_or_else(|| std::env::var("TRINO_CATALOG")
                            .unwrap_or_else(|_| "memory".to_string()));
                    
                    let schema = trino_config
                        .and_then(|c| c.get_item("schema"))
                        .and_then(|v| v.extract::<String>().ok())
                        .unwrap_or_else(|| std::env::var("TRINO_SCHEMA")
                            .unwrap_or_else(|_| "default".to_string()));
                    
                    let user = trino_config
                        .and_then(|c| c.get_item("user"))
                        .and_then(|v| v.extract::<String>().ok())
                        .unwrap_or_else(|| std::env::var("TRINO_USER")
                            .unwrap_or_else(|_| "python_user".to_string()));
                    
                    match TrinoEngine::new(coordinator_url, catalog, schema, user) {
                        Ok(engine) => rust_engines.push(Arc::new(engine)),
                        Err(e) => {
                            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                                format!("Failed to create Trino engine: {}", e)
                            ));
                        }
                    }
                }
                "polars" => {
                    rust_engines.push(Arc::new(PolarsEngine::new(data_path.clone())));
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!("Unknown engine: {}", engine_name)
                    ));
                }
            }
        }
        
        Ok(Self {
            router: ExecutionRouter::new(rust_engines),
        })
    }
    
    /// Get available engines
    fn available_engines(&self) -> Vec<String> {
        self.router.available_engines()
    }
    
    /// Suggest engine based on SQL query
    fn suggest_engine(&self, sql: &str) -> PyResult<Vec<PyObject>> {
        Python::with_gil(|py| {
            let profile = match QueryProfile::from_sql(sql) {
                Ok(p) => p,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!("Failed to parse SQL: {}", e)
                    ));
                }
            };
            
            let suggestions = match self.router.suggest_engine(&profile) {
                Ok(s) => s,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to suggest engine: {}", e)
                    ));
                }
            };
            
            let mut result = Vec::new();
            for suggestion in suggestions {
                let dict = PyDict::new(py);
                dict.set_item("engine_name", suggestion.engine.clone())?;
                dict.set_item("score", suggestion.score)?;
                dict.set_item("reasons", suggestion.reasons.clone())?;
                dict.set_item("estimated_time_ms", suggestion.estimated_time_ms)?;
                result.push(dict.to_object(py));
            }
            
            Ok(result)
        })
    }
    
    /// Execute query with selected engine
    fn execute(&self, sql: &str, engine_selection: &PyDict) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            // Extract engine selection from Python dict
            let engine_name: String = engine_selection.get_item("engine_name")
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("Missing 'engine_name'"))?
                .extract()?;
            
            let reasoning: Vec<String> = engine_selection.get_item("reasoning")
                .and_then(|r| r.extract().ok())
                .unwrap_or_default();
            
            let fallback_available: bool = engine_selection.get_item("fallback_available")
                .and_then(|f| f.extract().ok())
                .unwrap_or(false);
            
            // Create engine selection
            let selection = EngineSelection {
                engine_name: engine_name.clone(),
                reasoning,
                fallback_available,
            };
            
            // Get query profile
            let profile = match QueryProfile::from_sql(sql) {
                Ok(p) => p,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!("Failed to parse SQL: {}", e)
                    ));
                }
            };
            
            // Create execution context
            let ctx = crate::execution::engine::ExecutionContext {
                user: crate::execution::engine::UserContext {
                    user_id: "python_user".to_string(),
                    roles: vec![],
                },
                timeout_ms: 30000,
                row_limit: None,
                preview: false,
                params: std::collections::HashMap::new(),
            };
            
            // Execute query (async, so we need to block on it)
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to create runtime: {}", e)
                )
            })?;
            
            let result = rt.block_on(async {
                self.router.execute(sql, &profile, &ctx, &selection).await
            });
            
            let result = match result {
                Ok(r) => r,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Execution failed: {}", e)
                    ));
                }
            };
            
            // Convert QueryResult to Python dict
            let result_dict = PyDict::new(py);
            result_dict.set_item("success", result.success)?;
            result_dict.set_item("rows_returned", result.row_count)?;
            result_dict.set_item("rows_scanned", result.row_count)?; // Use row_count as approximation
            result_dict.set_item("execution_time_ms", result.execution_time_ms)?;
            
            // Convert data_json to Python list
            let data_list = PyList::empty(py);
            if let Some(data_json) = &result.data_json {
                if let serde_json::Value::Array(rows) = data_json {
                    for row in rows {
                        if let serde_json::Value::Object(row_obj) = row {
                            let row_dict = PyDict::new(py);
                            for (key, value) in row_obj {
                                // Convert serde_json::Value to Python object
                                let py_value = match value {
                                    serde_json::Value::String(s) => s.to_object(py),
                                    serde_json::Value::Number(n) => {
                                        if n.is_u64() {
                                            n.as_u64().unwrap().to_object(py)
                                        } else if n.is_i64() {
                                            n.as_i64().unwrap().to_object(py)
                                        } else {
                                            n.as_f64().unwrap().to_object(py)
                                        }
                                    }
                                    serde_json::Value::Bool(b) => b.to_object(py),
                                    serde_json::Value::Null => py.None(),
                                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                                        // For complex types, serialize to JSON string
                                        serde_json::to_string(&value).unwrap().to_object(py)
                                    }
                                };
                                row_dict.set_item(key, py_value)?;
                            }
                            data_list.append(row_dict)?;
                        }
                    }
                }
            }
            result_dict.set_item("data", data_list)?;
            
            // Add columns
            result_dict.set_item("columns", result.columns.clone())?;
            
            // Add engine metadata
            let engine_metadata = PyDict::new(py);
            engine_metadata.set_item("agent_selected_engine", engine_name)?;
            // Add agent reasoning if available
            if let Some(reasoning_json) = result.engine_metadata.get("agent_reasoning") {
                if let serde_json::Value::Array(reasoning_array) = reasoning_json {
                    let reasoning: Vec<String> = reasoning_array
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    engine_metadata.set_item("agent_reasoning", reasoning)?;
                }
            }
            result_dict.set_item("engine_metadata", engine_metadata)?;
            
            Ok(result_dict.to_object(py))
        })
    }
}

