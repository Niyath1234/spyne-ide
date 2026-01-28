//! Python bindings for agent decision logic

use pyo3::prelude::*;
use pyo3::types::PyDict;
use crate::execution::agent_decision::{agent_select_engine, AgentDecisionContext, EnginePreferences};
use crate::execution::router::ExecutionRouter;
use crate::execution::profile::QueryProfile;
use crate::python_bindings::execution::PythonExecutionRouter;

/// Agent-based engine selection for Python
#[pyfunction]
pub fn agent_select_engine_py(
    router: &PythonExecutionRouter,
    query: &str,
    metadata: Option<&PyDict>,
) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        // Create agent decision context
        let mut context = AgentDecisionContext {
            query: query.to_string(),
            profile: None,
            metadata: None, // TODO: Convert Python dict to Metadata if needed
            user_preferences: None,
            available_engines: router.available_engines(),
        };
        
        // Try to extract user preferences from metadata if provided
        if let Some(meta) = metadata {
            if let Ok(prefer_speed) = meta.get_item("prefer_speed").and_then(|v| v.extract::<bool>().ok()) {
                context.user_preferences = Some(EnginePreferences {
                    preferred_engine: None,
                    max_execution_time_ms: None,
                    prefer_speed,
                    allow_preview: false,
                });
            }
        }
        
        // Get the internal router (we need to expose this or refactor)
        // For now, we'll use the suggest_engine method and create selection manually
        let profile = match QueryProfile::from_sql(query) {
            Ok(p) => p,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Failed to parse SQL: {}", e)
                ));
            }
        };
        
        context.profile = Some(profile.clone());
        
        // Use the router's suggest_engine to get recommendations
        let suggestions = match router.suggest_engine(query) {
            Ok(s) => s,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to suggest engine: {}", e)
                ));
            }
        };
        
        // Select the best engine (highest score)
        if suggestions.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No engine suggestions available"
            ));
        }
        
        // Get the first suggestion (they should be sorted by score)
        let best_suggestion = suggestions.get(0).unwrap();
        let engine_name: String = best_suggestion.get_item("engine_name")?.extract()?;
        let reasons: Vec<String> = best_suggestion.get_item("reasons")?.extract()?;
        
        // Create engine selection dict
        let selection_dict = PyDict::new(py);
        selection_dict.set_item("engine_name", engine_name)?;
        selection_dict.set_item("reasoning", reasons)?;
        selection_dict.set_item("fallback_available", false)?;
        
        Ok(selection_dict.to_object(py))
    })
}

