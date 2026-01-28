//! Python bindings for QueryProfile

use pyo3::prelude::*;
use pyo3::types::PyDict;
use crate::execution::profile::QueryProfile;

/// Python wrapper for QueryProfile
#[pyclass]
pub struct PythonQueryProfile {
    profile: QueryProfile,
}

#[pymethods]
impl PythonQueryProfile {
    /// Create query profile from SQL
    #[staticmethod]
    fn from_sql(sql: &str) -> PyResult<Self> {
        match QueryProfile::from_sql(sql) {
            Ok(profile) => Ok(Self { profile }),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Failed to parse SQL: {}", e)
            )),
        }
    }
    
    /// Get profile as dictionary
    fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("uses_ctes", self.profile.uses_ctes)?;
        dict.set_item("uses_window_functions", self.profile.uses_window_functions)?;
        dict.set_item("uses_case_expressions", self.profile.uses_case_expressions)?;
        dict.set_item("join_count", self.profile.join_count)?;
        dict.set_item("estimated_scan_gb", self.profile.estimated_scan_gb)?;
        dict.set_item("requires_federation", self.profile.requires_federation)?;
        dict.set_item("complexity_score", self.profile.complexity_score)?;
        dict.set_item("is_read_only", self.profile.is_read_only)?;
        Ok(dict.to_object(py))
    }
}

