//! Python Bindings Library - Main entry point for PyO3 module
//! 
//! This creates the Python module `spyne_execution` that can be imported in Python.

use pyo3::prelude::*;
use crate::python_bindings::execution::PythonExecutionRouter;
use crate::python_bindings::profile::PythonQueryProfile;
use crate::python_bindings::agent_decision::agent_select_engine_py;

/// Python module for Spyne execution engines
#[pymodule]
#[pyo3(name = "spyne_execution")]
fn spyne_execution(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PythonExecutionRouter>()?;
    m.add_class::<PythonQueryProfile>()?;
    m.add_function(wrap_pyfunction!(agent_select_engine_py, m)?)?;
    
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    
    Ok(())
}

