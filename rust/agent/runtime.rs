use crate::agent::catalog::CatalogIndex;
use crate::agent::tools::{kb_tools, table_tools};
use crate::error::{RcaError, Result};
use serde_json::Value;

pub struct ToolRuntime {
    pub catalog: CatalogIndex,
}

impl ToolRuntime {
    pub fn new(catalog: CatalogIndex) -> Self {
        Self { catalog }
    }

    pub fn execute(&self, tool_name: &str, args: &Value) -> Result<Value> {
        match tool_name {
            "list_systems" => Ok(table_tools::list_systems(&self.catalog)),
            "search_systems" => {
                let q = args.get("query").and_then(|v| v.as_str()).unwrap_or("");
                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
                Ok(table_tools::search_systems(&self.catalog, q, limit))
            }
            "list_tables" => {
                let system = args.get("system").and_then(|v| v.as_str()).unwrap_or("");
                Ok(table_tools::list_tables(&self.catalog, system))
            }
            "search_tables" => {
                let q = args.get("query").and_then(|v| v.as_str()).unwrap_or("");
                let system = args.get("system").and_then(|v| v.as_str());
                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
                Ok(table_tools::search_tables(&self.catalog, q, system, limit))
            }
            "open_table" => {
                let tid = args
                    .get("table_id")
                    .or_else(|| args.get("table"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                table_tools::open_table(&self.catalog, tid)
            }
            "show_schema" => {
                let tid = args
                    .get("table_id")
                    .or_else(|| args.get("table"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let t = self.catalog.resolve_table(tid)?;
                Ok(table_tools::show_schema(&self.catalog, &t))
            }
            "head" => {
                let tid = args.get("table_id").and_then(|v| v.as_str()).unwrap_or("");
                let n = args.get("n").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
                table_tools::head(&self.catalog, tid, n)
            }
            "tail" => {
                let tid = args.get("table_id").and_then(|v| v.as_str()).unwrap_or("");
                let n = args.get("n").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
                let max_scan_rows = args.get("max_scan_rows").and_then(|v| v.as_u64()).unwrap_or(5000) as usize;
                table_tools::tail(&self.catalog, tid, n, max_scan_rows)
            }
            "search_columns" => {
                let q = args.get("pattern").and_then(|v| v.as_str()).unwrap_or("");
                let system = args.get("system").and_then(|v| v.as_str());
                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
                Ok(table_tools::search_columns(&self.catalog, q, system, limit))
            }
            "search_values" => {
                let tid = args.get("table_id").and_then(|v| v.as_str()).unwrap_or("");
                let pat = args.get("pattern").and_then(|v| v.as_str()).unwrap_or("");
                let col = args.get("column").and_then(|v| v.as_str());
                let sample_limit = args.get("sample_limit").and_then(|v| v.as_u64()).unwrap_or(200) as usize;
                table_tools::search_values(&self.catalog, tid, pat, col, sample_limit)
            }
            "open_knowledge" => {
                let key = args.get("key").and_then(|v| v.as_str()).unwrap_or("");
                kb_tools::open_knowledge(&self.catalog.metadata_dir, key)
            }
            other => Err(RcaError::Execution(format!("Unknown tool '{}'", other))),
        }
    }
}






