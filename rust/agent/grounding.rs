use crate::agent::catalog::CatalogIndex;
use crate::agent::tools::{kb_tools, table_tools};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroundingProvenance {
    #[serde(default)]
    pub selected_tables: Vec<String>,
    #[serde(default)]
    pub kb_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroundingContext {
    pub provenance: GroundingProvenance,
    pub schemas: serde_json::Value,
    pub samples: serde_json::Value,
    pub knowledge: serde_json::Value,
}

/// Build lightweight grounding context from metadata + small CSV samples.
///
/// Guardrails:
/// - Uses metadata first
/// - Limits to top_k tables
/// - Samples only a few rows per table
pub fn build_grounding_context(
    catalog: &CatalogIndex,
    query: &str,
    top_k: usize,
    sample_rows: usize,
) -> Result<GroundingContext> {
    let top_k = top_k.max(1).min(3);
    let sample_rows = sample_rows.max(1).min(10);

    let search = table_tools::search_tables(catalog, query, None, top_k);
    let mut selected_tables: Vec<String> = Vec::new();
    if let Some(arr) = search.get("results").and_then(|v| v.as_array()) {
        for r in arr {
            if let Some(id) = r.get("id").and_then(|v| v.as_str()) {
                selected_tables.push(id.to_string());
            }
        }
    }

    let mut schema_objs = Vec::new();
    let mut sample_objs = Vec::new();
    for tid in &selected_tables {
        if let Ok(t) = catalog.resolve_table(tid) {
            schema_objs.push(table_tools::show_schema(catalog, &t));
            if let Ok(h) = table_tools::head(catalog, tid, sample_rows) {
                sample_objs.push(h);
            }
        }
    }

    // Naive KB probe: use query as key; in later phases we’ll do keyword extraction + embeddings.
    let kb = kb_tools::open_knowledge(&catalog.metadata_dir, query).unwrap_or_else(|_| {
        json!({"found": false, "key": query, "hits": []})
    });

    Ok(GroundingContext {
        provenance: GroundingProvenance {
            selected_tables: selected_tables.clone(),
            kb_keys: vec![query.to_string()],
        },
        schemas: json!({ "schemas": schema_objs }),
        samples: json!({ "samples": sample_objs }),
        knowledge: kb,
    })
}






