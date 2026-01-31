use crate::error::{RcaError, Result};
use serde_json::json;
use std::path::{Path, PathBuf};

fn default_kb_path(metadata_dir: &Path) -> PathBuf {
    metadata_dir.join("knowledge_base.json")
}

pub fn open_knowledge(metadata_dir: &Path, key: &str) -> Result<serde_json::Value> {
    let kb_path = default_kb_path(metadata_dir);
    if !kb_path.exists() {
        return Ok(json!({
            "found": false,
            "message": "knowledge_base.json not found in metadata directory",
            "key": key
        }));
    }

    let content = std::fs::read_to_string(&kb_path)?;
    let kb: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| RcaError::Execution(format!("Failed to parse knowledge base JSON: {}", e)))?;

    // Common KB layouts in this repo: { terms: {...}, tables: {...}, relationships: {...} }
    let key_lc = key.to_lowercase();
    let mut hits = Vec::new();

    for section in ["terms", "tables", "relationships"] {
        if let Some(obj) = kb.get(section).and_then(|v| v.as_object()) {
            for (k, v) in obj {
                if k.to_lowercase().contains(&key_lc) {
                    hits.push(json!({
                        "section": section,
                        "key": k,
                        "value": v
                    }));
                }
            }
        }
    }

    Ok(json!({
        "found": !hits.is_empty(),
        "key": key,
        "hits": hits
    }))
}






