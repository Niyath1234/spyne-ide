use crate::agent::catalog::{CatalogIndex, TableSummary};
use crate::error::{RcaError, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use strsim::jaro_winkler;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedItem {
    pub id: String,
    pub label: String,
    pub score: f64,
    #[serde(default)]
    pub meta: HashMap<String, serde_json::Value>,
}

pub fn list_systems(catalog: &CatalogIndex) -> serde_json::Value {
    json!({
        "systems": catalog.systems.iter().map(|s| json!({
            "system_id": s.system_id,
            "label": s.label,
            "aliases": s.aliases,
        })).collect::<Vec<_>>()
    })
}

pub fn search_systems(catalog: &CatalogIndex, query: &str, limit: usize) -> serde_json::Value {
    let q = query.to_lowercase();
    let mut ranked: Vec<RankedItem> = catalog
        .systems
        .iter()
        .map(|s| {
            let mut best = jaro_winkler(&q, &s.system_id.to_lowercase());
            best = best.max(jaro_winkler(&q, &s.label.to_lowercase()));
            for a in &s.aliases {
                best = best.max(jaro_winkler(&q, &a.to_lowercase()));
            }
            RankedItem {
                id: s.system_id.clone(),
                label: s.label.clone(),
                score: best,
                meta: HashMap::new(),
            }
        })
        .collect();
    ranked.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(limit.max(1).min(50));
    json!({ "results": ranked })
}

pub fn list_tables(catalog: &CatalogIndex, system: &str) -> serde_json::Value {
    let tables = catalog
        .tables_by_system
        .get(system)
        .cloned()
        .unwrap_or_default();
    json!({
        "system": system,
        "tables": tables
    })
}

pub fn search_tables(catalog: &CatalogIndex, query: &str, system: Option<&str>, limit: usize) -> serde_json::Value {
    let q = query.to_lowercase();
    let mut items: Vec<RankedItem> = Vec::new();

    let iter: Box<dyn Iterator<Item = &TableSummary>> = if let Some(sys) = system {
        let v = catalog.tables_by_system.get(sys).map(|v| v.as_slice()).unwrap_or(&[]);
        Box::new(v.iter())
    } else {
        Box::new(catalog.tables.iter())
    };

    for t in iter {
        let mut score: f64 = 0.0;
        score = score.max(jaro_winkler(&q, &t.name.to_lowercase()));
        score = score.max(jaro_winkler(&q, &t.entity.to_lowercase()));
        score = score.max(jaro_winkler(&q, &t.system.to_lowercase()));
        for c in &t.column_names {
            score = score.max(jaro_winkler(&q, &c.to_lowercase()) * 0.9);
        }
        for l in &t.labels {
            score = score.max(jaro_winkler(&q, &l.to_lowercase()) * 0.9);
        }
        if score > 0.35 {
            let mut meta = HashMap::new();
            meta.insert("system".to_string(), json!(t.system));
            meta.insert("entity".to_string(), json!(t.entity));
            meta.insert("path".to_string(), json!(t.path));
            items.push(RankedItem {
                id: t.table_id.clone(),
                label: t.name.clone(),
                score,
                meta,
            });
        }
    }

    items.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    items.truncate(limit.max(1).min(100));

    json!({ "results": items })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumn {
    pub name: String,
    #[serde(default)]
    pub data_type: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

pub fn show_schema(catalog: &CatalogIndex, table: &TableSummary) -> serde_json::Value {
    let meta_table = catalog
        .metadata
        .tables_by_name
        .get(&table.name)
        .cloned();

    let mut cols: Vec<SchemaColumn> = Vec::new();
    if let Some(t) = meta_table {
        if let Some(md_cols) = t.columns {
            cols = md_cols
                .into_iter()
                .map(|c| SchemaColumn {
                    name: c.name,
                    data_type: c.data_type,
                    description: c.description,
                })
                .collect();
        }
    }

    json!({
        "table_id": table.table_id,
        "name": table.name,
        "system": table.system,
        "entity": table.entity,
        "path": table.path,
        "primary_key": table.primary_key,
        "time_column": table.time_column,
        "columns": cols,
    })
}

pub fn open_table(catalog: &CatalogIndex, table_id_or_name: &str) -> Result<serde_json::Value> {
    let table = catalog.resolve_table(table_id_or_name)?;
    Ok(json!({
        "table": table,
        "schema": show_schema(catalog, &table),
    }))
}

fn resolve_table_path(catalog: &CatalogIndex, table: &TableSummary) -> std::path::PathBuf {
    // metadata.tables[].path typically like "system_x/foo.csv" relative to data_dir
    let p = std::path::PathBuf::from(&table.path);
    if p.is_absolute() {
        p
    } else {
        catalog.data_dir.join(p)
    }
}

pub fn head(catalog: &CatalogIndex, table_id_or_name: &str, n: usize) -> Result<serde_json::Value> {
    let n = n.max(1).min(200);
    let table = catalog.resolve_table(table_id_or_name)?;
    let path = resolve_table_path(catalog, &table);
    if !path.exists() {
        return Err(RcaError::Execution(format!(
            "CSV file not found for table '{}': {}",
            table.name,
            path.display()
        )));
    }

    let file = File::open(&path)?;
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(file);
    let headers = rdr
        .headers()
        .map_err(|e| RcaError::Execution(format!("Failed to read CSV headers: {}", e)))?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let mut rows: Vec<Vec<String>> = Vec::new();
    for (i, rec) in rdr.records().enumerate() {
        if i >= n {
            break;
        }
        let rec = rec.map_err(|e| RcaError::Execution(format!("Failed to read CSV row: {}", e)))?;
        rows.push(rec.iter().map(|s| s.to_string()).collect());
    }

    Ok(json!({
        "table": { "id": table.table_id, "name": table.name, "path": table.path, "system": table.system },
        "headers": headers,
        "rows": rows,
        "row_count": rows.len()
    }))
}

pub fn tail(catalog: &CatalogIndex, table_id_or_name: &str, n: usize, max_scan_rows: usize) -> Result<serde_json::Value> {
    let n = n.max(1).min(200);
    let max_scan_rows = max_scan_rows.max(n).min(50_000);
    let table = catalog.resolve_table(table_id_or_name)?;
    let path = resolve_table_path(catalog, &table);
    if !path.exists() {
        return Err(RcaError::Execution(format!(
            "CSV file not found for table '{}': {}",
            table.name,
            path.display()
        )));
    }

    let file = File::open(&path)?;
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(file);
    let headers = rdr
        .headers()
        .map_err(|e| RcaError::Execution(format!("Failed to read CSV headers: {}", e)))?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    // Ring buffer of last n rows within scan limit
    let mut ring: std::collections::VecDeque<Vec<String>> = std::collections::VecDeque::with_capacity(n);
    for (i, rec) in rdr.records().enumerate() {
        if i >= max_scan_rows {
            break;
        }
        let rec = rec.map_err(|e| RcaError::Execution(format!("Failed to read CSV row: {}", e)))?;
        let row = rec.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(row);
    }
    let rows: Vec<Vec<String>> = ring.into_iter().collect();

    Ok(json!({
        "table": { "id": table.table_id, "name": table.name, "path": table.path, "system": table.system },
        "headers": headers,
        "rows": rows,
        "row_count": rows.len(),
        "note": format!("tail is computed from the first {} data rows (bounded scan for safety)", max_scan_rows)
    }))
}

pub fn search_columns(catalog: &CatalogIndex, pattern: &str, system: Option<&str>, limit: usize) -> serde_json::Value {
    let q = pattern.to_lowercase();
    let mut results: Vec<RankedItem> = Vec::new();

    let iter: Box<dyn Iterator<Item = &TableSummary>> = if let Some(sys) = system {
        let v = catalog.tables_by_system.get(sys).map(|v| v.as_slice()).unwrap_or(&[]);
        Box::new(v.iter())
    } else {
        Box::new(catalog.tables.iter())
    };

    for t in iter {
        for c in &t.column_names {
            let score = jaro_winkler(&q, &c.to_lowercase());
            if score > 0.5 || c.to_lowercase().contains(&q) {
                let mut meta = HashMap::new();
                meta.insert("table".to_string(), json!(t.name));
                meta.insert("system".to_string(), json!(t.system));
                results.push(RankedItem {
                    id: format!("{}::{}", t.name, c),
                    label: format!("{}.{}", t.name, c),
                    score,
                    meta,
                });
            }
        }
    }

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(limit.max(1).min(200));
    json!({ "results": results })
}

pub fn search_values(
    catalog: &CatalogIndex,
    table_id_or_name: &str,
    pattern: &str,
    column: Option<&str>,
    sample_limit: usize,
) -> Result<serde_json::Value> {
    let sample_limit = sample_limit.max(1).min(1000);
    let needle = pattern.to_lowercase();
    let table = catalog.resolve_table(table_id_or_name)?;
    let path = resolve_table_path(catalog, &table);
    if !path.exists() {
        return Err(RcaError::Execution(format!(
            "CSV file not found for table '{}': {}",
            table.name,
            path.display()
        )));
    }

    let file = File::open(&path)?;
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(file);
    let headers = rdr
        .headers()
        .map_err(|e| RcaError::Execution(format!("Failed to read CSV headers: {}", e)))?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    let col_idx = column.and_then(|col| headers.iter().position(|h| h == col));

    let mut matches: Vec<serde_json::Value> = Vec::new();
    for (i, rec) in rdr.records().enumerate() {
        if i >= sample_limit {
            break;
        }
        let rec = rec.map_err(|e| RcaError::Execution(format!("Failed to read CSV row: {}", e)))?;
        if let Some(idx) = col_idx {
            if let Some(v) = rec.get(idx) {
                if v.to_lowercase().contains(&needle) {
                    matches.push(json!({ "row": i, "column": headers[idx], "value": v }));
                }
            }
        } else {
            for (j, v) in rec.iter().enumerate() {
                if v.to_lowercase().contains(&needle) {
                    matches.push(json!({ "row": i, "column": headers.get(j).cloned().unwrap_or_default(), "value": v }));
                }
            }
        }
    }

    Ok(json!({
        "table": { "id": table.table_id, "name": table.name, "system": table.system },
        "pattern": pattern,
        "column": column,
        "scanned_rows": sample_limit,
        "matches": matches,
        "match_count": matches.len()
    }))
}

pub fn sniff_csv_header_only(catalog: &CatalogIndex, table_id_or_name: &str) -> Result<serde_json::Value> {
    let table = catalog.resolve_table(table_id_or_name)?;
    let path = resolve_table_path(catalog, &table);
    if !path.exists() {
        return Err(RcaError::Execution(format!(
            "CSV file not found for table '{}': {}",
            table.name,
            path.display()
        )));
    }

    let file = File::open(&path)?;
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(file);
    let headers = rdr
        .headers()
        .map_err(|e| RcaError::Execution(format!("Failed to read CSV headers: {}", e)))?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    Ok(json!({
        "table": { "id": table.table_id, "name": table.name, "system": table.system },
        "headers": headers
    }))
}

pub fn file_tail_lines(path: &std::path::Path, n: usize, max_bytes: usize) -> Result<Vec<String>> {
    let n = n.max(1).min(200);
    let max_bytes = max_bytes.max(1024).min(5_000_000);
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut ring: std::collections::VecDeque<String> = std::collections::VecDeque::with_capacity(n);
    let mut bytes_read = 0usize;
    for line in reader.lines() {
        let line = line?;
        bytes_read += line.len();
        if bytes_read > max_bytes {
            break;
        }
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(line);
    }
    Ok(ring.into_iter().collect())
}


