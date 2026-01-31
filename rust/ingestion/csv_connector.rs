//! CSV Connector - Accepts raw CSV payloads for ingestion

use crate::ingestion::connector::{Checkpoint, ConnectorResult, IngestionConnector};
use anyhow::{Context, Result};
use csv::ReaderBuilder;
use serde_json::{Map, Value};

/// CSV Connector - Wraps provided CSV text and converts it into JSON records.
pub struct CsvConnector {
    source_id: String,
    source_uri: Option<String>,
    schema_name: Option<String>,
    csv_text: String,
    consumed: bool,
}

impl CsvConnector {
    pub fn new(source_id: String, csv_text: String) -> Self {
        Self::new_with_schema(source_id, None, csv_text)
    }
    
    pub fn new_with_schema(source_id: String, schema_name: Option<String>, csv_text: String) -> Self {
        Self {
            source_id,
            source_uri: None,
            schema_name,
            csv_text,
            consumed: false,
        }
    }
    
    pub fn with_source_uri(source_id: String, source_uri: Option<String>, csv_text: String) -> Self {
        Self::with_source_uri_and_schema(source_id, source_uri, None, csv_text)
    }
    
    pub fn with_source_uri_and_schema(
        source_id: String, 
        source_uri: Option<String>, 
        schema_name: Option<String>,
        csv_text: String
    ) -> Self {
        Self {
            source_id,
            source_uri,
            schema_name,
            csv_text,
            consumed: false,
        }
    }
    
    pub fn effective_schema_name(&self) -> String {
        self.schema_name.clone().unwrap_or_else(|| "main".to_string())
    }

    fn coerce_cell(s: &str) -> Value {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Value::Null;
        }

        if trimmed.eq_ignore_ascii_case("true") {
            return Value::Bool(true);
        }
        if trimmed.eq_ignore_ascii_case("false") {
            return Value::Bool(false);
        }

        if let Ok(i) = trimmed.parse::<i64>() {
            return Value::Number(i.into());
        }

        if let Ok(f) = trimmed.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Value::Number(n);
            }
        }

        Value::String(trimmed.to_string())
    }

    fn parse_csv_to_json(&self) -> Result<Vec<Value>> {
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(self.csv_text.as_bytes());

        let headers = rdr
            .headers()
            .context("Failed to read CSV headers")?
            .iter()
            .map(|h| h.trim().to_string())
            .collect::<Vec<_>>();

        let mut out = Vec::new();
        for result in rdr.records() {
            let record = result.context("Failed to read CSV record")?;
            let mut obj = Map::new();

            for (idx, header) in headers.iter().enumerate() {
                let cell = record.get(idx).unwrap_or("");
                obj.insert(header.clone(), Self::coerce_cell(cell));
            }

            out.push(Value::Object(obj));
        }

        Ok(out)
    }
}

impl IngestionConnector for CsvConnector {
    fn fetch(&mut self, _checkpoint: Option<Checkpoint>) -> Result<ConnectorResult> {
        if self.consumed {
            return Ok(ConnectorResult {
                payloads: vec![],
                checkpoint: Checkpoint::new("done".to_string()),
                has_more: false,
            });
        }

        let payloads = self.parse_csv_to_json()?;
        self.consumed = true;

        Ok(ConnectorResult {
            payloads,
            checkpoint: Checkpoint::new("done".to_string()),
            has_more: false,
        })
    }

    fn source_id(&self) -> &str {
        &self.source_id
    }

    fn source_type(&self) -> &str {
        "csv"
    }

    fn source_uri(&self) -> Option<&str> {
        self.source_uri.as_deref()
    }
    
    fn schema_name(&self) -> Option<String> {
        self.schema_name.clone()
    }
}

