// Import from library crate
use spyne_ide::metadata::Metadata;
use spyne_ide::llm::LlmClient;
use spyne_ide::rca::RcaEngine;
use spyne_ide::agentic_reasoner::{AgenticReasoner, ExplorationResult};
use spyne_ide::graph::Hypergraph;
use spyne_ide::one_shot_runner::OneShotRunner;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::io::{self, Write};
use tracing::{info, error};
use polars::prelude::*;
use std::fs;
use regex::Regex;

#[derive(Parser)]
#[command(name = "rca-engine")]
#[command(about = "Root Cause Analysis Engine for Data Reconciliation")]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run RCA with metadata and data directories
    Run {
        /// The reconciliation query in natural language
        query: String,
        
        /// Path to metadata directory (default: ./metadata)
        #[arg(short, long, default_value = "metadata")]
        metadata_dir: PathBuf,
        
        /// Path to data directory (default: ./data)
        #[arg(short, long, default_value = "data")]
        data_dir: PathBuf,
        
        /// OpenAI API key (or set OPENAI_API_KEY env var)
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Run RCA on two CSV files interactively
    Csv {
        /// First CSV file (System A)
        csv_a: PathBuf,
        
        /// Second CSV file (System B)
        csv_b: PathBuf,
        
        /// System A name (default: system_a)
        #[arg(long, default_value = "system_a")]
        system_a: String,
        
        /// System B name (default: system_b)
        #[arg(long, default_value = "system_b")]
        system_b: String,
        
        /// Metric column name (will be auto-detected if not provided)
        #[arg(long)]
        metric: Option<String>,
        
        /// OpenAI API key (or set OPENAI_API_KEY env var)
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Run agentic RCA reasoning (explores graph/knowledge base stage-wise)
    Agentic {
        /// The RCA problem to solve
        problem: String,
        
        /// Path to metadata directory (default: ./metadata)
        #[arg(short, long, default_value = "metadata")]
        metadata_dir: PathBuf,
        
        /// OpenAI API key (or set OPENAI_API_KEY env var)
        #[arg(long)]
        api_key: Option<String>,
    },
    /// Run one-shot agentic RCA + DV (unified system)
    OneShot {
        /// The query in natural language (RCA or DV)
        query: String,
        
        /// Path to metadata directory (default: ./metadata)
        #[arg(short, long, default_value = "metadata")]
        metadata_dir: PathBuf,
        
        /// Path to data directory (default: ./data)
        #[arg(short, long, default_value = "data")]
        data_dir: PathBuf,
        
        /// OpenAI API key (or set OPENAI_API_KEY env var)
        #[arg(long)]
        api_key: Option<String>,
        
        /// Enable explainability output
        #[arg(long)]
        explain: bool,
    },
    /// Upload a CSV file - simple interface to create nodes and edges
    Upload {
        /// CSV file to upload
        csv_file: PathBuf,
        
        /// Path to metadata directory (default: ./metadata)
        #[arg(short, long, default_value = "metadata")]
        metadata_dir: PathBuf,
        
        /// Path to data directory (default: ./data)
        #[arg(short, long, default_value = "data")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();
    
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    match args.command {
        Commands::Run { query, metadata_dir, data_dir, api_key } => {
            run_with_metadata(query, metadata_dir, data_dir, api_key).await
        }
        Commands::Csv { csv_a, csv_b, system_a, system_b, metric, api_key } => {
            run_csv_rca(csv_a, csv_b, system_a, system_b, metric, api_key).await
        }
        Commands::Agentic { problem, metadata_dir, api_key } => {
            run_agentic_rca(problem, metadata_dir, api_key).await
        }
        Commands::OneShot { query, metadata_dir, data_dir, api_key, explain } => {
            run_one_shot(query, metadata_dir, data_dir, api_key, explain).await
        }
        Commands::Upload { csv_file, metadata_dir, data_dir } => {
            upload_csv(csv_file, metadata_dir, data_dir).await
        }
    }
}

async fn run_agentic_rca(
    problem: String,
    metadata_dir: PathBuf,
    api_key: Option<String>,
) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!(" AGENTIC RCA REASONING (Cursor-like Stage-wise Planning)");
    println!("{}", "=".repeat(80));
    println!("\n Problem: {}\n", problem);
    
    // Load metadata
    info!("Loading metadata from {:?}", metadata_dir);
    let metadata = Metadata::load(&metadata_dir)
        .map_err(|e| anyhow::anyhow!("Failed to load metadata: {}", e))?;
    
    // Get API key
    let api_key = api_key
        .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        .unwrap_or_else(|| "dummy-api-key".to_string());
    
    // Create LLM client
    let llm = LlmClient::new(
        api_key,
        std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4".to_string()),
        std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
    );
    
    // Create graph
    let graph = Hypergraph::new(metadata.clone());
    
    // Create agentic reasoner (knowledge base optional for now)
    let mut reasoner = AgenticReasoner::new(llm, graph, metadata, None);
    
    // Run agentic reasoning
    println!(" Starting agentic reasoning with stage-wise planning...\n");
    println!("{}", "-".repeat(80));
    let solution = reasoner.reason(&problem).await
        .map_err(|e| anyhow::anyhow!("Agentic reasoning failed: {}", e))?;
    
    // Display results
    println!("\n{}", "=".repeat(80));
    println!(" AGENTIC REASONING COMPLETE");
    println!("{}", "=".repeat(80));
    
    if let Some(ref plan) = solution.plan {
        println!("\n Plan Created:");
        println!("   Goal: {}", plan.goal);
        println!("   Reasoning: {}", plan.reasoning);
        println!("   Steps: {}", plan.steps.len());
        println!("   Confidence: {:.2}%", plan.confidence * 100.0);
        println!("\n   Plan Steps:");
        for (i, step) in plan.steps.iter().enumerate() {
            println!("   {}. {:?}", i + 1, step.action);
            println!("      Reasoning: {}", step.reasoning);
            println!("      Expected: {}", step.expected_outcome);
        }
    }
    
    println!("\n EXPLORATION STEPS EXECUTED: {}", solution.exploration_steps.len());
    println!("{}", "-".repeat(80));
    for (i, step) in solution.exploration_steps.iter().enumerate() {
        println!("\n Step {}: {:?}", i + 1, step.step_type);
        println!("   Action: {}", step.query);
        println!("   Reasoning: {}", step.reasoning);
        match &step.result {
            ExplorationResult::Tables(tables) => {
                println!("   Result: Found {} tables", tables.len());
                for table in tables.iter().take(3) {
                    println!("      - {} (system: {}, grain: {:?})", 
                        table.name, table.system, table.grain);
                }
            }
            ExplorationResult::Columns(cols) => {
                println!("   Result: Found {} columns", cols.len());
                for col in cols.iter().take(3) {
                    println!("      - {}.{} ({})", col.table, col.name, col.data_type);
                }
            }
            ExplorationResult::Path(path) => {
                println!("   Result: Path with {} steps: {:?}", path.len(), path);
            }
            ExplorationResult::Concepts(concepts) => {
                println!("   Result: Found {} concepts", concepts.len());
                for concept in concepts.iter().take(3) {
                    println!("      - {} ({})", concept.name, concept.concept_type);
                }
            }
            ExplorationResult::Rules(rules) => {
                println!("   Result: Found {} rules", rules.len());
                for rule in rules.iter() {
                    println!("      - {}: {} (system: {})", rule.id, rule.metric, rule.system);
                }
            }
            ExplorationResult::Relationships(rels) => {
                println!("   Result: Found {} relationships", rels.len());
                for rel in rels.iter().take(3) {
                    println!("      - {} -> {} ({})", rel.from_table, rel.to_table, rel.relationship_type);
                }
            }
            ExplorationResult::Error(err) => {
                println!("   Result: Error - {}", err);
            }
            ExplorationResult::Success(msg) => {
                println!("   Result: Success - {}", msg);
            }
        }
    }
    
    if let Some(ref answer) = solution.final_answer {
        println!("\n{}", "=".repeat(80));
        println!(" FINAL ANSWER");
        println!("{}", "=".repeat(80));
        println!("\n{}", answer);
    }
    
    println!("\n Overall Confidence: {:.2}%", solution.confidence * 100.0);
    println!("\n{}", "=".repeat(80));
    
    Ok(())
}

async fn run_with_metadata(
    query: String,
    metadata_dir: PathBuf,
    data_dir: PathBuf,
    api_key: Option<String>,
) -> Result<()> {
    // Detect task type from query prefix
    let trimmed_query = query.trim();
    let (task_type, actual_query) = if trimmed_query.starts_with("~DV") {
        ("validation", trimmed_query.trim_start_matches("~DV").trim())
    } else if trimmed_query.starts_with("~RCA") {
        ("rca", trimmed_query.trim_start_matches("~RCA").trim())
    } else {
        // Default to RCA for backward compatibility
        ("rca", trimmed_query)
    };
    
    if actual_query.is_empty() {
        return Err(anyhow::anyhow!("Query cannot be empty after removing prefix"));
    }
    
    info!("Task Type: {}", task_type);
    info!("Query: {}", actual_query);
    
    // Load metadata
    let metadata = Metadata::load(&metadata_dir)?;
    
    // Initialize LLM client
    let api_key = api_key
        .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        .unwrap_or_else(|| "dummy-api-key".to_string());
    let model = std::env::var("OPENAI_MODEL")
        .unwrap_or_else(|_| "gpt-4o-mini".to_string());
    let base_url = std::env::var("OPENAI_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let llm = LlmClient::new(api_key, model, base_url);
    
    match task_type {
        "validation" => {
            info!("Running Data Validation...");
            use spyne_ide::validation::ValidationEngine;
            let engine = ValidationEngine::new(metadata, llm, data_dir);
            let result = engine.run(actual_query).await?;
            
            // Print results
            println!("\n=== Validation Results ===");
            println!("{}", result);
        }
        "rca" | _ => {
            info!("Running RCA...");
            let engine = RcaEngine::new(metadata, llm, data_dir);
            let result = engine.run(actual_query).await?;
            
            // Print results
            println!("\n=== RCA Results ===");
            println!("{}", result);
        }
    }
    
    Ok(())
}

/// Convert string columns containing scientific notation to numeric
fn convert_scientific_notation_columns(df: DataFrame) -> Result<DataFrame> {
    let scientific_regex = Regex::new(r"^-?\d+\.?\d*[Ee][+-]?\d+$").unwrap();
    let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let mut result = df;
    
    // Check each column
    for col_name in &column_names {
        if let Ok(col_data) = result.column(col_name) {
            // If column is string type, check if it contains scientific notation
            if matches!(col_data.dtype(), DataType::String) {
                // Check if any value matches scientific notation pattern
                let has_scientific = if let Ok(str_col) = col_data.str() {
                    (0..str_col.len()).any(|i| {
                        if let Some(val) = str_col.get(i) {
                            scientific_regex.is_match(val)
                        } else {
                            false
                        }
                    })
                } else {
                    false
                };
                
                if has_scientific {
                    // Try to convert to float64, handling scientific notation
                    result = result
                        .lazy()
                        .with_columns([
                            col(col_name)
                                .cast(DataType::Float64)
                                .alias(col_name)
                        ])
                        .collect()?;
                }
            }
        }
    }
    
    Ok(result)
}

async fn run_csv_rca(
    csv_a: PathBuf,
    csv_b: PathBuf,
    system_a: String,
    system_b: String,
    metric: Option<String>,
    api_key: Option<String>,
) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!(" RCA Engine - CSV Mode");
    println!("{}\n", "=".repeat(80));
    
    // Check files exist
    if !csv_a.exists() {
        return Err(anyhow::anyhow!("CSV file A not found: {}", csv_a.display()));
    }
    if !csv_b.exists() {
        return Err(anyhow::anyhow!("CSV file B not found: {}", csv_b.display()));
    }
    
    println!(" Loading CSV files...");
    println!("  System A: {} ({})", system_a, csv_a.display());
    println!("  System B: {} ({})", system_b, csv_b.display());
    
    // Load CSVs with explicit handling for scientific notation
    // Use infer_schema_length to ensure proper type inference including scientific notation
    let df_a = LazyCsvReader::new(&csv_a)
        .with_try_parse_dates(true)
        .with_infer_schema_length(Some(1000)) // Infer schema from more rows to catch scientific notation
        .finish()
        .and_then(|lf| lf.collect())
        .map_err(|e| anyhow::anyhow!("Failed to load CSV A: {}", e))?;
    
    let df_b = LazyCsvReader::new(&csv_b)
        .with_try_parse_dates(true)
        .with_infer_schema_length(Some(1000)) // Infer schema from more rows to catch scientific notation
        .finish()
        .and_then(|lf| lf.collect())
        .map_err(|e| anyhow::anyhow!("Failed to load CSV B: {}", e))?;
    
    // Convert any string columns that contain scientific notation to numeric
    // This handles cases where scientific notation might be read as strings
    // Preserves full precision - no rounding, as these are financial numbers where each digit matters
    let df_a = convert_scientific_notation_columns(df_a)?;
    let df_b = convert_scientific_notation_columns(df_b)?;
    
    // Round all Float64 columns to integers for normalization
    // This ensures values like -3.97E+07 and -3.9695424E7 are compared as integers
    let df_a = spyne_ide::data_utils::round_float64_to_integers(df_a)?;
    let df_b = spyne_ide::data_utils::round_float64_to_integers(df_b)?;
    
    println!("   Loaded {} rows from System A ({} columns)", df_a.height(), df_a.width());
    println!("   Loaded {} rows from System B ({} columns)", df_b.height(), df_b.width());
    
    // Show column names
    println!("\n Columns in System A:");
    for (i, col) in df_a.get_column_names().iter().enumerate() {
        println!("  {}. {}", i + 1, col);
    }
    
    println!("\n Columns in System B:");
    for (i, col) in df_b.get_column_names().iter().enumerate() {
        println!("  {}. {}", i + 1, col);
    }
    
    // Detect grain columns (common columns that look like keys)
    let cols_a: Vec<String> = df_a.get_column_names().iter().map(|s: &&str| s.to_string()).collect();
    let cols_b: Vec<String> = df_b.get_column_names().iter().map(|s: &&str| s.to_string()).collect();
    let common_cols: Vec<String> = cols_a.iter()
        .filter(|c| cols_b.contains(c))
        .cloned()
        .collect();
    
    // Auto-detect grain (columns that look like IDs/keys)
    let potential_grain: Vec<String> = common_cols.iter()
        .filter(|c| {
            let lower = c.to_lowercase();
            lower.contains("id") || lower.contains("key") || lower == "loan_id" || 
            lower == "customer_id" || lower.contains("code") || lower.contains("number")
        })
        .cloned()
        .collect();
    
    // Auto-detect metric (numeric columns that aren't grain)
    let numeric_cols_a: Vec<String> = df_a.get_column_names().iter()
        .filter(|c| {
            if let Ok(col) = df_a.column(*c) {
                matches!(col.dtype(), DataType::Float64 | DataType::Int64 | DataType::UInt64)
            } else {
                false
            }
        })
        .map(|s| s.to_string())
        .collect();
    
    let numeric_cols_b: Vec<String> = df_b.get_column_names().iter()
        .filter(|c| {
            if let Ok(col) = df_b.column(*c) {
                matches!(col.dtype(), DataType::Float64 | DataType::Int64 | DataType::UInt64)
            } else {
                false
            }
        })
        .map(|s| s.to_string())
        .collect();
    
    let potential_metrics: Vec<String> = numeric_cols_a.iter()
        .filter(|c| numeric_cols_b.contains(c) && !potential_grain.contains(c))
        .cloned()
        .collect();
    
    // Interactive prompt for query
    println!("\n{}", "=".repeat(80));
    println!(" Enter your reconciliation query:");
    println!("   Example: 'MSME numbers not matching between {} and {}'", system_a, system_b);
    println!("   Example: 'Compare {} vs {} for total disbursement amount'", system_a, system_b);
    println!("   Example: 'Find differences in loan counts between {} and {}'", system_a, system_b);
    print!("\nQuery: ");
    io::stdout().flush()?;
    
    let mut query = String::new();
    io::stdin().read_line(&mut query)?;
    let query = query.trim();
    
    if query.is_empty() {
        return Err(anyhow::anyhow!("Query cannot be empty"));
    }
    
    // Initialize LLM client for intelligent analysis
    let api_key = api_key
        .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        .unwrap_or_else(|| "dummy-api-key".to_string());
    let model = std::env::var("OPENAI_MODEL")
        .unwrap_or_else(|_| "gpt-4o-mini".to_string());
    let base_url = std::env::var("OPENAI_BASE_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let llm = LlmClient::new(api_key.clone(), model.clone(), base_url.clone());
    
    // Get sample data for LLM context (first few rows)
    let sample_a = if df_a.height() > 0 {
        Some(format!("{:?}", df_a.head(Some(3))))
    } else {
        None
    };
    let sample_b = if df_b.height() > 0 {
        Some(format!("{:?}", df_b.head(Some(3))))
    } else {
        None
    };
    
    println!("\n Analyzing query with LLM...");
    println!("   Query: \"{}\"", query);
    
    // Use LLM to intelligently analyze the query and determine:
    // - Grain column
    // - Metric column (if any)
    // - Aggregation type (count, sum, avg, etc.)
    // - Filters to apply (e.g., msme_flag = yes)
    let analysis = llm.analyze_csv_query(
        query,
        &cols_a,
        &cols_b,
        sample_a.as_deref(),
        sample_b.as_deref(),
    ).await?;
    
    println!("    LLM Analysis:");
    println!("      - Grain column: {}", analysis.grain_column);
    if let Some(ref mc) = analysis.metric_column {
        println!("      - Metric column: {}", mc);
    } else {
        println!("      - Metric: Row count (no specific column)");
    }
    println!("      - Aggregation: {}", analysis.aggregation_type);
    println!("      - Metric name: {}", analysis.metric_name);
    if !analysis.filters.is_empty() {
        println!("      - Filters:");
        for filter in &analysis.filters {
            println!("        * {} {} {:?}", filter.column, filter.operator, filter.value);
        }
    }
    
    // Apply filters to dataframes if specified
    let mut df_a_filtered = df_a.clone();
    let mut df_b_filtered = df_b.clone();
    
    for filter in &analysis.filters {
        println!("\n    Applying filter: {} {} {:?}", filter.column, filter.operator, filter.value);
        
        // Check if column exists
        if !df_a_filtered.get_column_names().contains(&filter.column.as_str()) {
            println!("      ️  Warning: Column '{}' not found in System A, skipping filter", filter.column);
            continue;
        }
        if !df_b_filtered.get_column_names().contains(&filter.column.as_str()) {
            println!("      ️  Warning: Column '{}' not found in System B, skipping filter", filter.column);
            continue;
        }
        
        // Apply filter based on operator
        match filter.operator.as_str() {
            "=" => {
                let filter_value = match &filter.value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => filter.value.to_string(),
                };
                
                df_a_filtered = df_a_filtered
                    .lazy()
                    .filter(col(&filter.column).eq(lit(filter_value.clone())))
                    .collect()?;
                df_b_filtered = df_b_filtered
                    .lazy()
                    .filter(col(&filter.column).eq(lit(filter_value)))
                    .collect()?;
            }
            "!=" => {
                let filter_value = match &filter.value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => filter.value.to_string(),
                };
                
                df_a_filtered = df_a_filtered
                    .lazy()
                    .filter(col(&filter.column).neq(lit(filter_value.clone())))
                    .collect()?;
                df_b_filtered = df_b_filtered
                    .lazy()
                    .filter(col(&filter.column).neq(lit(filter_value)))
                    .collect()?;
            }
            "contains" => {
                let filter_value = filter.value.as_str().unwrap_or("");
                // For contains, we'll use a simple equality check for now
                // More complex pattern matching can be added later
                println!("      ️  Note: 'contains' operator simplified to equality check");
                df_a_filtered = df_a_filtered
                    .lazy()
                    .filter(col(&filter.column).eq(lit(filter_value)))
                    .collect()?;
                df_b_filtered = df_b_filtered
                    .lazy()
                    .filter(col(&filter.column).eq(lit(filter_value)))
                    .collect()?;
            }
            _ => {
                println!("      ️  Warning: Operator '{}' not yet supported, skipping filter", filter.operator);
            }
        }
        
        println!("       Applied filter: {} rows remaining in A, {} rows in B", 
            df_a_filtered.height(), df_b_filtered.height());
    }
    
    // Create temporary metadata and data structure
    let temp_dir = std::env::temp_dir().join(format!("rca_csv_{}", uuid::Uuid::new_v4()));
    let metadata_dir = temp_dir.join("metadata");
    let data_dir = temp_dir.join("data");
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir.join(&system_a))?;
    fs::create_dir_all(&data_dir.join(&system_b))?;
    
    // Save filtered CSVs as parquet
    let parquet_a = data_dir.join(&system_a).join("data.parquet");
    let parquet_b = data_dir.join(&system_b).join("data.parquet");
    
    let mut file_a = fs::File::create(&parquet_a)?;
    ParquetWriter::new(&mut file_a).finish(&mut df_a_filtered.clone())?;
    
    let mut file_b = fs::File::create(&parquet_b)?;
    ParquetWriter::new(&mut file_b).finish(&mut df_b_filtered.clone())?;
    
    println!("\n Creating metadata...");
    
    // Use LLM-determined grain and metric
    let grain = analysis.grain_column;
    let metric_col = analysis.metric_column.clone().unwrap_or_else(|| {
        // If no metric column, we'll use count aggregation
        "count".to_string()
    });
    
    println!("   Grain column: {}", grain);
    println!("   Metric: {} ({})", analysis.metric_name, analysis.aggregation_type);
    
    // Create metadata with aggregation type
    create_csv_metadata_with_agg(
        &metadata_dir, 
        &system_a, 
        &system_b, 
        &grain, 
        &metric_col,
        &analysis.aggregation_type,
        &analysis.metric_name,
    )?;
    
    // Load metadata
    let metadata = Metadata::load(&metadata_dir)?;
    
    // Run RCA (LLM client already initialized above)
    println!("\n Running RCA...\n");
    let engine = RcaEngine::new(metadata, llm, data_dir.clone());
    let result = engine.run(query).await?;
    
    // Print results
    println!("\n{}", "=".repeat(80));
    println!(" RCA Results");
    println!("{}\n", "=".repeat(80));
    println!("{}", result);
    
    // Cleanup
    println!("\n Cleaning up temporary files...");
    fs::remove_dir_all(&temp_dir)?;
    println!("   Done");
    
    Ok(())
}

fn create_csv_metadata_with_agg(
    metadata_dir: &PathBuf,
    system_a: &str,
    system_b: &str,
    grain: &str,
    metric_col: &str,
    agg_type: &str,
    metric_name: &str,
) -> Result<()> {
    use serde_json::json;
    
    // Normalize metric_name to create a valid metric_id (lowercase, replace spaces with underscores)
    let metric_id = metric_name
        .to_lowercase()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>();
    
    // Build formula based on aggregation type
    let formula = match agg_type.to_lowercase().as_str() {
        "count" => {
            if metric_col == "count" {
                "COUNT(*)".to_string()
            } else {
                format!("COUNT({})", metric_col)
            }
        }
        "sum" => format!("SUM({})", metric_col),
        "avg" | "average" => format!("AVG({})", metric_col),
        "max" => format!("MAX({})", metric_col),
        "min" => format!("MIN({})", metric_col),
        _ => format!("SUM({})", metric_col), // Default to sum
    };
    
    // Create entities.json
    let entities = json!([
        {
            "id": "entity",
            "name": "Entity",
            "description": "Generic entity from CSV",
            "grain": [grain],
            "attributes": []
        }
    ]);
    fs::write(metadata_dir.join("entities.json"), serde_json::to_string_pretty(&entities)?)?;
    
    // Create tables.json
    let tables = json!([
        {
            "name": format!("{}_data", system_a),
            "system": system_a,
            "entity": "entity",
            "primary_key": [grain],
            "time_column": "",
            "path": format!("{}/data.parquet", system_a),
            "columns": null
        },
        {
            "name": format!("{}_data", system_b),
            "system": system_b,
            "entity": "entity",
            "primary_key": [grain],
            "time_column": "",
            "path": format!("{}/data.parquet", system_b),
            "columns": null
        }
    ]);
    fs::write(metadata_dir.join("tables.json"), serde_json::to_string_pretty(&tables)?)?;
    
    // Create rules.json with proper aggregation
    let rules = json!([
        {
            "id": format!("{}_metric", system_a),
            "system": system_a,
            "metric": metric_id.clone(),
            "target_entity": "entity",
            "target_grain": [grain],
            "computation": {
                "description": format!("{} from {} CSV", metric_name, system_a),
                "source_entities": ["entity"],
                "attributes_needed": {
                    "entity": [grain, metric_col]
                },
                "formula": formula.clone(),
                "aggregation_grain": [grain]
            }
        },
        {
            "id": format!("{}_metric", system_b),
            "system": system_b,
            "metric": metric_id.clone(),
            "target_entity": "entity",
            "target_grain": [grain],
            "computation": {
                "description": format!("{} from {} CSV", metric_name, system_b),
                "source_entities": ["entity"],
                "attributes_needed": {
                    "entity": [grain, metric_col]
                },
                "formula": formula,
                "aggregation_grain": [grain]
            }
        }
    ]);
    fs::write(metadata_dir.join("rules.json"), serde_json::to_string_pretty(&rules)?)?;
    
    // Create metrics.json
    let metrics = json!([
        {
            "id": metric_id.clone(),
            "name": metric_name,
            "description": format!("{} calculated using {}", metric_name, agg_type),
            "grain": [grain],
            "precision": 2,
            "null_policy": "zero",
            "unit": "",
            "versions": []
        }
    ]);
    fs::write(metadata_dir.join("metrics.json"), serde_json::to_string_pretty(&metrics)?)?;
    
    // Create business_labels.json
    let business_labels = json!({
        "systems": [
            {
                "system_id": system_a,
                "label": system_a,
                "aliases": []
            },
            {
                "system_id": system_b,
                "label": system_b,
                "aliases": []
            }
        ],
        "metrics": [
            {
                "metric_id": metric_id.clone(),
                "label": metric_name,
                "aliases": [metric_name, metric_col]
            }
        ],
        "reconciliation_types": []
    });
    fs::write(metadata_dir.join("business_labels.json"), serde_json::to_string_pretty(&business_labels)?)?;
    
    // Create lineage.json
    use std::collections::HashMap;
    let mut join_keys_map = HashMap::new();
    join_keys_map.insert(grain.to_string(), grain.to_string());
    let lineage = json!({
        "edges": [
            {
                "from": format!("{}_data", system_a),
                "to": format!("{}_data", system_a),
                "keys": join_keys_map.clone(),
                "relationship": "one_to_one"
            },
            {
                "from": format!("{}_data", system_b),
                "to": format!("{}_data", system_b),
                "keys": join_keys_map,
                "relationship": "one_to_one"
            }
        ],
        "possible_joins": []
    });
    fs::write(metadata_dir.join("lineage.json"), serde_json::to_string_pretty(&lineage)?)?;
    
    // Create empty files for other required metadata
    let identity = json!({
        "canonical_keys": [
            {
                "entity": "entity",
                "canonical": grain,
                "alternates": []
            }
        ],
        "key_mappings": []
    });
    fs::write(metadata_dir.join("identity.json"), serde_json::to_string_pretty(&identity)?)?;
    
    let time_rules = json!({
        "as_of_rules": [],
        "lateness_rules": []
    });
    fs::write(metadata_dir.join("time.json"), serde_json::to_string_pretty(&time_rules)?)?;
    
    let exceptions = json!({
        "exceptions": []
    });
    fs::write(metadata_dir.join("exceptions.json"), serde_json::to_string_pretty(&exceptions)?)?;
    
    Ok(())
}

fn create_csv_metadata(
    metadata_dir: &PathBuf,
    system_a: &str,
    system_b: &str,
    grain: &str,
    metric: &str,
) -> Result<()> {
    use serde_json::json;
    
    // Create entities.json
    let entities = json!([
        {
            "id": "entity",
            "name": "Entity",
            "description": "Generic entity from CSV",
            "grain": [grain],
            "attributes": []
        }
    ]);
    fs::write(metadata_dir.join("entities.json"), serde_json::to_string_pretty(&entities)?)?;
    
    // Create tables.json
    let tables = json!([
        {
            "name": format!("{}_data", system_a),
            "system": system_a,
            "entity": "entity",
            "primary_key": [grain],
            "time_column": "",
            "path": format!("{}/data.parquet", system_a),
            "columns": null
        },
        {
            "name": format!("{}_data", system_b),
            "system": system_b,
            "entity": "entity",
            "primary_key": [grain],
            "time_column": "",
            "path": format!("{}/data.parquet", system_b),
            "columns": null
        }
    ]);
    fs::write(metadata_dir.join("tables.json"), serde_json::to_string_pretty(&tables)?)?;
    
    // Create rules.json
    let rules = json!([
        {
            "id": format!("{}_metric", system_a),
            "system": system_a,
            "metric": "metric",
            "target_entity": "entity",
            "target_grain": [grain],
            "computation": {
                "description": format!("Metric from {} CSV", system_a),
                "source_entities": ["entity"],
                "attributes_needed": {
                    "entity": [grain, metric]
                },
                "formula": metric,
                "aggregation_grain": [grain]
            }
        },
        {
            "id": format!("{}_metric", system_b),
            "system": system_b,
            "metric": "metric",
            "target_entity": "entity",
            "target_grain": [grain],
            "computation": {
                "description": format!("Metric from {} CSV", system_b),
                "source_entities": ["entity"],
                "attributes_needed": {
                    "entity": [grain, metric]
                },
                "formula": metric,
                "aggregation_grain": [grain]
            }
        }
    ]);
    fs::write(metadata_dir.join("rules.json"), serde_json::to_string_pretty(&rules)?)?;
    
    // Create metrics.json
    let metrics = json!([
        {
            "id": "metric",
            "name": "Metric",
            "description": format!("Metric column: {}", metric),
            "grain": [grain],
            "precision": 2,
            "null_policy": "zero",
            "unit": "",
            "versions": []
        }
    ]);
    fs::write(metadata_dir.join("metrics.json"), serde_json::to_string_pretty(&metrics)?)?;
    
    // Create business_labels.json
    let business_labels = json!({
        "systems": [
            {
                "system_id": system_a,
                "label": system_a,
                "aliases": []
            },
            {
                "system_id": system_b,
                "label": system_b,
                "aliases": []
            }
        ],
        "metrics": [
            {
                "metric_id": "metric",
                "label": "Metric",
                "aliases": [metric]
            }
        ],
        "reconciliation_types": []
    });
    fs::write(metadata_dir.join("business_labels.json"), serde_json::to_string_pretty(&business_labels)?)?;
    
    // Create lineage.json
    use std::collections::HashMap;
    let mut join_keys_map = HashMap::new();
    join_keys_map.insert(grain.to_string(), grain.to_string());
    let lineage = json!({
        "edges": [
            {
                "from": format!("{}_data", system_a),
                "to": format!("{}_data", system_a),
                "keys": join_keys_map.clone(),
                "relationship": "one_to_one"
            },
            {
                "from": format!("{}_data", system_b),
                "to": format!("{}_data", system_b),
                "keys": join_keys_map,
                "relationship": "one_to_one"
            }
        ],
        "possible_joins": []
    });
    fs::write(metadata_dir.join("lineage.json"), serde_json::to_string_pretty(&lineage)?)?;
    
    // Create empty files for other required metadata
    let identity = json!({
        "canonical_keys": [
            {
                "entity": "entity",
                "canonical": grain,
                "alternates": []
            }
        ],
        "key_mappings": []
    });
    fs::write(metadata_dir.join("identity.json"), serde_json::to_string_pretty(&identity)?)?;
    
    let time_rules = json!({
        "as_of_rules": [],
        "lateness_rules": []
    });
    fs::write(metadata_dir.join("time.json"), serde_json::to_string_pretty(&time_rules)?)?;
    
    let exceptions = json!({
        "exceptions": []
    });
    fs::write(metadata_dir.join("exceptions.json"), serde_json::to_string_pretty(&exceptions)?)?;
    
    Ok(())
}

async fn run_one_shot(
    query: String,
    metadata_dir: PathBuf,
    data_dir: PathBuf,
    api_key: Option<String>,
    explain: bool,
) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!(" ONE-SHOT AGENTIC RCA + DV ENGINE");
    println!("{}", "=".repeat(80));
    println!("\n Query: {}\n", query);
    
    // Load metadata
    info!("Loading metadata from {:?}", metadata_dir);
    let metadata = Metadata::load(&metadata_dir)
        .map_err(|e| anyhow::anyhow!("Failed to load metadata: {}", e))?;
    
    // Get API key
    let api_key = api_key
        .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        .unwrap_or_else(|| "dummy-api-key".to_string());
    
    // Create LLM client
    let llm = LlmClient::new(
        api_key,
        std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".to_string()),
        std::env::var("OPENAI_BASE_URL").unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
    );
    
    // Create one-shot runner
    let runner = OneShotRunner::new(metadata, llm, data_dir);
    
    // Run query
    println!(" Processing query...\n");
    let result = runner.run(&query).await
        .map_err(|e| anyhow::anyhow!("One-shot execution failed: {}", e))?;
    
    // Display results
    println!("{}", "=".repeat(80));
    if result.success {
        println!(" SUCCESS");
    } else {
        println!(" FAILED");
    }
    println!("{}", "=".repeat(80));
    
    println!("\n Task Type: {:?}", result.intent.task_type);
    println!(" Systems: {:?}", result.intent.systems);
    println!(" Metrics: {:?}", result.intent.target_metrics);
    println!(" Grain: {:?}", result.intent.grain);
    
    if let Some(ref task) = result.grounded_task {
        println!("\n Grounded Task:");
        println!("   Candidate Tables: {}", task.candidate_tables.len());
        for table in &task.candidate_tables {
            println!("      - {} (system: {}, confidence: {:.2})", 
                table.table_name, table.system, table.confidence);
        }
        if !task.unresolved_fields.is_empty() {
            println!("   ️  Unresolved: {:?}", task.unresolved_fields);
        }
    }
    
    if let Some(ref plan) = result.execution_plan {
        println!("\n Execution Plan:");
        println!("   Nodes: {}", plan.nodes.len());
        println!("   Edges: {}", plan.edges.len());
        println!("   Root Nodes: {:?}", plan.root_nodes);
    }
    
    if explain {
        println!("\n Explanation:");
        println!("   Summary: {}", result.explanation.summary);
        if !result.explanation.why_tables.is_empty() {
            println!("\n   Why These Tables:");
            for table_expl in &result.explanation.why_tables {
                println!("      - {} (system: {}, confidence: {:.2})", 
                    table_expl.table_name, table_expl.system, table_expl.confidence);
                for reason in &table_expl.reasons {
                    println!("        • {}", reason);
                }
            }
        }
        if !result.explanation.why_joins.is_empty() {
            println!("\n   Why These Joins:");
            for join_expl in &result.explanation.why_joins {
                println!("      - {} → {} (keys: {:?})", 
                    join_expl.from_table, join_expl.to_table, join_expl.keys_used);
                for reason in &join_expl.reasons {
                    println!("        • {}", reason);
                }
            }
        }
        if !result.explanation.why_grain.is_empty() {
            println!("\n   Why This Grain:");
            for grain_expl in &result.explanation.why_grain {
                println!("      - {:?} (source: {:?})", grain_expl.grain, grain_expl.source);
                for reason in &grain_expl.reasons {
                    println!("        • {}", reason);
                }
            }
        }
        if !result.explanation.why_rules.is_empty() {
            println!("\n   Why These Rules:");
            for rule_expl in &result.explanation.why_rules {
                println!("      - {}: {} (system: {})", 
                    rule_expl.rule_id, rule_expl.metric, rule_expl.system);
                println!("        Formula: {}", rule_expl.formula);
                println!("        Reasoning: {}", rule_expl.reasoning);
            }
        }
        if !result.explanation.why_constraints.is_empty() {
            println!("\n   Why These Constraints:");
            for constraint_expl in &result.explanation.why_constraints {
                println!("      - {} on {} ({})", 
                    constraint_expl.constraint_type, constraint_expl.column, constraint_expl.interpretation);
                println!("        Reasoning: {}", constraint_expl.reasoning);
            }
        }
        if !result.explanation.decision_tree.is_empty() {
            println!("\n   Decision Tree:");
            for (idx, decision) in result.explanation.decision_tree.iter().enumerate() {
                println!("      {}. {}: {}", idx + 1, decision.decision, decision.chosen);
                println!("         Reasoning: {}", decision.reasoning);
                if !decision.alternatives.is_empty() {
                    println!("         Alternatives considered: {:?}", decision.alternatives);
                }
            }
        }
    }
    
    if !result.failures.is_empty() {
        println!("\n️  Failures:");
        for failure in &result.failures {
            println!("   - [{}] {}", failure.failure_type, failure.message);
        }
    }
    
    if let Some(ref data) = result.result_data {
        println!("\n Result Data:");
        println!("{}", serde_json::to_string_pretty(data)?);
    }
    
    println!("\n{}", "=".repeat(80));
    
    Ok(())
}

async fn upload_csv(
    csv_file: PathBuf,
    metadata_dir: PathBuf,
    data_dir: PathBuf,
) -> Result<()> {
    use spyne_ide::ingestion::{CsvConnector, IngestionOrchestrator};
    use spyne_ide::world_state::WorldState;
    use std::io::{self, Write};
    
    println!("\n{}", "=".repeat(80));
    println!(" CSV Upload - Simple Interface");
    println!("{}", "=".repeat(80));
    
    // Check file exists
    if !csv_file.exists() {
        return Err(anyhow::anyhow!("CSV file not found: {}", csv_file.display()));
    }
    
    println!("\n Reading CSV file: {}", csv_file.display());
    
    // Read CSV file
    let csv_content = fs::read_to_string(&csv_file)
        .map_err(|e| anyhow::anyhow!("Failed to read CSV file: {}", e))?;
    
    // Parse CSV to get column names
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_content.as_bytes());
    
    let headers = rdr.headers()
        .map_err(|e| anyhow::anyhow!("Failed to read CSV headers: {}", e))?
        .iter()
        .map(|h| h.trim().to_string())
        .collect::<Vec<String>>();
    
    println!("\n Found {} columns:", headers.len());
    for (i, col) in headers.iter().enumerate() {
        println!("  {}. {}", i + 1, col);
    }
    
    // Prompt for column descriptions
    println!("\n{}", "-".repeat(80));
    println!(" Please provide a brief description for each column:");
    println!("{}", "-".repeat(80));
    
    let mut column_descriptions: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    
    for col in &headers {
        print!("\n  Column '{}': ", col);
        io::stdout().flush()?;
        let mut desc = String::new();
        io::stdin().read_line(&mut desc)?;
        let desc = desc.trim();
        if !desc.is_empty() {
            column_descriptions.insert(col.clone(), desc.to_string());
        }
    }
    
    // Prompt for table information
    println!("\n{}", "-".repeat(80));
    println!(" Table Information:");
    println!("{}", "-".repeat(80));
    
    print!("\n  What is this table about? (brief description): ");
    io::stdout().flush()?;
    let mut table_description = String::new();
    io::stdin().read_line(&mut table_description)?;
    let table_description = table_description.trim();
    
    print!("\n  System name (e.g., 'system_a', 'core_banking'): ");
    io::stdout().flush()?;
    let mut system_name = String::new();
    io::stdin().read_line(&mut system_name)?;
    let system_name = system_name.trim();
    if system_name.is_empty() {
        return Err(anyhow::anyhow!("System name cannot be empty"));
    }
    
    print!("\n  Entity name (e.g., 'loan', 'customer', 'transaction'): ");
    io::stdout().flush()?;
    let mut entity_name = String::new();
    io::stdin().read_line(&mut entity_name)?;
    let entity_name = entity_name.trim();
    if entity_name.is_empty() {
        return Err(anyhow::anyhow!("Entity name cannot be empty"));
    }
    
    println!("\n  Select primary key columns (comma-separated, e.g., '1,2' or column names): ");
    println!("     Available columns:");
    for (i, col) in headers.iter().enumerate() {
        println!("       {}. {}", i + 1, col);
    }
    print!("  Primary keys: ");
    io::stdout().flush()?;
    let mut primary_keys_input = String::new();
    io::stdin().read_line(&mut primary_keys_input)?;
    let primary_keys_input = primary_keys_input.trim();
    
    // Parse primary keys (support both indices and names)
    let primary_keys: Vec<String> = if primary_keys_input.contains(',') {
        primary_keys_input.split(',')
            .map(|s| s.trim())
            .filter_map(|s| {
                if let Ok(idx) = s.parse::<usize>() {
                    headers.get(idx - 1).cloned()
                } else {
                    Some(s.to_string())
                }
            })
            .collect()
    } else {
        if let Ok(idx) = primary_keys_input.parse::<usize>() {
            headers.get(idx - 1).map(|s| vec![s.clone()]).unwrap_or_default()
        } else {
            vec![primary_keys_input.to_string()]
        }
    };
    
    if primary_keys.is_empty() {
        return Err(anyhow::anyhow!("At least one primary key column is required"));
    }
    
    print!("\n  Grain columns (comma-separated, or press Enter to use primary keys): ");
    io::stdout().flush()?;
    let mut grain_input = String::new();
    io::stdin().read_line(&mut grain_input)?;
    let grain_input = grain_input.trim();
    
    let grain: Vec<String> = if grain_input.is_empty() {
        primary_keys.clone()
    } else {
        grain_input.split(',')
            .map(|s| s.trim())
            .filter_map(|s| {
                if let Ok(idx) = s.parse::<usize>() {
                    headers.get(idx - 1).cloned()
                } else {
                    Some(s.to_string())
                }
            })
            .collect()
    };
    
    // Generate table name from file name
    let table_name = csv_file.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("uploaded_table")
        .to_string();
    
    println!("\n{}", "=".repeat(80));
    println!(" Creating metadata and ingesting data...");
    println!("{}", "=".repeat(80));
    
    // Ensure directories exist
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;
    
    // Create minimal metadata structure if needed
    use serde_json::json;
    
    // Create empty rules.json if it doesn't exist
    if !metadata_dir.join("rules.json").exists() {
        fs::write(metadata_dir.join("rules.json"), "[]")?;
    }
    
    // Create empty exceptions.json if it doesn't exist
    if !metadata_dir.join("exceptions.json").exists() {
        let exceptions = json!({"exceptions": []});
        fs::write(metadata_dir.join("exceptions.json"), serde_json::to_string_pretty(&exceptions)?)?;
    }
    
    // Create tables.json if it doesn't exist
    if !metadata_dir.join("tables.json").exists() {
        fs::write(metadata_dir.join("tables.json"), "[]")?;
    }
    
    // Load metadata
    let mut metadata = Metadata::load(&metadata_dir)?;
    
    // Create entity if it doesn't exist
    let entity_exists = metadata.entities.iter().any(|e| e.id == entity_name);
    if !entity_exists {
        let entity = spyne_ide::metadata::Entity {
            id: entity_name.to_string(),
            name: entity_name.to_string(),
            description: table_description.to_string(),
            grain: grain.clone(),
            attributes: headers.clone(),
        };
        metadata.entities.push(entity);
        
        // Save entities.json
        let entities_json = json!(metadata.entities);
        fs::write(metadata_dir.join("entities.json"), serde_json::to_string_pretty(&entities_json)?)?;
    }
    
    // Create table metadata
    let table_metadata = spyne_ide::metadata::Table {
        name: table_name.clone(),
        entity: entity_name.to_string(),
        primary_key: primary_keys.clone(),
        time_column: None, // Can be added later if needed
        system: system_name.to_string(),
        path: format!("{}.parquet", table_name),
        columns: Some(headers.iter().map(|col| {
            spyne_ide::metadata::ColumnMetadata {
                name: col.clone(),
                description: column_descriptions.get(col).cloned(),
                data_type: None,
                distinct_values: None,
            }
        }).collect()),
        labels: None,
    };
    
    // Check if table already exists, if so update it
    if let Some(existing_idx) = metadata.tables.iter().position(|t| t.name == table_name) {
        metadata.tables[existing_idx] = table_metadata.clone();
    } else {
        metadata.tables.push(table_metadata.clone());
    }
    
    // Save tables.json
    let tables_json = json!(metadata.tables);
    fs::write(metadata_dir.join("tables.json"), serde_json::to_string_pretty(&tables_json)?)?;
    
    println!("\n Metadata created successfully!");
    println!("   - Table: {}", table_name);
    println!("   - System: {}", system_name);
    println!("   - Entity: {}", entity_name);
    println!("   - Primary keys: {:?}", primary_keys);
    println!("   - Grain: {:?}", grain);
    
    // Create WorldState
    let mut world_state = WorldState::new();
    
    // Create CSV connector
    let source_id = format!("csv_{}", table_name);
    let connector = Box::new(CsvConnector::new(source_id.clone(), csv_content));
    
    // Create orchestrator
    let orchestrator = IngestionOrchestrator::new();
    
    // Ingest data
    println!("\n Ingesting data...");
    let result = orchestrator.ingest(
        &mut world_state,
        &data_dir,
        connector,
        Some(table_name.clone()),
    )?;
    
    println!("\n Ingestion complete!");
    println!("   - Records ingested: {}", result.records_ingested);
    println!("   - Tables affected: {:?}", result.tables_affected);
    println!("   - Schema versions: {:?}", result.schema_versions);
    
    // Create hypergraph to automatically create nodes and edges
    println!("\n Creating nodes and edges in hypergraph...");
    let graph = Hypergraph::new(metadata.clone());
    
    println!("\n Nodes and edges created successfully!");
    println!("   - Graph initialized with {} tables", metadata.tables.len());
    println!("   - Entities: {}", metadata.entities.len());
    
    println!("\n{}", "=".repeat(80));
    println!(" Upload complete! Your CSV has been processed and added to the system.");
    println!("{}", "=".repeat(80));
    
    Ok(())
}
