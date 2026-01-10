mod ambiguity;
mod diff;
mod drilldown;
mod error;
mod explain;
mod graph;
mod identity;
mod llm;
mod metadata;
mod operators;
mod rca;
mod rule_compiler;
mod time;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::{info, error};

#[derive(Parser)]
#[command(name = "rca-engine")]
#[command(about = "Root Cause Analysis Engine for Data Reconciliation")]
struct Args {
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    info!("RCA Engine starting...");
    info!("Query: {}", args.query);
    
    // Load metadata
    let metadata = metadata::Metadata::load(&args.metadata_dir)?;
    
    // Initialize LLM client
    let api_key = args.api_key
        .or_else(|| std::env::var("OPENAI_API_KEY").ok())
        .unwrap_or_else(|| "dummy-api-key".to_string());
    let llm = llm::LlmClient::new(api_key);
    
    // Run RCA
    let engine = rca::RcaEngine::new(metadata, llm, args.data_dir);
    let result = engine.run(&args.query).await?;
    
    // Print results
    println!("\n=== RCA Results ===");
    println!("{}", result);
    
    Ok(())
}

