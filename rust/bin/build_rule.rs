use anyhow::Result;
use clap::Parser;
use spyne_ide::intelligent_rule_builder::IntelligentRuleBuilder;
use spyne_ide::metadata::Metadata;
use std::path::PathBuf;

/// CLI tool to build rules from natural language business rules
#[derive(Parser)]
#[command(name = "build-rule")]
#[command(about = "Build RCA rules from natural language business rules")]
struct Args {
    /// Natural language business rule (e.g., "TOS = A+B-C for all loans")
    business_rule: String,
    
    /// System name (e.g., "khatabook")
    #[arg(short, long)]
    system: String,
    
    /// Metric name (e.g., "tos")
    #[arg(short, long)]
    metric: String,
    
    /// Target entity (e.g., "loan")
    #[arg(short, long, default_value = "loan")]
    entity: String,
    
    /// Path to metadata directory (default: ./metadata)
    #[arg(long, default_value = "metadata")]
    metadata_dir: PathBuf,
    
    /// Path to data directory (default: ./data)
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
    
    /// Output file for the generated rule (default: stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    println!("🔍 Intelligent Rule Builder");
    println!("==========================\n");
    println!("Business Rule: \"{}\"", args.business_rule);
    println!("System: {}", args.system);
    println!("Metric: {}", args.metric);
    println!("Target Entity: {}\n", args.entity);
    
    // Load metadata
    let metadata = Metadata::load(&args.metadata_dir)?;
    
    // Create rule builder
    let mut builder = IntelligentRuleBuilder::new(metadata, args.data_dir);
    
    // Build rule from natural language
    let rule = builder.build_rule_from_natural_language(
        &args.business_rule,
        &args.system,
        &args.metric,
        &args.entity,
    ).await?;
    
    // Output the rule
    let rule_json = serde_json::to_string_pretty(&rule)?;
    
    if let Some(output_path) = args.output {
        std::fs::write(&output_path, rule_json)?;
        println!("\n✅ Rule saved to: {}", output_path.display());
    } else {
        println!("\n📋 Generated Rule:");
        println!("{}", rule_json);
    }
    
    Ok(())
}


