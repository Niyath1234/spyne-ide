use polars::prelude::*;
use rca_engine::metadata::Metadata;
use rca_engine::rca::RcaEngine;
use rca_engine::llm::LlmClient;
use std::path::PathBuf;
use std::fs;

/// Create test data files in Parquet format
fn create_test_data_files(data_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Create directories
    fs::create_dir_all(data_dir.join("khatabook"))?;
    fs::create_dir_all(data_dir.join("tb"))?;

    // 1. Create khatabook_loans.parquet
    let loans_df = df! [
        "loan_id" => ["1001", "1002"],
        "customer_id" => ["C001", "C002"],
        "disbursement_date" => ["2025-01-15", "2025-02-20"],
        "principal_amount" => [100000.0, 50000.0]
    ]?;
    
    let loans_path = data_dir.join("khatabook/loans.parquet");
    let mut file = std::fs::File::create(&loans_path)?;
    ParquetWriter::new(&mut file).finish(&mut loans_df.clone())?;

    // 2. Create khatabook_emis.parquet
    // Note: Formula uses "emi_amount" so column must be named exactly that
    let emis_df = df! [
        "loan_id" => ["1001", "1001", "1002"],
        "emi_number" => [1, 2, 1],
        "due_date" => ["2025-02-15", "2025-03-15", "2025-03-20"],
        "emi_amount" => [5000.0, 5000.0, 3000.0]
    ]?;
    
    let emis_path = data_dir.join("khatabook/emis.parquet");
    let mut file = std::fs::File::create(&emis_path)?;
    ParquetWriter::new(&mut file).finish(&mut emis_df.clone())?;

    // 3. Create khatabook_transactions.parquet
    // Note: Formula uses "transaction_amount" so column must be named exactly that
    // For loan 1001, emi 1: transaction_amount = 4500
    // For loan 1001, emi 2: no transaction (will be NULL/COALESCE to 0)
    // For loan 1002, emi 1: transaction_amount = 3000
    let transactions_df = df! [
        "transaction_id" => ["T001", "T003"],
        "loan_id" => ["1001", "1002"],
        "emi_number" => [1, 1],
        "transaction_date" => ["2025-02-10", "2025-03-18"],
        "transaction_amount" => [4500.0, 3000.0]
    ]?;
    
    let transactions_path = data_dir.join("khatabook/transactions.parquet");
    let mut file = std::fs::File::create(&transactions_path)?;
    ParquetWriter::new(&mut file).finish(&mut transactions_df.clone())?;

    // 4. Create tb_loans.parquet (first table for loan entity)
    // Note: This table needs total_outstanding column for the tb_tos rule
    let tb_loans_df = df! [
        "loan_id" => ["1001", "1002"],
        "customer_id" => ["C001", "C002"],
        "disbursement_date" => ["2025-01-15", "2025-02-20"],
        "principal_amount" => [100000.0, 50000.0],
        "total_outstanding" => [6000.0, 0.0]  // Required by tb_tos rule
    ]?;
    
    let tb_loans_path = data_dir.join("tb/loans.parquet");
    let mut file = std::fs::File::create(&tb_loans_path)?;
    ParquetWriter::new(&mut file).finish(&mut tb_loans_df.clone())?;

    // 5. Create tb_loan_summary.parquet  
    // Note: TB has loan 1001 with TOS=6000 (different from Khatabook's calculation)
    let tb_summary_df = df! [
        "loan_id" => ["1001", "1002"],
        "as_of_date" => ["2025-12-31", "2025-12-31"],
        "total_outstanding" => [6000.0, 0.0]
    ]?;
    
    let tb_summary_path = data_dir.join("tb/loan_summary.parquet");
    let mut file = std::fs::File::create(&tb_summary_path)?;
    ParquetWriter::new(&mut file).finish(&mut tb_summary_df.clone())?;

    println!("✅ Created test data files:");
    println!("  - {}", loans_path.display());
    println!("  - {}", emis_path.display());
    println!("  - {}", transactions_path.display());
    println!("  - {}", tb_loans_path.display());
    println!("  - {}", tb_summary_path.display());

    Ok(())
}

#[tokio::test]
async fn test_end_to_end_reconciliation() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Starting End-to-End Reconciliation Test\n");

    // Setup: Create temporary directories
    let test_dir = std::env::temp_dir().join("rca_engine_test");
    let metadata_dir = test_dir.join("metadata");
    let data_dir = test_dir.join("data");
    
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;

    // Copy metadata files to test directory
    println!("📋 Copying metadata files...");
    let source_metadata = PathBuf::from("metadata");
    for entry in fs::read_dir(&source_metadata)? {
        let entry = entry?;
        let file_name = entry.file_name();
        fs::copy(entry.path(), metadata_dir.join(&file_name))?;
        println!("  ✓ Copied {}", file_name.to_string_lossy());
    }

    // Create test data files
    println!("\n📊 Creating test data files...");
    create_test_data_files(&data_dir)?;

    // Load metadata
    println!("\n🔍 Loading metadata...");
    let metadata = Metadata::load(&metadata_dir)?;
    println!("  ✓ Loaded {} entities", metadata.entities.len());
    println!("  ✓ Loaded {} tables", metadata.tables.len());
    println!("  ✓ Loaded {} rules", metadata.rules.len());
    println!("  ✓ Loaded {} metrics", metadata.metrics.len());

    // Initialize LLM client (with dummy API key for testing)
    println!("\n🤖 Initializing LLM client (dummy mode)...");
    let llm = LlmClient::new(
        "dummy-api-key".to_string(),
        "gpt-4o-mini".to_string(),
        "https://api.openai.com/v1".to_string(),
    );
    println!("  ✓ LLM client initialized (dummy mode)");

    // Create RCA engine
    println!("\n🚀 Creating RCA Engine...");
    let engine = RcaEngine::new(metadata, llm, data_dir.clone());
    println!("  ✓ RCA Engine created");

    // Run reconciliation query
    println!("\n⚙️  Running reconciliation query...");
    let query = "Khatabook vs TB TOS recon as of 2025-12-31";
    println!("  Query: {}", query);
    
    let result = engine.run(query).await?;

    // Verify results
    println!("\n✅ Verification Results:");
    println!("  System A: {}", result.system_a);
    println!("  System B: {}", result.system_b);
    println!("  Metric: {}", result.metric);
    assert_eq!(result.system_a, "khatabook");
    assert_eq!(result.system_b, "tb");
    assert_eq!(result.metric, "tos");

    println!("\n  Classifications: {}", result.classifications.len());
    assert!(!result.classifications.is_empty(), "Should have at least one classification");

    println!("\n  Population Diff:");
    println!("    Common: {}", result.comparison.population_diff.common_count);
    println!("    Missing in B: {}", result.comparison.population_diff.missing_in_b.len());
    println!("    Extra in B: {}", result.comparison.population_diff.extra_in_b.len());

    println!("\n  Data Diff:");
    println!("    Matches: {}", result.comparison.data_diff.matches);
    println!("    Mismatches: {}", result.comparison.data_diff.mismatches);

    // Expected: loan_id=1001 should have mismatch (5500 vs 6000)
    assert!(
        result.comparison.data_diff.mismatches > 0 || 
        result.comparison.population_diff.missing_in_b.len() > 0 ||
        result.comparison.population_diff.extra_in_b.len() > 0,
        "Should have at least one mismatch to demonstrate RCA"
    );

    println!("\n📄 Full Result:");
    println!("{}", result);

    println!("\n✅ Test PASSED: End-to-end reconciliation completed successfully!");
    
    // Cleanup (optional - comment out to keep test files for inspection)
    // fs::remove_dir_all(&test_dir)?;

    Ok(())
}

#[tokio::test]
async fn test_pipeline_construction() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Pipeline Construction\n");

    let test_dir = std::env::temp_dir().join("rca_engine_test_pipeline");
    let metadata_dir = test_dir.join("metadata");
    let data_dir = test_dir.join("data");
    
    fs::create_dir_all(&metadata_dir)?;
    fs::create_dir_all(&data_dir)?;

    // Copy metadata
    let source_metadata = PathBuf::from("metadata");
    for entry in fs::read_dir(&source_metadata)? {
        let entry = entry?;
        fs::copy(entry.path(), metadata_dir.join(entry.file_name()))?;
    }

    // Load metadata
    let metadata = Metadata::load(&metadata_dir)?;

    // Test pipeline construction for khatabook_tos rule
    println!("🔍 Testing pipeline construction for khatabook_tos rule...");
    let rule_id = "khatabook_tos";
    let rule = metadata.get_rule(rule_id)
        .ok_or_else(|| format!("Rule not found: {}", rule_id))?;

    println!("  Rule: {}", rule.id);
    println!("  System: {}", rule.system);
    println!("  Metric: {}", rule.metric);
    println!("  Source entities: {:?}", rule.computation.source_entities);
    println!("  Formula: {}", rule.computation.formula);
    println!("  Aggregation grain: {:?}", rule.computation.aggregation_grain);

    // Create compiler and compile
    let compiler = rca_engine::rule_compiler::RuleCompiler::new(metadata.clone(), data_dir.clone());
    let plan = compiler.compile(rule_id)?;

    println!("\n  Generated Pipeline ({} steps):", plan.steps.len());
    for (i, step) in plan.steps.iter().enumerate() {
        println!("    Step {}: {:?}", i + 1, step);
    }

    // Verify pipeline structure
    assert!(!plan.steps.is_empty(), "Pipeline should have steps");
    
    // Should start with Scan
    match &plan.steps[0] {
        rca_engine::metadata::PipelineOp::Scan { table } => {
            println!("  ✓ Pipeline starts with Scan: {}", table);
            assert!(table == "khatabook_loans" || table.starts_with("khatabook"));
        }
        _ => panic!("Pipeline should start with Scan operation"),
    }

    // Should have joins
    let join_count = plan.steps.iter()
        .filter(|s| matches!(s, rca_engine::metadata::PipelineOp::Join { .. }))
        .count();
    println!("  ✓ Found {} join operations", join_count);
    assert!(join_count >= 1, "Should have at least one join for multi-entity rule");

    // Should have derive or group for formula
    let has_derive_or_group = plan.steps.iter().any(|s| 
        matches!(s, 
            rca_engine::metadata::PipelineOp::Derive { .. } |
            rca_engine::metadata::PipelineOp::Group { .. }
        )
    );
    assert!(has_derive_or_group, "Should have derive or group operation for formula");
    println!("  ✓ Pipeline includes formula computation");

    println!("\n✅ Test PASSED: Pipeline construction works correctly!");

    Ok(())
}

#[tokio::test]
async fn test_metadata_loading() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Metadata Loading\n");

    let metadata_dir = PathBuf::from("metadata");
    let metadata = Metadata::load(&metadata_dir)?;

    println!("✅ Metadata loaded successfully:");
    println!("  Entities: {}", metadata.entities.len());
    println!("  Tables: {}", metadata.tables.len());
    println!("  Rules: {}", metadata.rules.len());
    println!("  Metrics: {}", metadata.metrics.len());
    println!("  Lineage edges: {}", metadata.lineage.edges.len());

    // Verify required entities exist
    let entity_ids: Vec<String> = metadata.entities.iter().map(|e| e.id.clone()).collect();
    assert!(entity_ids.contains(&"loan".to_string()), "Should have loan entity");
    assert!(entity_ids.contains(&"emi".to_string()), "Should have emi entity");
    assert!(entity_ids.contains(&"transaction".to_string()), "Should have transaction entity");

    // Verify required tables exist
    let table_names: Vec<String> = metadata.tables.iter().map(|t| t.name.clone()).collect();
    assert!(table_names.contains(&"khatabook_loans".to_string()), "Should have khatabook_loans table");
    assert!(table_names.contains(&"tb_loan_summary".to_string()), "Should have tb_loan_summary table");

    // Verify required rules exist
    let rule_ids: Vec<String> = metadata.rules.iter().map(|r| r.id.clone()).collect();
    assert!(rule_ids.contains(&"khatabook_tos".to_string()), "Should have khatabook_tos rule");
    assert!(rule_ids.contains(&"tb_tos".to_string()), "Should have tb_tos rule");

    // Verify required metrics exist
    let metric_ids: Vec<String> = metadata.metrics.iter().map(|m| m.id.clone()).collect();
    assert!(metric_ids.contains(&"tos".to_string()), "Should have tos metric");

    println!("\n✅ Test PASSED: All required metadata present!");

    Ok(())
}
