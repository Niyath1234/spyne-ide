//! Migration script to load JSON metadata into PostgreSQL
//! 
//! Run with: cargo run --bin migrate_metadata

use rca_engine::metadata::Metadata;
use rca_engine::db::{init_pool, MetadataRepository};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    
    println!("🚀 RCA Engine - Metadata Migration to PostgreSQL\n");
    println!("=" .repeat(70));
    
    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env file");
    
    println!("📡 Connecting to database...");
    let pool = init_pool(&database_url).await?;
    println!("✅ Connected successfully!\n");
    
    // Load metadata from JSON files
    let metadata_dir = PathBuf::from("metadata");
    println!("📂 Loading metadata from: {:?}", metadata_dir);
    
    let metadata = match Metadata::load(&metadata_dir) {
        Ok(m) => {
            println!("✅ Loaded metadata from JSON files:");
            println!("   - Entities: {}", m.entities.len());
            println!("   - Tables: {}", m.tables.len());
            println!("   - Metrics: {}", m.metrics.len());
            println!("   - Rules: {}", m.rules.len());
            println!("   - Lineage Edges: {}", m.lineage.edges.len());
            println!("   - Time Rules: {} as_of, {} lateness", 
                m.time_rules.as_of_rules.len(), 
                m.time_rules.lateness_rules.len());
            println!("   - Exceptions: {}", m.exceptions.exceptions.len());
            m
        }
        Err(e) => {
            eprintln!("❌ Failed to load metadata: {}", e);
            return Err(e.into());
        }
    };
    
    println!("\n📥 Inserting metadata into PostgreSQL...");
    println!("-" .repeat(70));
    
    let repo = MetadataRepository::new(pool);
    
    match repo.insert_all(&metadata).await {
        Ok(()) => {
            println!("✅ Successfully migrated all metadata to PostgreSQL!");
            println!("\n📊 Migration Summary:");
            println!("   ✓ {} entities", metadata.entities.len());
            println!("   ✓ {} tables", metadata.tables.len());
            println!("   ✓ {} metrics", metadata.metrics.len());
            println!("   ✓ {} rules", metadata.rules.len());
            println!("   ✓ {} lineage edges", metadata.lineage.edges.len());
            println!("   ✓ {} business labels (systems + metrics + recon types)", 
                metadata.business_labels.systems.len() + 
                metadata.business_labels.metrics.len() + 
                metadata.business_labels.reconciliation_types.len());
            println!("   ✓ {} time rules", 
                metadata.time_rules.as_of_rules.len() + 
                metadata.time_rules.lateness_rules.len());
            println!("   ✓ {} exceptions", metadata.exceptions.exceptions.len());
            
            println!("\n🎉 Migration completed successfully!");
            println!("   You can now use PostgreSQL as the metadata source.");
        }
        Err(e) => {
            eprintln!("❌ Migration failed: {}", e);
            return Err(e.into());
        }
    }
    
    // Verify the migration
    println!("\n🔍 Verifying migration...");
    match repo.load_all().await {
        Ok(loaded_metadata) => {
            println!("✅ Verification successful!");
            println!("   - Entities: {}", loaded_metadata.entities.len());
            println!("   - Tables: {}", loaded_metadata.tables.len());
            println!("   - Metrics: {}", loaded_metadata.metrics.len());
            println!("   - Rules: {}", loaded_metadata.rules.len());
            
            if loaded_metadata.entities.len() == metadata.entities.len() &&
               loaded_metadata.tables.len() == metadata.tables.len() &&
               loaded_metadata.metrics.len() == metadata.metrics.len() &&
               loaded_metadata.rules.len() == metadata.rules.len() {
                println!("\n✨ All counts match! Migration is complete and verified.");
            } else {
                println!("\n⚠️  Warning: Some counts don't match. Please review the data.");
            }
        }
        Err(e) => {
            eprintln!("❌ Verification failed: {}", e);
            return Err(e.into());
        }
    }
    
    Ok(())
}

