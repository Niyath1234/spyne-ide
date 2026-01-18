//! Test PostgreSQL database connection
//! 
//! Run with: cargo run --bin test_db_connection

use sqlx::PgPool;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    
    println!("🔌 Testing PostgreSQL Connection...\n");
    
    let database_url = match env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("❌ DATABASE_URL not found in environment variables.");
            eprintln!("   Please set DATABASE_URL in your .env file:");
            eprintln!("   DATABASE_URL=postgresql://postgres:password@localhost:5432/rca_engine");
            return Err("DATABASE_URL not set".into());
        }
    };
    
    println!("📡 Connecting to: {}", 
        database_url.split('@').nth(1).unwrap_or("database"));
    
    match PgPool::connect(&database_url).await {
        Ok(pool) => {
            println!("✅ Connected successfully!\n");
            
            // Test basic query
            println!("🧪 Running test queries...\n");
            
            // Check if tables exist
            let table_count: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM information_schema.tables 
                 WHERE table_schema = 'public'"
            )
            .fetch_one(&pool)
            .await?;
            
            println!("   📊 Tables in database: {}", table_count.0);
            
            // Check entities table
            match sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM entities")
                .fetch_one(&pool)
                .await
            {
                Ok((count,)) => println!("   📋 Entities: {}", count),
                Err(e) => println!("   ⚠️  Entities table not found or error: {}", e),
            }
            
            // Check tables table
            match sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM tables")
                .fetch_one(&pool)
                .await
            {
                Ok((count,)) => println!("   📋 Tables metadata: {}", count),
                Err(e) => println!("   ⚠️  Tables metadata table not found or error: {}", e),
            }
            
            // Check rules table
            match sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM rules")
                .fetch_one(&pool)
                .await
            {
                Ok((count,)) => println!("   📋 Rules: {}", count),
                Err(e) => println!("   ⚠️  Rules table not found or error: {}", e),
            }
            
            // Check UUID extension
            match sqlx::query_as::<_, (String,)>(
                "SELECT extname FROM pg_extension WHERE extname = 'uuid-ossp'"
            )
            .fetch_optional(&pool)
            .await
            {
                Ok(Some(_)) => println!("   ✅ UUID extension installed"),
                Ok(None) => println!("   ⚠️  UUID extension not found (run: CREATE EXTENSION \"uuid-ossp\";)"),
                Err(e) => println!("   ⚠️  Error checking extension: {}", e),
            }
            
            println!("\n✅ Database connection test completed successfully!");
            println!("   You're ready to start migrating data!");
            
            Ok(())
        }
        Err(e) => {
            eprintln!("❌ Connection failed: {}", e);
            eprintln!("\n💡 Troubleshooting:");
            eprintln!("   1. Is PostgreSQL running? (check with: pg_isready)");
            eprintln!("   2. Is the database 'rca_engine' created?");
            eprintln!("   3. Are the username and password correct?");
            eprintln!("   4. Is port 5432 accessible?");
            Err(format!("Connection failed: {}", e).into())
        }
    }
}

