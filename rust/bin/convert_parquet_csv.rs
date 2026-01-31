///! Convert multi_grain_test parquet files to CSV for simplified RCA testing

use polars::prelude::*;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Converting multi_grain_test parquet files to CSV...\n");
    
    // Create output directory
    let csv_base = Path::new("test_data/multi_grain_csv");
    fs::create_dir_all(csv_base)?;
    
    // System A tables
    let system_a_tables = vec![
        "loan_summary",
        "customer_loan_mapping",
        "daily_interest_accruals",
        "daily_fees",
        "daily_penalties",
        "emi_schedule",
        "emi_transactions",
        "detailed_transactions",
        "fee_details",
        "customer_summary",
    ];
    
    println!("Converting System A tables:");
    for table in &system_a_tables {
        let parquet_path = format!("data/multi_grain_test/system_a/{}.parquet", table);
        let csv_path = csv_base.join(format!("system_a_{}.csv", table));
        
        match convert_parquet_to_csv(&parquet_path, &csv_path) {
            Ok(rows) => println!("✅ {}: {} rows → {:?}", table, rows, csv_path),
            Err(e) => println!("❌ {}: {}", table, e),
        }
    }
    
    // System B tables
    let system_b_tables = vec!["loan_summary"];
    
    println!("\nConverting System B tables:");
    for table in &system_b_tables {
        let parquet_path = format!("data/multi_grain_test/system_b/{}.parquet", table);
        let csv_path = csv_base.join(format!("system_b_{}.csv", table));
        
        match convert_parquet_to_csv(&parquet_path, &csv_path) {
            Ok(rows) => println!("✅ {}: {} rows → {:?}", table, rows, csv_path),
            Err(e) => println!("❌ {}: {}", table, e),
        }
    }
    
    println!("\n✅ Conversion complete!");
    println!("CSV files saved to: {:?}", csv_base);
    
    Ok(())
}

fn convert_parquet_to_csv(
    parquet_path: &str,
    csv_path: &Path,
) -> Result<usize, Box<dyn std::error::Error>> {
    // Read parquet file
    let df = ParquetReader::new(std::fs::File::open(parquet_path)?)
        .finish()?;
    
    let row_count = df.height();
    
    // Write to CSV
    let mut file = std::fs::File::create(csv_path)?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(&mut df.clone())?;
    
    Ok(row_count)
}





