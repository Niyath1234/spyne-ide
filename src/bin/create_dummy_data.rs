use polars::prelude::*;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create directories
    fs::create_dir_all("data/khatabook")?;
    fs::create_dir_all("data/tb")?;

    // Create dummy khatabook_loans data
    let khatabook_loans = df! [
        "loan_id" => ["L001", "L002", "L003", "L004", "L005"],
        "disbursement_date" => ["2025-01-15", "2025-02-20", "2025-03-10", "2025-04-05", "2025-05-12"],
        "disbursement_amount" => [100000.0, 150000.0, 200000.0, 120000.0, 180000.0],
        "principal_amount" => [100000.0, 150000.0, 200000.0, 120000.0, 180000.0],
        "final_outstanding" => [75000.0, 120000.0, 180000.0, 90000.0, 150000.0],
        "leadger_balance" => [75000.0, 120000.0, 180000.0, 90000.0, 150000.0],
        "customer_id" => ["C001", "C002", "C003", "C004", "C005"],
        "branch_code" => ["BR001", "BR002", "BR001", "BR003", "BR002"],
    ]?;
    
    let mut file = std::fs::File::create("data/khatabook/loans.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut khatabook_loans.clone())?;
    println!("✅ Created data/khatabook/loans.parquet");

    // Create dummy khatabook_emis data
    let khatabook_emis = df! [
        "loan_id" => ["L001", "L001", "L001", "L002", "L002", "L003", "L003", "L004", "L005"],
        "emi_number" => [1, 2, 3, 1, 2, 1, 2, 1, 1],
        "due_date" => ["2025-02-15", "2025-03-15", "2025-04-15", "2025-03-20", "2025-04-20", "2025-04-10", "2025-05-10", "2025-05-05", "2025-06-12"],
        "amount" => [10000.0, 10000.0, 10000.0, 15000.0, 15000.0, 20000.0, 20000.0, 12000.0, 18000.0],
        "status" => ["PAID", "PAID", "PENDING", "PAID", "PENDING", "PENDING", "PENDING", "PENDING", "PENDING"],
    ]?;
    
    let mut file = std::fs::File::create("data/khatabook/emis.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut khatabook_emis.clone())?;
    println!("✅ Created data/khatabook/emis.parquet");

    // Create dummy khatabook_transactions data
    let khatabook_transactions = df! [
        "transaction_id" => ["T001", "T002", "T003", "T004", "T005", "T006", "T007"],
        "loan_id" => ["L001", "L001", "L001", "L002", "L002", "L003", "L004"],
        "emi_number" => [1, 2, 3, 1, 2, 1, 1],
        "amount" => [10000.0, 10000.0, 5000.0, 15000.0, 10000.0, 20000.0, 30000.0],
        "transaction_date" => ["2025-02-10", "2025-03-12", "2025-04-10", "2025-03-18", "2025-04-15", "2025-04-05", "2025-05-01"],
        "type" => ["repayment", "repayment", "repayment", "repayment", "repayment", "repayment", "repayment"],
    ]?;
    
    let mut file = std::fs::File::create("data/khatabook/transactions.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut khatabook_transactions.clone())?;
    println!("✅ Created data/khatabook/transactions.parquet");

    // Create dummy tb_loans data
    let tb_loans = df! [
        "loan_id" => ["L001", "L002", "L003", "L004", "L005"],
        "disbursement_date" => ["2025-01-15", "2025-02-20", "2025-03-10", "2025-04-05", "2025-05-12"],
        "disbursement_amount" => [100000.0, 150000.0, 200000.0, 120000.0, 180000.0],
        "customer_id" => ["C001", "C002", "C003", "C004", "C005"],
        "branch_code" => ["BR001", "BR002", "BR001", "BR003", "BR002"],
    ]?;
    
    let mut file = std::fs::File::create("data/tb/loans.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut tb_loans.clone())?;
    println!("✅ Created data/tb/loans.parquet");

    // Create dummy tb_loan_summary data (with different values to create mismatch)
    let tb_loan_summary = df! [
        "loan_id" => ["L001", "L002", "L003", "L004", "L005"],
        "as_of_date" => ["2025-05-01", "2025-05-01", "2025-05-01", "2025-05-01", "2025-05-01"],
        "final_outstanding" => [80000.0, 125000.0, 185000.0, 95000.0, 155000.0],
        "principal_outstanding" => [70000.0, 110000.0, 170000.0, 85000.0, 140000.0],
        "interest_outstanding" => [10000.0, 15000.0, 15000.0, 10000.0, 15000.0],
        "total_outstanding" => [80000.0, 125000.0, 185000.0, 95000.0, 155000.0],
    ]?;
    
    let mut file = std::fs::File::create("data/tb/loan_summary.parquet")?;
    ParquetWriter::new(&mut file).finish(&mut tb_loan_summary.clone())?;
    println!("✅ Created data/tb/loan_summary.parquet");

    println!("\n✅ All dummy data files created successfully!");
    Ok(())
}
