use thiserror::Error;

#[derive(Error, Debug)]
pub enum RcaError {
    #[error("Metadata error: {0}")]
    Metadata(String),
    
    #[error("LLM error: {0}")]
    Llm(String),
    
    #[error("Graph error: {0}")]
    Graph(String),
    
    #[error("Execution error: {0}")]
    Execution(String),
    
    #[error("Identity resolution error: {0}")]
    Identity(String),
    
    #[error("Time logic error: {0}")]
    Time(String),
    
    #[error("Ambiguity resolution error: {0}")]
    Ambiguity(String),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("Polars error: {0}")]
    Polars(String),
}

pub type Result<T> = std::result::Result<T, RcaError>;

