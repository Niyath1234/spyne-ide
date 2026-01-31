use crate::error::{RcaError, Result};
use crate::llm::{Explanation, LlmClient};
use crate::rca::RcaResult;

pub struct ExplanationEngine {
    llm: LlmClient,
}

impl ExplanationEngine {
    pub fn new(llm: LlmClient) -> Self {
        Self { llm }
    }
    
    pub async fn explain(&self, result: &RcaResult) -> Result<Explanation> {
        self.llm.explain_rca(result).await
    }
}


