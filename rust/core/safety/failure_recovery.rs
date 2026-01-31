//! Failure Recovery
//! 
//! Implements retry logic and graceful degradation:
//! - Retry transient failures with exponential backoff
//! - Fallback to simpler plan if complex plan fails
//! - Return partial results if full execution fails

use crate::error::{RcaError, Result};
use std::time::Duration;
use std::future::Future;

/// Retry policy configuration
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries
    pub max_retries: usize,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new(
        max_retries: usize,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_multiplier: f64,
    ) -> Self {
        Self {
            max_retries,
            initial_delay,
            max_delay,
            backoff_multiplier,
        }
    }
    
    /// Create a conservative retry policy (fewer retries, shorter delays)
    pub fn conservative() -> Self {
        Self {
            max_retries: 2,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 1.5,
        }
    }
    
    /// Create an aggressive retry policy (more retries, longer delays)
    pub fn aggressive() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.5,
        }
    }
    
    /// Calculate delay for retry attempt (exponential backoff)
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let delay_ms = (self.initial_delay.as_millis() as f64) * 
                       (self.backoff_multiplier.powi(attempt as i32));
        let delay_ms = delay_ms.min(self.max_delay.as_millis() as f64);
        Duration::from_millis(delay_ms as u64)
    }
}

/// Failure recovery handler
pub struct FailureRecovery {
    retry_policy: RetryPolicy,
}

impl FailureRecovery {
    /// Create a new failure recovery handler with default retry policy
    pub fn new() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
        }
    }
    
    /// Create with custom retry policy
    pub fn with_retry_policy(retry_policy: RetryPolicy) -> Self {
        Self { retry_policy }
    }
    
    /// Retry an async operation with exponential backoff
    /// 
    /// # Arguments
    /// * `operation` - Async operation to retry
    /// * `is_retryable` - Function to determine if error is retryable
    /// 
    /// # Returns
    /// Result of the operation, or error if all retries exhausted
    pub async fn retry_with_backoff<F, Fut, T, E>(
        &self,
        mut operation: F,
        is_retryable: impl Fn(&E) -> bool,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut last_error: Option<E> = None;
        
        for attempt in 0..=self.retry_policy.max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    last_error = Some(e);
                    
                    // Check if error is retryable
                    if let Some(ref err) = last_error {
                        if !is_retryable(err) {
                            return Err(RcaError::Execution(format!(
                                "Non-retryable error: {}",
                                err
                            )));
                        }
                    }
                    
                    // If not last attempt, wait before retrying
                    if attempt < self.retry_policy.max_retries {
                        let delay = self.retry_policy.delay_for_attempt(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        
        // All retries exhausted
        Err(RcaError::Execution(format!(
            "Operation failed after {} retries: {}",
            self.retry_policy.max_retries,
            last_error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string())
        )))
    }
    
    /// Check if an error is transient (retryable)
    pub fn is_transient_error(error: &RcaError) -> bool {
        match error {
            RcaError::Execution(msg) => {
                // Check for transient error patterns
                msg.contains("timeout") ||
                msg.contains("network") ||
                msg.contains("connection") ||
                msg.contains("temporary") ||
                msg.contains("busy") ||
                msg.contains("locked")
            }
            _ => false,
        }
    }
    
    /// Check if an error is permanent (not retryable)
    pub fn is_permanent_error(error: &RcaError) -> bool {
        match error {
            RcaError::Execution(msg) => {
                // Check for permanent error patterns
                msg.contains("not found") ||
                msg.contains("invalid") ||
                msg.contains("permission denied") ||
                msg.contains("syntax error") ||
                msg.contains("type mismatch")
            }
            _ => true, // Unknown errors are considered permanent
        }
    }
}

impl Default for FailureRecovery {
    fn default() -> Self {
        Self::new()
    }
}

/// Graceful degradation strategies
#[derive(Debug, Clone)]
pub enum DegradationStrategy {
    /// Fallback to simpler execution plan
    SimplerPlan,
    /// Fallback to fixed pipeline (non-agentic)
    FixedPipeline,
    /// Return partial results
    PartialResults,
    /// Skip optional phases
    SkipOptional,
}

/// Result with degradation information
#[derive(Debug)]
pub struct DegradedResult<T> {
    /// The result (may be partial)
    pub result: T,
    /// Degradation strategies applied
    pub strategies_applied: Vec<DegradationStrategy>,
    /// Whether result is complete
    pub is_complete: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_retry_with_backoff_success() {
        let recovery = FailureRecovery::new();
        let attempts = std::sync::Arc::new(std::sync::Mutex::new(0));
        let attempts_clone = attempts.clone();
        
        let result = recovery.retry_with_backoff(
            move || {
                let attempts = attempts_clone.clone();
                async move {
                    let mut count = attempts.lock().unwrap();
                    *count += 1;
                    if *count < 2 {
                        Err::<(), _>("Temporary error".to_string())
                    } else {
                        Ok(())
                    }
                }
            },
            |_| true, // All errors are retryable
        ).await;
        
        assert!(result.is_ok());
        assert_eq!(*attempts.lock().unwrap(), 2);
    }
    
    #[tokio::test]
    async fn test_retry_with_backoff_exhausted() {
        let recovery = FailureRecovery::new();
        
        let result = recovery.retry_with_backoff(
            || async {
                Err::<(), _>("Permanent error".to_string())
            },
            |_| true, // All errors are retryable
        ).await;
        
        assert!(result.is_err());
    }
    
    #[test]
    fn test_retry_policy_delay_calculation() {
        let policy = RetryPolicy::default();
        
        let delay1 = policy.delay_for_attempt(0);
        let delay2 = policy.delay_for_attempt(1);
        
        assert!(delay2 > delay1); // Exponential backoff
    }
}

