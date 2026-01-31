use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Success,
    NeedsClarification,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRequest {
    pub session_id: String,
    pub user_query: String,
    #[serde(default)]
    pub ui_context: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentContinueRequest {
    pub session_id: String,
    pub choice_id: String,
    #[serde(default)]
    pub ui_context: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub goal: String,
    pub steps: Vec<PlanStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    pub id: String,
    pub tool_name: String,
    #[serde(default)]
    pub args: Value,
    #[serde(default)]
    pub budget: HashMap<String, Value>,
    #[serde(default)]
    pub expected_artifact: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TraceEventType {
    Plan,
    ThoughtSummary,
    ToolCall,
    ToolResult,
    Error,
    Retry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub ts_ms: u64,
    pub event_type: TraceEventType,
    #[serde(default)]
    pub payload: Value,
}

impl TraceEvent {
    pub fn now(event_type: TraceEventType, payload: Value) -> Self {
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            ts_ms,
            event_type,
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClarificationChoice {
    pub id: String,
    pub label: String,
    pub score: f64,
    #[serde(default)]
    pub preview_action: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClarificationRequestV2 {
    pub question: String,
    pub choices: Vec<ClarificationChoice>,
    #[serde(default)]
    pub expected_next: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResponse {
    pub status: AgentStatus,
    #[serde(default)]
    pub final_answer: Option<String>,
    #[serde(default)]
    pub clarification: Option<ClarificationRequestV2>,
    #[serde(default)]
    pub plan: Option<Plan>,
    #[serde(default)]
    pub trace: Vec<TraceEvent>,
    #[serde(default)]
    pub error: Option<String>,
}


