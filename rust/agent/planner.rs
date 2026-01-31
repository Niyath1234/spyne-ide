use crate::agent::contracts::{
    AgentResponse, AgentStatus, ClarificationChoice, ClarificationRequestV2, Plan, PlanStep,
    TraceEvent, TraceEventType,
};
use crate::agent::runtime::ToolRuntime;
use serde_json::json;
use std::collections::HashMap;

pub struct PlannerAgent;

impl PlannerAgent {
    pub fn plan(
        runtime: &ToolRuntime,
        user_query: &str,
        grounding: Option<&crate::agent::grounding::GroundingContext>,
    ) -> (Plan, Vec<TraceEvent>) {
        let mut trace = Vec::new();

        let q = user_query.to_lowercase();
        let mut steps: Vec<PlanStep> = Vec::new();

        if q.trim() == "list systems" || q.contains("show systems") {
            steps.push(PlanStep {
                id: "step_1".to_string(),
                tool_name: "list_systems".to_string(),
                args: json!({}),
                budget: HashMap::new(),
                expected_artifact: Some("systems".to_string()),
            });
        } else if q.starts_with("search systems") {
            let query = user_query.splitn(3, ' ').skip(2).collect::<Vec<_>>().join(" ");
            steps.push(PlanStep {
                id: "step_1".to_string(),
                tool_name: "search_systems".to_string(),
                args: json!({ "query": query, "limit": 10 }),
                budget: HashMap::new(),
                expected_artifact: Some("system_candidates".to_string()),
            });
        } else if q.starts_with("list tables") {
            let system = user_query.splitn(3, ' ').skip(2).collect::<Vec<_>>().join(" ");
            steps.push(PlanStep {
                id: "step_1".to_string(),
                tool_name: "list_tables".to_string(),
                args: json!({ "system": system }),
                budget: HashMap::new(),
                expected_artifact: Some("tables".to_string()),
            });
        } else if q.starts_with("open ") {
            let table_id = user_query.splitn(2, ' ').nth(1).unwrap_or("").trim();
            steps.push(PlanStep {
                id: "step_1".to_string(),
                tool_name: "open_table".to_string(),
                args: json!({ "table_id": table_id }),
                budget: HashMap::new(),
                expected_artifact: Some("schema".to_string()),
            });
            steps.push(PlanStep {
                id: "step_2".to_string(),
                tool_name: "head".to_string(),
                args: json!({ "table_id": table_id, "n": 10 }),
                budget: HashMap::new(),
                expected_artifact: Some("preview".to_string()),
            });
        } else if q.starts_with("head ") {
            // Formats:
            // - "head <table_id> <n>"
            // - "head <table_id>"
            let parts: Vec<&str> = user_query.split_whitespace().collect();
            let n = parts
                .last()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(10);
            let table_id = if parts.len() >= 3 && parts.last().and_then(|s| s.parse::<usize>().ok()).is_some() {
                parts[1..parts.len() - 1].join(" ")
            } else {
                parts[1..].join(" ")
            };
            steps.push(PlanStep {
                id: "step_1".to_string(),
                tool_name: "head".to_string(),
                args: json!({ "table_id": table_id, "n": n }),
                budget: HashMap::new(),
                expected_artifact: Some("preview".to_string()),
            });
        } else if q.starts_with("tail ") {
            let parts: Vec<&str> = user_query.split_whitespace().collect();
            let n = parts
                .last()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(10);
            let table_id = if parts.len() >= 3 && parts.last().and_then(|s| s.parse::<usize>().ok()).is_some() {
                parts[1..parts.len() - 1].join(" ")
            } else {
                parts[1..].join(" ")
            };
            steps.push(PlanStep {
                id: "step_1".to_string(),
                tool_name: "tail".to_string(),
                args: json!({ "table_id": table_id, "n": n, "max_scan_rows": 5000 }),
                budget: HashMap::new(),
                expected_artifact: Some("preview".to_string()),
            });
        } else {
            // Default: search tables from natural language, then open the top one.
            steps.push(PlanStep {
                id: "step_1".to_string(),
                tool_name: "search_tables".to_string(),
                args: json!({ "query": user_query, "limit": 8 }),
                budget: HashMap::new(),
                expected_artifact: Some("candidate_tables".to_string()),
            });
            steps.push(PlanStep {
                id: "step_2".to_string(),
                tool_name: "open_table".to_string(),
                args: json!({ "table_id": "<top_candidate>" }),
                budget: HashMap::new(),
                expected_artifact: Some("schema".to_string()),
            });
        }

        let plan = Plan {
            goal: format!("Assist with: {}", user_query),
            steps,
        };

        trace.push(TraceEvent::now(TraceEventType::Plan, json!({ "plan": plan })));
        if let Some(g) = grounding {
            trace.push(TraceEvent::now(
                TraceEventType::ThoughtSummary,
                json!({
                    "summary": "Grounding context prepared (schemas/samples/KB). Planner should only reason using these artifacts.",
                    "provenance": g.provenance,
                }),
            ));
        }
        trace.push(TraceEvent::now(
            TraceEventType::ThoughtSummary,
            json!({
                "summary": "Heuristic planner selected initial tool sequence. Next step: executor runs tool calls with guardrails and may ask for clarification."
            }),
        ));

        // Silence unused warning while we’re still heuristic-only (runtime used in executor).
        let _ = runtime.catalog.tables.len();

        (plan, trace)
    }

    pub fn maybe_build_clarification(search_tables_result: &serde_json::Value) -> Option<ClarificationRequestV2> {
        let results = search_tables_result.get("results")?.as_array()?.clone();
        if results.len() <= 1 {
            return None;
        }

        let mut choices: Vec<ClarificationChoice> = Vec::new();
        for r in results.iter().take(5) {
            let id = r.get("id")?.as_str()?.to_string();
            let label = r.get("label")?.as_str()?.to_string();
            let score = r.get("score")?.as_f64().unwrap_or(0.0);
            choices.push(ClarificationChoice {
                id: id.clone(),
                label,
                score,
                preview_action: Some(json!({ "tool_name": "open_table", "args": { "table_id": id } })),
            });
        }

        Some(ClarificationRequestV2 {
            question: "I found multiple possible tables. Which one should I open?".to_string(),
            choices,
            expected_next: Some(json!({ "tool_name": "open_table" })),
        })
    }

    pub fn respond_needs_clarification(
        plan: Plan,
        mut trace: Vec<TraceEvent>,
        clarification: ClarificationRequestV2,
    ) -> AgentResponse {
        trace.push(TraceEvent::now(
            TraceEventType::ThoughtSummary,
            json!({ "summary": "Need user selection to proceed safely." }),
        ));
        AgentResponse {
            status: AgentStatus::NeedsClarification,
            final_answer: None,
            clarification: Some(clarification),
            plan: Some(plan),
            trace,
            error: None,
        }
    }
}


