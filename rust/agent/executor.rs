use crate::agent::contracts::{AgentResponse, AgentStatus, Plan, TraceEvent, TraceEventType};
use crate::agent::planner::PlannerAgent;
use crate::agent::runtime::ToolRuntime;
use serde_json::json;

pub struct PlanExecutor;

impl PlanExecutor {
    pub fn execute(runtime: &ToolRuntime, plan: Plan, mut trace: Vec<TraceEvent>) -> AgentResponse {
        let mut artifacts: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        let max_retries_per_step: usize = 2;

        for step in &plan.steps {
            trace.push(TraceEvent::now(
                TraceEventType::ToolCall,
                json!({ "tool_name": step.tool_name, "args": step.args, "step_id": step.id }),
            ));

            // Replace placeholder args from artifacts (very MVP).
            let mut args = step.args.clone();
            if step.tool_name == "open_table" {
                if let Some(tid) = args.get("table_id").and_then(|v| v.as_str()) {
                    if tid == "<top_candidate>" {
                        if let Some(st) = artifacts.get("candidate_tables") {
                            if let Some(first) = st.get("results").and_then(|v| v.as_array()).and_then(|a| a.first()) {
                                if let Some(id) = first.get("id").and_then(|v| v.as_str()) {
                                    args = json!({ "table_id": id });
                                }
                            }
                        }
                    }
                }
            }

            let mut attempt_args = vec![args.clone()];

            // If we have candidate tables from search, allow fallback attempts for table-targeting tools.
            if matches!(step.tool_name.as_str(), "open_table" | "show_schema" | "head" | "tail" | "search_values") {
                if let Some(cands) = artifacts.get("candidate_tables") {
                    if let Some(arr) = cands.get("results").and_then(|v| v.as_array()) {
                        for cand in arr.iter().take(3) {
                            if let Some(id) = cand.get("id").and_then(|v| v.as_str()) {
                                // Avoid duplicating the first attempt if it already matches.
                                let current = attempt_args
                                    .first()
                                    .and_then(|a| a.get("table_id"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if current != id {
                                    let mut a = attempt_args[0].clone();
                                    if a.is_object() {
                                        a["table_id"] = json!(id);
                                    }
                                    attempt_args.push(a);
                                }
                            }
                        }
                    }
                }
            }

            let mut last_err: Option<String> = None;
            let mut ok_result: Option<serde_json::Value> = None;
            for (attempt_idx, a) in attempt_args.iter().take(1 + max_retries_per_step).enumerate() {
                if attempt_idx > 0 {
                    trace.push(TraceEvent::now(
                        TraceEventType::Retry,
                        json!({
                            "step_id": step.id,
                            "tool_name": step.tool_name,
                            "attempt": attempt_idx + 1,
                            "args": a
                        }),
                    ));
                }
                match runtime.execute(&step.tool_name, a) {
                    Ok(r) => {
                        ok_result = Some(r);
                        last_err = None;
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e.to_string());
                        continue;
                    }
                }
            }

            match (ok_result, last_err) {
                (Some(result), _) => {
                    trace.push(TraceEvent::now(
                        TraceEventType::ToolResult,
                        json!({ "tool_name": step.tool_name, "result": result, "step_id": step.id }),
                    ));

                    if let Some(name) = &step.expected_artifact {
                        artifacts.insert(name.clone(), result.clone());
                    }

                    if step.tool_name == "search_tables" {
                        if let Some(clar) = PlannerAgent::maybe_build_clarification(&result) {
                            return PlannerAgent::respond_needs_clarification(plan, trace, clar);
                        }
                    }
                }
                (None, Some(e)) => {
                    trace.push(TraceEvent::now(
                        TraceEventType::Error,
                        json!({ "step_id": step.id, "tool_name": step.tool_name, "error": e }),
                    ));
                    return AgentResponse {
                        status: AgentStatus::Error,
                        final_answer: None,
                        clarification: None,
                        plan: Some(plan),
                        trace,
                        error: Some(e),
                    };
                }
                (None, None) => {
                    return AgentResponse {
                        status: AgentStatus::Error,
                        final_answer: None,
                        clarification: None,
                        plan: Some(plan),
                        trace,
                        error: Some("Unknown executor error".to_string()),
                    };
                }
            }
        }

        let final_answer = json!({
            "message": "Tool execution complete (MVP).",
            "artifacts": artifacts
        })
        .to_string();

        trace.push(TraceEvent::now(
            TraceEventType::ThoughtSummary,
            json!({ "summary": "Executed plan without errors. Returning artifacts." }),
        ));

        AgentResponse {
            status: AgentStatus::Success,
            final_answer: Some(final_answer),
            clarification: None,
            plan: Some(plan),
            trace,
            error: None,
        }
    }
}


