use crate::agent::catalog::CatalogIndex;
use crate::agent::contracts::{AgentRequest, AgentResponse, AgentStatus};
use crate::agent::executor::PlanExecutor;
use crate::agent::grounding;
use crate::agent::memory;
use crate::agent::planner::PlannerAgent;
use crate::agent::runtime::ToolRuntime;

/// MVP agent handler.
///
/// Phase 1-2 will replace this with a real planner/executor + tool runtime.
pub async fn run_agent(req: AgentRequest) -> AgentResponse {
    let session = memory::get_session(&req.session_id);
    let resolved_query = memory::resolve_references(&session, &req.user_query);

    let catalog = match CatalogIndex::load("metadata", "data") {
        Ok(c) => c,
        Err(e) => {
            return AgentResponse {
                status: AgentStatus::Error,
                final_answer: None,
                clarification: None,
                plan: None,
                trace: vec![],
                error: Some(format!("Failed to load catalog: {}", e)),
            };
        }
    };
    let runtime = ToolRuntime::new(catalog);
    let grounding_ctx = grounding::build_grounding_context(&runtime.catalog, &resolved_query, 2, 3).ok();
    let (plan, trace) = PlannerAgent::plan(&runtime, &resolved_query, grounding_ctx.as_ref());
    let resp = PlanExecutor::execute(&runtime, plan, trace);

    // Update session memory from trace/tool results.
    for ev in &resp.trace {
        if ev.event_type != crate::agent::contracts::TraceEventType::ToolResult {
            continue;
        }
        if let Some(tool) = ev.payload.get("tool_name").and_then(|v| v.as_str()) {
            if tool == "open_table" || tool == "head" || tool == "tail" || tool == "show_schema" {
                if let Some(table_obj) = ev
                    .payload
                    .get("result")
                    .and_then(|r| r.get("table").or_else(|| r.get("schema").and_then(|s| s.get("table"))))
                {
                    let table_id = table_obj
                        .get("table_id")
                        .or_else(|| table_obj.get("id"))
                        .or_else(|| table_obj.get("name"))
                        .and_then(|v| v.as_str());
                    let system = table_obj.get("system").and_then(|v| v.as_str());
                    if let Some(tid) = table_id {
                        memory::update_last_table(&req.session_id, tid, system);
                    }
                }
            }
        }
    }

    resp
}


