use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionState {
    #[serde(default)]
    pub last_system: Option<String>,
    #[serde(default)]
    pub last_table: Option<String>,
    #[serde(default)]
    pub recent_tables: Vec<String>,
    #[serde(default)]
    pub user_defaults: HashMap<String, Value>,
}

lazy_static::lazy_static! {
    static ref SESSIONS: Mutex<HashMap<String, SessionState>> = Mutex::new(HashMap::new());
}

pub fn get_session(session_id: &str) -> SessionState {
    SESSIONS
        .lock()
        .ok()
        .and_then(|m| m.get(session_id).cloned())
        .unwrap_or_default()
}

pub fn set_session(session_id: &str, state: SessionState) {
    if let Ok(mut m) = SESSIONS.lock() {
        m.insert(session_id.to_string(), state);
    }
}

pub fn update_last_table(session_id: &str, table_id: &str, system: Option<&str>) {
    if let Ok(mut m) = SESSIONS.lock() {
        let s = m.entry(session_id.to_string()).or_default();
        s.last_table = Some(table_id.to_string());
        if let Some(sys) = system {
            s.last_system = Some(sys.to_string());
        }
        if !s.recent_tables.contains(&table_id.to_string()) {
            s.recent_tables.insert(0, table_id.to_string());
            s.recent_tables.truncate(10);
        }
    }
}

pub fn resolve_references(session: &SessionState, query: &str) -> String {
    let mut q = query.to_string();
    if let Some(t) = &session.last_table {
        q = q.replace("that table", t);
        q = q.replace("this table", t);
        q = q.replace("the table", t);
    }
    if let Some(s) = &session.last_system {
        q = q.replace("that system", s);
        q = q.replace("this system", s);
    }
    q
}






