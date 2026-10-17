//! Prometheus text exposition of live mesh state (served on the mesh listener only).
use crate::health::ALL_MODELS;
use crate::state::AppState;
use axum::extract::State;
use axum::response::IntoResponse;
use std::fmt::Write;
use std::sync::Arc;

fn label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', " ")
}

pub fn render(state: &AppState, now: i64) -> String {
    let mut out = String::new();
    let mesh = state.mesh.lock().unwrap();
    let node = label(&state.cfg.node_id);
    let _ = writeln!(out, "# TYPE geoclass_inflight gauge\ngeoclass_inflight{{node=\"{node}\"}} {}", state.inflight.load(std::sync::atomic::Ordering::Relaxed));
    let _ = writeln!(out, "# TYPE geoclass_bindings gauge\ngeoclass_bindings{{node=\"{node}\"}} {}", mesh.bindings.as_ref().map(|b| b.len()).unwrap_or(0));
    let live = mesh.peers.values().filter(|p| state.is_live(p, now)).count();
    let _ = writeln!(out, "# TYPE geoclass_peers gauge\ngeoclass_peers{{node=\"{node}\",live=\"true\"}} {live}\ngeoclass_peers{{node=\"{node}\",live=\"false\"}} {}", mesh.peers.len() - live);
    let _ = writeln!(out, "# TYPE geoclass_region_state gauge\n# HELP geoclass_region_state 0 green, 1 amber, 2 red");
    let _ = writeln!(out, "# TYPE geoclass_region_window gauge\n# TYPE geoclass_region_weight gauge\n# TYPE geoclass_region_available gauge");
    for region in state.region_names(&mesh, now) {
        let r = label(&region);
        let d = label(&state.domain(&mesh, &region));
        let s = mesh.health.state(&region, ALL_MODELS) as u8;
        let c = mesh.health.counts(&region, ALL_MODELS);
        let _ = writeln!(out, "geoclass_region_state{{node=\"{node}\",region=\"{r}\",domain=\"{d}\"}} {s}");
        let _ = writeln!(out, "geoclass_region_available{{node=\"{node}\",region=\"{r}\"}} {}", u8::from(state.available(&mesh, &region, now)));
        let _ = writeln!(out, "geoclass_region_weight{{node=\"{node}\",region=\"{r}\"}} {:.6}", state.weight(&mesh, &region, now));
        for (kind, value) in [("attempts", c.attempts), ("overloaded", c.overloaded), ("errors", c.errors)] {
            let _ = writeln!(out, "geoclass_region_window{{node=\"{node}\",region=\"{r}\",kind=\"{kind}\"}} {value}");
        }
    }
    out
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ([(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], render(&state, crate::now_ms()))
}
