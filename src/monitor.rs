//! Live monitor snapshot for `geoclass top`, the web UI, and the local monitor socket.
use crate::health::ALL_MODELS;
use crate::mesh::PeerOut;
use crate::state::AppState;
use crate::store::Event;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MonitorRegion {
    pub name: String,
    pub domain: String,
    pub state: String,
    pub available: bool,
    pub local: bool,
    pub weight: f64,
    pub configured_weight: f64,
    pub attempts: i64,
    pub overloaded: i64,
    pub errors: i64,
    pub rate: f64,
    pub ttft_mean_ms: Option<i64>,
    pub bindings: i64,
    pub inflight: i64,
    /// Per-model states that differ from green.
    #[serde(default)]
    pub models: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MonitorAttempt {
    pub ts: i64,
    pub request_id: String,
    pub attempt_no: i64,
    pub model: String,
    pub session: String,
    pub egress_region: String,
    pub egress_node: String,
    pub reason: String,
    pub status: u16,
    pub gate: String,
    pub outcome: Option<String>,
    pub error_code: Option<String>,
    pub ttft_ms: Option<i64>,
    pub duration_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MonitorSnapshot {
    pub now_ms: i64,
    pub node: String,
    pub region: String,
    pub version: String,
    pub uptime_ms: i64,
    pub inflight: i64,
    pub regions: Vec<MonitorRegion>,
    pub peers: Vec<PeerOut>,
    pub bindings_total: i64,
    /// Per egress region, 60 `(attempts, retried)` minute buckets ending now.
    pub minute_bins: BTreeMap<String, Vec<(i64, i64)>>,
    pub recent: Vec<MonitorAttempt>,
    pub events: Vec<Event>,
}

/// @cc [owner:ghuntley,label:security] monitor-no-secrets
/// The snapshot MUST NOT contain prompt cache keys, account ids, headers, or bodies; sessions
/// appear only as the first 12 characters of their hash.
pub fn build(state: &AppState, now: i64) -> MonitorSnapshot {
    let peers = crate::mesh::peers_out(state, now);
    let inflight_by_region = state.inflight_by_region.lock().unwrap().clone();
    let (regions, bindings_total) = {
        let mesh = state.mesh.lock().unwrap();
        let counts = mesh.bindings.as_ref().map(|b| b.counts_by_region(now)).unwrap_or_default();
        let regions = state
            .region_names(&mesh, now)
            .into_iter()
            .map(|name| {
                let c = mesh.health.counts(&name, ALL_MODELS);
                let cfg = state.cfg.regions.iter().find(|r| r.name == name).cloned().unwrap_or_default();
                let models = mesh
                    .health
                    .states()
                    .iter()
                    .filter(|((r, m), s)| *r == name && m != ALL_MODELS && **s != crate::health::State::Green)
                    .map(|((_, m), s)| (m.clone(), s.as_str().to_string()))
                    .collect();
                MonitorRegion {
                    domain: state.domain(&mesh, &name),
                    state: mesh.health.state(&name, ALL_MODELS).as_str().to_string(),
                    available: state.available(&mesh, &name, now),
                    local: name == state.cfg.region,
                    weight: state.weight(&mesh, &name, now),
                    configured_weight: crate::prior::configured_weight(&cfg, now),
                    attempts: c.attempts,
                    overloaded: c.overloaded,
                    errors: c.errors,
                    rate: c.rate(),
                    ttft_mean_ms: (c.ttft_n > 0).then(|| c.ttft_sum_ms / c.ttft_n),
                    bindings: counts.get(&name).copied().unwrap_or(0),
                    inflight: inflight_by_region.get(&name).copied().unwrap_or(0),
                    models,
                    name,
                }
            })
            .collect();
        (regions, mesh.bindings.as_ref().map(|b| b.len() as i64).unwrap_or(0))
    };
    let recent = state
        .store
        .recent_attempts(40)
        .unwrap_or_default()
        .into_iter()
        .map(|r| MonitorAttempt {
            ts: r.ts,
            session: crate::logging::short(r.session.as_deref()).to_string(),
            request_id: r.request_id,
            attempt_no: r.attempt_no,
            model: r.model,
            egress_region: r.egress_region,
            egress_node: r.egress_node,
            reason: r.placement_reason,
            status: r.status,
            gate: r.gate_result,
            outcome: r.stream_outcome,
            error_code: r.error_code,
            ttft_ms: r.ttft_ms,
            duration_ms: r.duration_ms,
        })
        .collect();
    MonitorSnapshot {
        now_ms: now,
        node: state.cfg.node_id.clone(),
        region: state.cfg.region.clone(),
        version: crate::state::VERSION.to_string(),
        uptime_ms: now - state.started_ms,
        inflight: state.inflight.load(std::sync::atomic::Ordering::Relaxed),
        regions,
        peers,
        bindings_total,
        minute_bins: state.store.minute_bins(now).unwrap_or_default(),
        recent,
        events: state.store.recent_events(20).unwrap_or_default(),
    }
}

pub async fn snapshot(State(state): State<Arc<AppState>>) -> Json<MonitorSnapshot> {
    let now = crate::now_ms();
    let s = state.clone();
    Json(tokio::task::spawn_blocking(move || build(&s, now)).await.unwrap_or_default())
}
