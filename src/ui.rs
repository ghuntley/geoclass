//! Admin web UI and token-gated admin API.
use crate::state::AppState;
use crate::stats_api::StatsParams;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;

const UI_HTML: &str = include_str!("ui.html");

pub async fn index() -> impl IntoResponse {
    ([(http::header::CONTENT_TYPE, "text/html; charset=utf-8")], UI_HTML)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// @cc [owner:ghuntley,label:security] admin-token-gate
/// Every `/admin/api/*` route MUST be rejected with 401 unless the `Authorization` header carries
/// exactly `Bearer <ui_token>`.
pub async fn require_ui_token(State(state): State<Arc<AppState>>, headers: HeaderMap, req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let provided = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !constant_time_eq(provided.as_bytes(), state.ui_token.as_bytes()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    next.run(req).await
}

/// @cc [owner:ghuntley,label:security] stats-admin-only
/// Statistics MUST be served only under the admin token gate (or the mesh gate for peers), and
/// responses MUST NOT include prompt cache keys, request bodies, or credentials.
pub async fn stats(State(state): State<Arc<AppState>>, Query(params): Query<StatsParams>) -> Response {
    match params.scope.as_deref() {
        None | Some("local") => crate::mesh::local_stats(&state, params).await,
        Some("cluster") => crate::mesh::cluster_stats(&state, params).await,
        Some(_) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"message": "scope must be local or cluster"}))).into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct HealthParams {
    pub since: Option<String>,
    pub model: Option<String>,
}

/// Per-minute region health samples recorded by this node.
pub async fn health(State(state): State<Arc<AppState>>, Query(params): Query<HealthParams>) -> Response {
    let now = crate::now_ms();
    let Some(span) = crate::stats::parse_since(params.since.as_deref().unwrap_or("6h")).filter(|s| *s <= 31 * crate::stats::DAY_MS) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"message": "since must look like 6h or 7d (max 31d)"}))).into_response();
    };
    let model = params.model.unwrap_or_else(|| crate::health::ALL_MODELS.to_string());
    let store = state.store.clone();
    match tokio::task::spawn_blocking(move || store.health_samples(now - span, now + 1, &model)).await {
        Ok(Ok(samples)) => Json(samples).into_response(),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "health query failed").into_response(),
    }
}

/// The session → region binding counts and the learned hour-of-week failure rates.
pub async fn routing(State(state): State<Arc<AppState>>) -> Response {
    let now = crate::now_ms();
    let mesh = state.mesh.lock().unwrap();
    let regions = state.region_names(&mesh, now);
    let learned: serde_json::Map<String, serde_json::Value> = regions
        .iter()
        .map(|r| (r.clone(), serde_json::json!(mesh.learned.table(r))))
        .collect();
    let configured: serde_json::Map<String, serde_json::Value> = regions
        .iter()
        .map(|r| {
            let cfg = state.cfg.regions.iter().find(|c| &c.name == r).cloned().unwrap_or_default();
            let day_start = now - now.rem_euclid(crate::stats::DAY_MS);
            let hours: Vec<f64> = (0..24).map(|h| crate::prior::configured_weight(&cfg, day_start + h * crate::stats::HOUR_MS)).collect();
            (r.clone(), serde_json::json!(hours))
        })
        .collect();
    Json(serde_json::json!({
        "bindings": mesh.bindings.as_ref().map(|b| b.counts_by_region(now)).unwrap_or_default(),
        "domains": mesh.domains,
        "configured_prior_today_utc": configured,
        "learned_failure_rate_by_hour_of_week": learned,
    }))
    .into_response()
}
