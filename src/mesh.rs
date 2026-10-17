//! The mesh: node info, push-pull gossip, binding sync, and the peer-facing stats endpoint.
//!
//! Every mesh route sits behind `require_mesh`: the shared mesh key, plus (with Tailscale
//! discovery) a LocalAPI `whois` check that the caller carries the GeoClass tag.
use crate::state::{AppState, Digest, MESH_KEY_HEADER, NodeInfo};
use crate::stats_api::{self, StatsParams};
use axum::extract::{ConnectInfo, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// @cc [owner:ghuntley,label:security] mesh-auth
/// Every `/mesh/*` and `/relay/*` request MUST be rejected with 401 unless a mesh key is
/// configured and the request's `x-geoclass-mesh-key` equals it; with Tailscale discovery it
/// MUST additionally be rejected with 403 unless LocalAPI `whois` reports the caller's tailnet
/// node carries the configured tag. Rejections MUST NOT echo the presented key.
pub async fn require_mesh(State(state): State<Arc<AppState>>, ConnectInfo(addr): ConnectInfo<SocketAddr>, req: Request, next: Next) -> Response {
    let Some(expected) = state.cfg.mesh_key.as_deref().filter(|k| !k.is_empty()) else {
        return (StatusCode::UNAUTHORIZED, "mesh disabled: no mesh key configured").into_response();
    };
    let presented = req.headers().get(MESH_KEY_HEADER).map(|v| v.as_bytes()).unwrap_or_default();
    if !constant_time_eq(presented, expected.as_bytes()) {
        return (StatusCode::UNAUTHORIZED, "invalid mesh key").into_response();
    }
    if let Some(api) = &state.tailscale {
        match api.whois_tags(&addr.to_string()).await {
            Ok(tags) if tags.iter().any(|t| *t == state.cfg.tailscale_tag) => {}
            Ok(_) => return (StatusCode::FORBIDDEN, "caller is not a tagged geoclass node").into_response(),
            Err(error) => {
                tracing::warn!(error = %error, "mesh.whois_failed");
                return (StatusCode::FORBIDDEN, "caller identity unavailable").into_response();
            }
        }
    }
    next.run(req).await
}

pub async fn info(State(state): State<Arc<AppState>>) -> Json<NodeInfo> {
    let mesh = state.mesh.lock().unwrap();
    Json(state.node_info(&mesh))
}

#[derive(Debug, Deserialize)]
pub struct GossipQuery {
    /// The sender's mesh URL, so a peer that has not discovered it yet can still reach it.
    pub from: Option<String>,
}

/// Push-pull: apply the sender's digest, answer with ours.
pub async fn gossip(State(state): State<Arc<AppState>>, Query(q): Query<GossipQuery>, Json(digest): Json<Digest>) -> Json<Digest> {
    let now = crate::now_ms();
    let from = q.from.filter(|u| valid_peer_url(u));
    if let Some(event) = state.apply_digest(from.as_deref(), digest, now) {
        state.persist(vec![event], vec![]);
    }
    Json(state.digest(now))
}

pub async fn bindings(State(state): State<Arc<AppState>>) -> Json<Vec<crate::bindings::BindingRecord>> {
    let now = crate::now_ms();
    let mesh = state.mesh.lock().unwrap();
    Json(mesh.bindings.as_ref().map(|b| b.snapshot(now)).unwrap_or_default())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PeerOut {
    pub url: String,
    pub source: String,
    pub host: String,
    pub node_id: Option<String>,
    pub region: Option<String>,
    pub version: Option<String>,
    pub egress_ok: Option<bool>,
    pub live: bool,
    pub gossip_age_ms: Option<i64>,
    pub inflight: i64,
}

pub fn peers_out(state: &AppState, now: i64) -> Vec<PeerOut> {
    let mesh = state.mesh.lock().unwrap();
    mesh.peers
        .values()
        .map(|p| PeerOut {
            url: p.url.clone(),
            source: p.source.clone(),
            host: p.host.clone(),
            node_id: p.info.as_ref().map(|i| i.node_id.clone()),
            region: p.info.as_ref().map(|i| i.region.clone()),
            version: p.info.as_ref().map(|i| i.version.clone()),
            egress_ok: p.info.as_ref().map(|i| i.egress_ok),
            live: state.is_live(p, now),
            gossip_age_ms: p.last_digest_ms.map(|t| now - t),
            inflight: p.inflight,
        })
        .collect()
}

pub async fn peers(State(state): State<Arc<AppState>>) -> Json<Vec<PeerOut>> {
    Json(peers_out(&state, crate::now_ms()))
}

/// This node's local statistics, for a peer assembling a cluster view.
pub async fn stats(State(state): State<Arc<AppState>>, Query(mut params): Query<StatsParams>) -> Response {
    params.scope = Some("local".into());
    local_stats(&state, params).await
}

pub async fn local_stats(state: &Arc<AppState>, params: StatsParams) -> Response {
    let inflight = state.inflight.load(std::sync::atomic::Ordering::Relaxed);
    let retention = state.cfg.stats_retention_days;
    let store = state.store.clone();
    let node = crate::stats_api::NodeStatus { node: state.cfg.node_id.clone(), region: state.cfg.region.clone(), ok: true, error: None };
    let result = tokio::task::spawn_blocking(move || stats_api::query(&store, &params, crate::now_ms(), inflight, retention)).await;
    match result {
        Ok(Ok(mut response)) => {
            response.nodes = vec![node];
            Json(response).into_response()
        }
        Ok(Err(message)) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({"message": message}))).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "stats.query_failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "stats query failed").into_response()
        }
    }
}

/// Mesh URLs must be plain `http(s)://host:port` origins.
pub fn valid_peer_url(url: &str) -> bool {
    reqwest::Url::parse(url).is_ok_and(|u| {
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && matches!(u.path(), "" | "/")
            && u.query().is_none()
    })
}

/// Fetches `/mesh/info` from a candidate peer.
pub async fn fetch_info(state: &AppState, url: &str) -> Result<NodeInfo, String> {
    let response = state
        .mesh_client
        .get(format!("{url}/mesh/info"))
        .header(MESH_KEY_HEADER, state.cfg.mesh_key.clone().unwrap_or_default())
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .map_err(|_| "unreachable".to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status().as_u16()));
    }
    response.json().await.map_err(|_| "invalid info".to_string())
}

pub async fn pull_bindings(state: &AppState, url: &str) -> Result<usize, String> {
    let response = state
        .mesh_client
        .get(format!("{url}/mesh/bindings"))
        .header(MESH_KEY_HEADER, state.cfg.mesh_key.clone().unwrap_or_default())
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| "unreachable".to_string())?;
    let records: Vec<crate::bindings::BindingRecord> = response.json().await.map_err(|_| "invalid bindings".to_string())?;
    let now = crate::now_ms();
    let mut mesh = state.mesh.lock().unwrap();
    let Some(bindings) = mesh.bindings.as_mut() else { return Ok(0) };
    Ok(records.into_iter().filter(|r| bindings.merge(r.clone(), now)).count())
}

/// Gossips with every known peer each interval, then advances health.
pub async fn gossip_loop(state: Arc<AppState>) {
    let interval = std::time::Duration::from_millis(state.cfg.gossip_interval_ms as u64);
    loop {
        let now = crate::now_ms();
        let self_url = state.self_url.lock().unwrap().clone();
        let digest = state.digest(now);
        let targets: Vec<(String, bool)> = {
            let mesh = state.mesh.lock().unwrap();
            mesh.peers.values().filter(|p| p.info.is_some()).map(|p| (p.url.clone(), p.synced)).collect()
        };
        let sends = targets.into_iter().map(|(url, synced)| {
            let state = state.clone();
            let digest = digest.clone();
            let self_url = self_url.clone();
            async move {
                let mut request = state
                    .mesh_client
                    .post(format!("{url}/mesh/gossip"))
                    .header(MESH_KEY_HEADER, state.cfg.mesh_key.clone().unwrap_or_default())
                    .timeout(interval.max(std::time::Duration::from_secs(1)))
                    .json(&digest);
                if let Some(self_url) = &self_url {
                    request = request.query(&[("from", self_url.as_str())]);
                }
                match request.send().await {
                    Ok(response) if response.status().is_success() => {
                        if let Ok(reply) = response.json::<Digest>().await {
                            if let Some(event) = state.apply_digest(Some(&url), reply, crate::now_ms()) {
                                state.persist(vec![event], vec![]);
                            }
                            if !synced {
                                match pull_bindings(&state, &url).await {
                                    Ok(n) => {
                                        if let Some(peer) = state.mesh.lock().unwrap().peers.get_mut(&url) {
                                            peer.synced = true;
                                        }
                                        tracing::info!(peer = %url, merged = n, "mesh.bindings_synced");
                                    }
                                    Err(error) => tracing::debug!(peer = %url, error = %error, "mesh.bindings_sync_failed"),
                                }
                            }
                        }
                    }
                    Ok(response) => tracing::debug!(peer = %url, status = response.status().as_u16(), "mesh.gossip_rejected"),
                    Err(_) => tracing::debug!(peer = %url, "mesh.gossip_unreachable"),
                }
            }
        });
        futures::future::join_all(sends).await;
        let (events, samples) = state.tick(crate::now_ms());
        state.persist(events, samples);
        tokio::time::sleep(interval).await;
    }
}

/// Queries every live peer's local stats and merges them with this node's.
pub async fn cluster_stats(state: &Arc<AppState>, params: StatsParams) -> Response {
    let now = crate::now_ms();
    let mut local_params = params.clone();
    local_params.scope = Some("local".into());
    let inflight = state.inflight.load(std::sync::atomic::Ordering::Relaxed);
    let retention = state.cfg.stats_retention_days;
    let store = state.store.clone();
    let lp = local_params.clone();
    let local = match tokio::task::spawn_blocking(move || stats_api::query(&store, &lp, now, inflight, retention)).await {
        Ok(Ok(r)) => r,
        Ok(Err(message)) => return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"message": message}))).into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "stats query failed").into_response(),
    };
    // Pin every node to the same window so buckets line up.
    local_params.from_ms = Some(local.from_ms);
    local_params.to_ms = Some(local.to_ms);
    local_params.since = None;
    let peers: Vec<(String, String, String)> = {
        let mesh = state.mesh.lock().unwrap();
        mesh.peers
            .values()
            .filter(|p| state.is_live(p, now))
            .filter_map(|p| p.info.as_ref().map(|i| (p.url.clone(), i.node_id.clone(), i.region.clone())))
            .collect()
    };
    let mut nodes = vec![stats_api::NodeStatus { node: state.cfg.node_id.clone(), region: state.cfg.region.clone(), ok: true, error: None }];
    let mut parts = vec![local];
    let fetches = peers.into_iter().map(|(url, node, region)| {
        let state = state.clone();
        let params = local_params.clone();
        async move {
            let result = async {
                let response = state
                    .mesh_client
                    .get(format!("{url}/mesh/stats"))
                    .header(MESH_KEY_HEADER, state.cfg.mesh_key.clone().unwrap_or_default())
                    .query(&params)
                    .timeout(std::time::Duration::from_secs(60))
                    .send()
                    .await
                    .map_err(|_| "unreachable".to_string())?;
                if !response.status().is_success() {
                    return Err(format!("HTTP {}", response.status().as_u16()));
                }
                response.json::<stats_api::StatsResponse>().await.map_err(|_| "invalid stats".to_string())
            }
            .await;
            (node, region, result)
        }
    });
    for (node, region, result) in futures::future::join_all(fetches).await {
        match result {
            Ok(part) => {
                parts.push(part);
                nodes.push(stats_api::NodeStatus { node, region, ok: true, error: None });
            }
            Err(error) => nodes.push(stats_api::NodeStatus { node, region, ok: false, error: Some(error) }),
        }
    }
    match stats_api::merge_cluster(parts, nodes) {
        Some(merged) => Json(merged).into_response(),
        None => (StatusCode::INTERNAL_SERVER_ERROR, "no stats").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_urls_are_plain_origins() {
        assert!(valid_peer_url("http://100.64.0.2:9091"));
        assert!(!valid_peer_url("http://user:pw@100.64.0.2:9091"));
        assert!(!valid_peer_url("http://100.64.0.2:9091/x"));
        assert!(!valid_peer_url("file:///etc/passwd"));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
