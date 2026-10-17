//! Egress: sending an attempt to the upstream from this node, or relaying it through a peer.
//!
//! The relay is transparent. Request headers (including the account token underclass injected)
//! go upstream unchanged, minus hop-by-hop and GeoClass headers; the upstream response comes back
//! unchanged. Nothing here stores or logs a header value.
use crate::state::{self, AppState};
use crate::store::RelayRecord;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;

pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

const HOP_BY_HOP: [&str; 10] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

/// @cc [owner:ghuntley,label:security] forwarded-headers
/// `forwardable` MUST drop hop-by-hop headers, `host`, `content-length`, `accept-encoding` (so
/// the gate always reads an identity-encoded body), and every `x-geoclass-*` header (so the mesh
/// key never leaves the mesh), and MUST keep every other request header unchanged.
pub fn forwardable(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let n = name.as_str();
        if HOP_BY_HOP.contains(&n) || n == "accept-encoding" || n.starts_with("x-geoclass-") {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

/// Response headers safe to pass back to the client.
pub fn response_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let n = name.as_str();
        if HOP_BY_HOP.contains(&n) || (n.starts_with("x-geoclass-") && n != state::COLO_HEADER && n != state::NODE_HEADER) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

/// The Cloudflare colo from `cf-ray` (`<id>-<COLO>`), sanitized.
pub fn cf_colo(headers: &HeaderMap) -> Option<String> {
    let ray = headers.get("cf-ray")?.to_str().ok()?;
    let colo = ray.rsplit_once('-')?.1;
    (!colo.is_empty() && colo.len() <= 8 && colo.chars().all(|c| c.is_ascii_alphanumeric())).then(|| colo.to_ascii_uppercase())
}

fn upstream_url(state: &AppState, path_and_query: &str) -> String {
    format!("{}{}", state.cfg.upstream, path_and_query)
}

/// Sends the attempt straight to the upstream from this node.
pub async fn egress_local(
    state: &AppState,
    method: &http::Method,
    path_and_query: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<reqwest::Response, String> {
    state
        .client
        .request(method.clone(), upstream_url(state, path_and_query))
        .headers(forwardable(headers))
        .header(http::header::ACCEPT_ENCODING, "identity")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("upstream: {}", without_url(&e)))
}

/// Sends the attempt to a peer's `/relay`, which egresses from the peer's region.
#[allow(clippy::too_many_arguments)]
pub async fn egress_via_peer(
    state: &AppState,
    peer_url: &str,
    method: &http::Method,
    path_and_query: &str,
    headers: &HeaderMap,
    body: Bytes,
    request_id: &str,
) -> Result<reqwest::Response, String> {
    let mut out = forwardable(headers);
    if let Some(key) = &state.cfg.mesh_key
        && let Ok(v) = HeaderValue::from_str(key) {
            out.insert(state::MESH_KEY_HEADER, v);
        }
    for (name, value) in [
        (state::FROM_NODE_HEADER, state.cfg.node_id.as_str()),
        (state::FROM_REGION_HEADER, state.cfg.region.as_str()),
        ("x-request-id", request_id),
    ] {
        if let Ok(v) = HeaderValue::from_str(value) {
            out.insert(HeaderName::from_static(name), v);
        }
    }
    state
        .mesh_client
        .request(method.clone(), format!("{}/relay{}", peer_url.trim_end_matches('/'), path_and_query))
        .headers(out)
        .body(body)
        .send()
        .await
        .map_err(|e| format!("peer: {}", without_url(&e)))
}

/// Error text without the URL (a peer URL is harmless, but keep messages uniform and short).
fn without_url(e: &reqwest::Error) -> String {
    let kind = if e.is_connect() {
        "connect"
    } else if e.is_timeout() {
        "timeout"
    } else if e.is_request() {
        "request"
    } else {
        "transport"
    };
    kind.to_string()
}

struct RelayFinish {
    state: Arc<AppState>,
    record: RelayRecord,
    started: Instant,
}

impl Drop for RelayFinish {
    fn drop(&mut self) {
        self.record.duration_ms = self.started.elapsed().as_millis() as i64;
        crate::logging::log_relay(&self.record);
        if let Err(error) = self.state.store.insert_relay(&self.record) {
            tracing::error!(error = %error, "relay.persist_failed");
        }
    }
}

fn header_str(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.chars().filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')).take(64).collect())
        .unwrap_or_else(|| "unknown".to_string())
}

/// @cc [owner:ghuntley,label:proxy] relay-transparent
/// `/relay/{path}` MUST forward the request to `upstream + path` with `forwardable` headers and
/// the original body, MUST return the upstream status, headers, and body bytes unchanged (adding
/// only `x-geoclass-node`, `x-geoclass-cf-colo`, and `x-geoclass-headers-ms`), MUST answer 502
/// when the upstream cannot be reached, and MUST record one relay row per request.
pub async fn relay_handler(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let started = Instant::now();
    let method = req.method().clone();
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().strip_prefix("/relay").unwrap_or(p.as_str()).to_string())
        .unwrap_or_default();
    let path_and_query = if path_and_query.starts_with('/') { path_and_query } else { format!("/{path_and_query}") };
    let headers = req.headers().clone();
    let mut finish = RelayFinish {
        record: RelayRecord {
            ts: crate::now_ms(),
            request_id: header_str(&headers, "x-request-id"),
            from_node: header_str(&headers, state::FROM_NODE_HEADER),
            from_region: header_str(&headers, state::FROM_REGION_HEADER),
            ..Default::default()
        },
        state: state.clone(),
        started,
    };
    let body = match axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            finish.record.error = Some("body_too_large".into());
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        }
    };
    let response = match egress_local(&state, &method, &path_and_query, &headers, body).await {
        Ok(r) => r,
        Err(e) => {
            finish.record.error = Some(e);
            return (StatusCode::BAD_GATEWAY, axum::Json(serde_json::json!({"error": {"type": "geoclass_relay_error", "message": "relay could not reach the upstream"}}))).into_response();
        }
    };
    let headers_ms = started.elapsed().as_millis() as i64;
    let status = response.status();
    finish.record.status = status.as_u16();
    finish.record.headers_ms = Some(headers_ms);
    finish.record.cf_colo = cf_colo(response.headers());
    state.note_egress_colo(finish.record.cf_colo.as_deref());
    let mut out_headers = response_headers(response.headers());
    if let Ok(v) = HeaderValue::from_str(&state.cfg.node_id) {
        out_headers.insert(state::NODE_HEADER, v);
    }
    if let Some(colo) = &finish.record.cf_colo
        && let Ok(v) = HeaderValue::from_str(colo) {
            out_headers.insert(state::COLO_HEADER, v);
        }
    out_headers.insert(state::HEADERS_MS_HEADER, HeaderValue::from(headers_ms));
    let stream = response.bytes_stream();
    let body = Body::from_stream(futures::stream::unfold((stream, finish), |(mut stream, mut finish)| async move {
        match stream.next().await {
            Some(Ok(chunk)) => {
                finish.record.bytes += chunk.len() as i64;
                Some((Ok::<_, std::io::Error>(chunk), (stream, finish)))
            }
            Some(Err(_)) => {
                finish.record.error = Some("upstream_stream".into());
                Some((Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "upstream stream error")), (stream, finish)))
            }
            None => None,
        }
    }));
    let mut resp = Response::builder().status(status).body(body).expect("relay response");
    *resp.headers_mut() = out_headers;
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwarding_strips_hop_by_hop_and_mesh_headers_but_keeps_auth() {
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer account-token".parse().unwrap());
        h.insert("chatgpt-account-id", "acct".parse().unwrap());
        h.insert("connection", "keep-alive".parse().unwrap());
        h.insert("host", "127.0.0.1:9090".parse().unwrap());
        h.insert("accept-encoding", "gzip".parse().unwrap());
        h.insert("x-geoclass-mesh-key", "secret".parse().unwrap());
        let out = forwardable(&h);
        assert_eq!(out.get("authorization").unwrap(), "Bearer account-token");
        assert!(out.get("chatgpt-account-id").is_some());
        for gone in ["connection", "host", "accept-encoding", "x-geoclass-mesh-key"] {
            assert!(out.get(gone).is_none(), "{gone} must not be forwarded");
        }
    }

    #[test]
    fn colo_from_cf_ray() {
        let mut h = HeaderMap::new();
        h.insert("cf-ray", "8a1b2c3d4e5f6789-syd".parse().unwrap());
        assert_eq!(cf_colo(&h).as_deref(), Some("SYD"));
        h.insert("cf-ray", "nonsense".parse().unwrap());
        assert_eq!(cf_colo(&h), None);
    }
}
