//! Ingress: the endpoint underclass points `UNDERCLASS_CODEX_UPSTREAM` at.
//!
//! For each request: pick a region (placement), send the attempt from that region (locally or
//! through a peer's relay), and hold the response in the gate until it proves itself. A capacity
//! failure before output becomes a retry in another region; anything else goes to the client.
use crate::affinity;
use crate::gate::{CommitReason, Gate, RetryKind, Verdict};
use crate::health::Observation;
use crate::placement::{self, Reason, Request as PlacementRequest};
use crate::relay;
use crate::sse::{self, StreamEnd};
use crate::state::{self, AppState, InflightGuard, Target};
use crate::store::AttemptRecord;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;
use tower_http::request_id::RequestId;

const MAX_ERROR_BODY: usize = 1024 * 1024;

struct Failure {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

fn json_error(status: StatusCode, message: &str, kind: &str) -> Response {
    (status, axum::Json(serde_json::json!({"error": {"message": message, "type": kind}}))).into_response()
}

fn error_code_of(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    value.get("error").and_then(sse::error_code).or_else(|| sse::error_code(&value))
}

fn persist_attempt(state: &AppState, record: &AttemptRecord) {
    crate::logging::log_attempt(record);
    if let Err(error) = state.store.insert_attempt(record) {
        tracing::error!(error = %error, request_id = %record.request_id, "attempt.persist_failed");
    }
}

/// Writes the attempt row when a committed body ends for any reason, including client drop.
struct AttemptFinish {
    state: Arc<AppState>,
    record: AttemptRecord,
    gate: Gate,
    ended: Option<StreamEnd>,
    started: Instant,
    _guard: InflightGuard,
}

fn fill_from_gate(record: &mut AttemptRecord, gate: &Gate, end: StreamEnd) {
    if let Some(counts) = gate.counts() {
        record.input_tokens = Some(counts.input_tokens);
        record.cached_input_tokens = Some(counts.cached_input_tokens);
        record.output_tokens = Some(counts.output_tokens);
    }
    let report = gate.tap().report(end);
    record.stream_outcome = Some(report.outcome.as_str().to_string());
    record.error_code = report.error_code;
    record.saw_output = Some(report.saw_output);
}

impl Drop for AttemptFinish {
    fn drop(&mut self) {
        fill_from_gate(&mut self.record, &self.gate, self.ended.unwrap_or(StreamEnd::Dropped));
        self.record.duration_ms = Some(self.started.elapsed().as_millis() as i64);
        persist_attempt(&self.state, &self.record);
    }
}

fn with_geoclass_headers(mut headers: HeaderMap, region: &str, attempts: usize, reason: Reason) -> HeaderMap {
    if let Ok(v) = HeaderValue::from_str(region) {
        headers.insert(state::REGION_HEADER, v);
    }
    headers.insert(state::ATTEMPTS_HEADER, HeaderValue::from(attempts as u64));
    headers.insert("x-geoclass-reason", HeaderValue::from_static(reason.as_str()));
    headers
}

/// @cc [owner:ghuntley,label:proxy] ingress-retry-loop
/// `ingress` MUST try at most `retry_budget` regions (every region when 0) and stop starting new
/// attempts after `retry_deadline_ms`. It MUST retry in another region only when an attempt fails
/// before any output with a capacity code, a 5xx, or a transport error; every other upstream
/// response (including 429 quota errors) MUST reach the client unchanged. Once the gate commits,
/// the response MUST be streamed to the client without further retries. When every attempt
/// failed, the last upstream failure MUST be returned as received, or a 503 when no upstream
/// response was ever received. A committed attempt MUST bind the request's session to the
/// attempt's region.
pub async fn ingress(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .and_then(|r| r.header_value().to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    let method = req.method().clone();
    let path_and_query = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();
    let body = match axum::body::to_bytes(req.into_body(), relay::MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return json_error(StatusCode::PAYLOAD_TOO_LARGE, "request body too large", "invalid_request_error"),
    };
    let parsed: Option<Value> = serde_json::from_slice(&body).ok();
    let model = parsed.as_ref().and_then(affinity::model).unwrap_or("(none)").to_string();
    let session = parsed.as_ref().and_then(affinity::session_key).map(crate::sha256_hex);
    let account = affinity::account_hash(&headers);
    let wants_stream = parsed.as_ref().and_then(|v| v.get("stream")).and_then(Value::as_bool) == Some(true);
    let chat = path.ends_with("/chat/completions");

    let started_all = Instant::now();
    let budget = if state.cfg.retry_budget == 0 { usize::MAX } else { state.cfg.retry_budget };
    let mut tried: Vec<String> = Vec::new();
    let mut last_failure: Option<Failure> = None;
    let mut pending: Option<AttemptRecord> = None;
    let mut overload_seen = false;
    let mut last_reason = Reason::Unkeyed;

    for attempt_no in 0.. {
        if attempt_no >= budget {
            break;
        }
        if attempt_no > 0 && started_all.elapsed().as_millis() as i64 > state.cfg.retry_deadline_ms {
            if let Some(mut p) = pending.take() {
                p.gate_result = "abandoned".into();
                persist_attempt(&state, &p);
            }
            let e = state.event("retry_deadline", |e| {
                e.request_id = Some(request_id.clone());
                e.model = Some(model.clone());
            });
            state.persist(vec![e], vec![]);
            break;
        }
        let now = crate::now_ms();
        let (choice, target, domain) = {
            let mut mesh = state.mesh.lock().unwrap();
            let binding = session
                .as_deref()
                .and_then(|s| mesh.bindings.as_mut().and_then(|b| b.get(s, now)))
                .map(|b| b.region);
            let views = state.views(&mesh, &model, now);
            let choice = placement::choose(
                &views,
                &PlacementRequest { session: session.as_deref(), spread: &request_id, binding: binding.as_deref(), tried: &tried },
            );
            match choice {
                None => (None, None, String::new()),
                Some(choice) => {
                    let target = state.target(&mesh, &choice.region, &request_id, now);
                    let domain = state.domain(&mesh, &choice.region);
                    (Some(choice), target, domain)
                }
            }
        };
        let Some(choice) = choice else { break };
        if let Some(p) = pending.take() {
            persist_attempt(&state, &p);
        }
        last_reason = choice.reason;
        let Some(target) = target else {
            tried.push(choice.region);
            continue;
        };
        let guard = InflightGuard::new(state.clone(), &choice.region);
        let started = Instant::now();
        let mut record = AttemptRecord {
            request_id: request_id.clone(),
            attempt_no: attempt_no as i64,
            ts: now,
            endpoint: path.clone(),
            model: model.clone(),
            session: session.clone(),
            account: account.clone(),
            ingress_region: state.cfg.region.clone(),
            egress_region: choice.region.clone(),
            egress_node: match &target {
                Target::Local => state.cfg.node_id.clone(),
                Target::Peer { node_id, .. } => node_id.clone(),
            },
            failure_domain: domain,
            placement_reason: choice.reason.as_str().to_string(),
            ..Default::default()
        };
        let sent = match &target {
            Target::Local => relay::egress_local(&state, &method, &path_and_query, &headers, body.clone()).await,
            Target::Peer { url, .. } => relay::egress_via_peer(&state, url, &method, &path_and_query, &headers, body.clone(), &request_id).await,
        };
        let response = match sent {
            Ok(r) => r,
            Err(error) => {
                tracing::warn!(request_id = %request_id, egress = %choice.region, error = %error, "attempt.transport_error");
                state.observe(&choice.region, &model, Observation::Error, crate::now_ms());
                record.status = 0;
                record.gate_result = "retried".into();
                record.error_code = Some("transport".into());
                record.duration_ms = Some(started.elapsed().as_millis() as i64);
                pending = Some(record);
                tried.push(choice.region);
                continue;
            }
        };
        let status = response.status();
        record.status = status.as_u16();
        match &target {
            Target::Local => {
                record.cf_colo = relay::cf_colo(response.headers());
                state.note_egress_colo(record.cf_colo.as_deref());
            }
            Target::Peer { .. } => {
                record.cf_colo = response.headers().get(state::COLO_HEADER).and_then(|v| v.to_str().ok()).map(str::to_string);
                record.hop_ms = response
                    .headers()
                    .get(state::HEADERS_MS_HEADER)
                    .and_then(|v| v.to_str().ok()?.parse::<i64>().ok())
                    .map(|peer_ms| (started.elapsed().as_millis() as i64 - peer_ms).max(0));
            }
        }
        let resp_headers = relay::response_headers(response.headers());

        if !status.is_success() {
            let body = read_bounded(response).await;
            let code = error_code_of(&body);
            record.error_code = code.clone();
            record.duration_ms = Some(started.elapsed().as_millis() as i64);
            let capacity = sse::is_retryable_code(code.as_deref());
            if capacity || status.is_server_error() {
                let observation = if capacity { Observation::Overloaded } else { Observation::Error };
                state.observe(&choice.region, &model, observation, crate::now_ms());
                if capacity && !overload_seen {
                    overload_seen = true;
                    record.overload_hit = true;
                }
                record.gate_result = "retried".into();
                pending = Some(record);
                last_failure = Some(Failure { status, headers: resp_headers, body });
                tried.push(choice.region);
                continue;
            }
            state.observe(&choice.region, &model, Observation::Committed { ttft_ms: None }, crate::now_ms());
            record.gate_result = "passthrough".into();
            persist_attempt(&state, &record);
            drop(guard);
            let mut resp = Response::builder().status(status).body(Body::from(body)).expect("passthrough");
            *resp.headers_mut() = with_geoclass_headers(resp_headers, &choice.region, attempt_no + 1, choice.reason);
            return resp;
        }

        let content_type = resp_headers.get(http::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        let stream_body = wants_stream || content_type.starts_with("text/event-stream");
        let mut gate = Gate::new(stream_body, chat, state.cfg.gate_max_bytes);
        let headers_at = Instant::now();
        let mut upstream = response.bytes_stream();
        let mut ended: Option<StreamEnd> = None;
        let verdict = loop {
            match upstream.next().await {
                Some(Ok(chunk)) => match gate.feed(&chunk) {
                    Verdict::Hold => continue,
                    v => break v,
                },
                Some(Err(_)) => {
                    ended = Some(StreamEnd::UpstreamError);
                    break gate.finish(StreamEnd::UpstreamError);
                }
                None => {
                    ended = Some(StreamEnd::Eof);
                    break gate.finish(StreamEnd::Eof);
                }
            }
        };
        match verdict {
            Verdict::Retry { kind, code } => {
                let held = gate.take_held();
                fill_from_gate(&mut record, &gate, ended.unwrap_or(StreamEnd::Dropped));
                record.error_code = code.or(record.error_code.take());
                record.held_ms = Some(headers_at.elapsed().as_millis() as i64);
                record.duration_ms = Some(started.elapsed().as_millis() as i64);
                record.gate_result = "retried".into();
                let observation = match kind {
                    RetryKind::Capacity => Observation::Overloaded,
                    RetryKind::Transport => Observation::Error,
                };
                state.observe(&choice.region, &model, observation, crate::now_ms());
                if kind == RetryKind::Capacity && !overload_seen {
                    overload_seen = true;
                    record.overload_hit = true;
                }
                pending = Some(record);
                last_failure = Some(Failure { status, headers: resp_headers, body: Bytes::from(held) });
                tried.push(choice.region);
                continue;
            }
            Verdict::Commit(reason) => {
                let held = gate.take_held();
                let held_ms = headers_at.elapsed().as_millis() as i64;
                let ttft = (reason == CommitReason::Output).then(|| started.elapsed().as_millis() as i64);
                record.gate_result = reason.as_str().to_string();
                record.held_ms = Some(held_ms);
                record.ttft_ms = ttft;
                record.rescued = overload_seen;
                state.observe(&choice.region, &model, Observation::Committed { ttft_ms: ttft }, crate::now_ms());
                bind_session(&state, session.as_deref(), &choice.region, choice.reason, &request_id, &model);
                if overload_seen {
                    let from = tried.first().cloned();
                    let e = state.event("geo_retry_succeeded", |e| {
                        e.request_id = Some(request_id.clone());
                        e.model = Some(model.clone());
                        e.from_region = from;
                        e.to_region = Some(choice.region.clone());
                        e.region = Some(choice.region.clone());
                    });
                    state.persist(vec![e], vec![]);
                }
                let finish = AttemptFinish { state: state.clone(), record, gate, ended, started, _guard: guard };
                let first = futures::stream::once(async move { Ok::<_, std::io::Error>(Bytes::from(held)) });
                let rest = futures::stream::unfold((upstream, finish), |(mut upstream, mut finish)| async move {
                    if finish.ended.is_some() {
                        return None;
                    }
                    match upstream.next().await {
                        Some(Ok(chunk)) => {
                            finish.gate.feed(&chunk);
                            Some((Ok(chunk), (upstream, finish)))
                        }
                        Some(Err(_)) => {
                            finish.ended = Some(StreamEnd::UpstreamError);
                            Some((Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "upstream stream error")), (upstream, finish)))
                        }
                        None => {
                            finish.ended = Some(StreamEnd::Eof);
                            drop(finish);
                            None
                        }
                    }
                });
                let mut resp = Response::builder().status(status).body(Body::from_stream(first.chain(rest))).expect("stream");
                *resp.headers_mut() = with_geoclass_headers(resp_headers, &choice.region, attempt_no + 1, choice.reason);
                return resp;
            }
            Verdict::Hold | Verdict::Pass => unreachable!("the gate loop only breaks on a decision"),
        }
    }

    let attempts = tried.len();
    if let Some(mut p) = pending.take() {
        p.gate_result = "exhausted".into();
        persist_attempt(&state, &p);
    }
    if overload_seen {
        let e = state.event("all_regions_overloaded", |e| {
            e.request_id = Some(request_id.clone());
            e.model = Some(model.clone());
            e.detail = Some(tried.join(","));
        });
        state.persist(vec![e], vec![]);
    }
    match last_failure {
        Some(f) => {
            let region = tried.last().cloned().unwrap_or_default();
            let mut resp = Response::builder().status(f.status).body(Body::from(f.body)).expect("failure");
            *resp.headers_mut() = with_geoclass_headers(f.headers, &region, attempts, last_reason);
            resp
        }
        None => json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "geoclass could not reach the upstream from any region",
            "geoclass_no_region",
        ),
    }
}

/// Binds the session to the region that served it, reporting a `session_moved` event when the
/// binding changed region.
fn bind_session(state: &AppState, session: Option<&str>, region: &str, reason: Reason, request_id: &str, model: &str) {
    let Some(session) = session else { return };
    let now = crate::now_ms();
    let outcome = {
        let mut mesh = state.mesh.lock().unwrap();
        mesh.bindings.as_mut().map(|b| b.bind(session, region, &state.cfg.node_id, now))
    };
    if let Some(crate::bindings::BindOutcome::Moved { from }) = outcome {
        let e = state.event("session_moved", |e| {
            e.request_id = Some(request_id.to_string());
            e.model = Some(model.to_string());
            e.from_region = Some(from);
            e.to_region = Some(region.to_string());
            e.region = Some(region.to_string());
            e.detail = Some(reason.as_str().to_string());
        });
        state.persist(vec![e], vec![]);
    }
}

async fn read_bounded(response: reqwest::Response) -> Bytes {
    let mut out = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(Ok(chunk)) = stream.next().await {
        if out.len() + chunk.len() > MAX_ERROR_BODY {
            out.extend_from_slice(&chunk[..MAX_ERROR_BODY - out.len()]);
            break;
        }
        out.extend_from_slice(&chunk);
    }
    Bytes::from(out)
}
