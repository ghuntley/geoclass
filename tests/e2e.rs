//! Multi-node end-to-end tests: three in-process GeoClass nodes meshed with static discovery,
//! each egressing to its own mock upstream.
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use geoclass::config::Config;
use geoclass::server;
use geoclass::store::Store;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MESH_KEY: &str = "test-mesh-key";
const UI_TOKEN: &str = "test-ui-token";
const CANARY: &str = "Bearer canary-account-token";

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Ok,
    /// 200 stream that fails with `server_is_overloaded` before any output.
    Overloaded,
    /// 429 usage limit (account-scoped, must pass through).
    Quota,
    /// 503 before any body.
    Unavailable,
}

struct Mock {
    region: String,
    mode: Mode,
    requests: usize,
    authorizations: Vec<String>,
    mesh_keys_seen: usize,
}

async fn mock_handler(State(mock): State<Arc<Mutex<Mock>>>, headers: axum::http::HeaderMap, _body: String) -> Response {
    let (mode, region) = {
        let mut m = mock.lock().unwrap();
        m.requests += 1;
        if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
            m.authorizations.push(auth.to_string());
        }
        if headers.keys().any(|k| k.as_str().starts_with("x-geoclass-")) {
            m.mesh_keys_seen += 1;
        }
        (m.mode, m.region.clone())
    };
    let sse = [(axum::http::header::CONTENT_TYPE, "text/event-stream")];
    match mode {
        Mode::Ok => (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/event-stream"), (axum::http::HeaderName::from_static("cf-ray"), "8a1b2c3d4e5f-tst")],
            format!(
                "event: response.created\ndata: {{\"type\":\"response.created\"}}\n\n\
                 event: response.output_item.added\ndata: {{\"type\":\"response.output_item.added\"}}\n\n\
                 event: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"delta\":\"hello from {region}\"}}\n\n\
                 event: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{{\"usage\":{{\"input_tokens\":100,\"output_tokens\":5,\"input_tokens_details\":{{\"cached_tokens\":80}}}}}}}}\n\n"
            ),
        )
            .into_response(),
        Mode::Overloaded => (
            axum::http::StatusCode::OK,
            sse,
            "event: response.created\ndata: {\"type\":\"response.created\"}\n\n\
             event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Selected model is at capacity.\"}}}\n\n",
        )
            .into_response(),
        Mode::Quota => (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            r#"{"error":{"type":"usage_limit_reached","message":"limit"}}"#,
        )
            .into_response(),
        Mode::Unavailable => (axum::http::StatusCode::SERVICE_UNAVAILABLE, "upstream down").into_response(),
    }
}

struct Node {
    region: String,
    ingress: String,
    mesh: String,
    mock: Arc<Mutex<Mock>>,
    state: Arc<geoclass::state::AppState>,
}

struct Cluster {
    nodes: Vec<Node>,
    client: reqwest::Client,
}

async fn listener() -> (tokio::net::TcpListener, String) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    (l, format!("http://{addr}"))
}

/// `weights[i]` is region i's static weight; regions are named r0, r1, r2.
async fn cluster(weights: &[f64]) -> Cluster {
    let n = weights.len();
    let mut mocks = Vec::new();
    for i in 0..n {
        let mock = Arc::new(Mutex::new(Mock { region: format!("r{i}"), mode: Mode::Ok, requests: 0, authorizations: vec![], mesh_keys_seen: 0 }));
        let (l, url) = listener().await;
        let app = axum::Router::new().fallback(mock_handler).with_state(mock.clone());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        mocks.push((mock, url));
    }
    let mut ingress = Vec::new();
    let mut mesh = Vec::new();
    for _ in 0..n {
        ingress.push(listener().await);
        mesh.push(listener().await);
    }
    let mesh_urls: Vec<String> = mesh.iter().map(|(_, u)| u.clone()).collect();
    let regions_toml: String = weights
        .iter()
        .enumerate()
        .map(|(i, w)| format!("[[regions]]\nname = \"r{i}\"\nweight = {w}\n"))
        .collect();
    let mut nodes = Vec::new();
    for (i, ((ingress_l, ingress_url), (mesh_l, mesh_url))) in ingress.into_iter().zip(mesh).enumerate() {
        let peers: Vec<String> = mesh_urls.iter().filter(|u| **u != mesh_url).map(|u| format!("\"{u}\"")).collect();
        let toml = format!(
            "region = \"r{i}\"\nnode_id = \"node-{i}\"\ndiscovery = \"static\"\npeers = [{}]\nmesh_key = \"{MESH_KEY}\"\n\
             upstream = \"{}\"\nadvertise_url = \"{mesh_url}\"\ngossip_interval_ms = 100\ndiscovery_interval_ms = 200\n\
             [thresholds]\nmin_samples = 3\n{regions_toml}",
            peers.join(","),
            mocks[i].1
        );
        let cfg = Config::from_toml(&toml, None).unwrap();
        let state = server::start(cfg, Arc::new(Store::in_memory().unwrap()), UI_TOKEN.into()).unwrap();
        let apps = server::routers(state.clone());
        tokio::spawn(async move { axum::serve(ingress_l, apps.ingress).await.unwrap() });
        tokio::spawn(async move {
            axum::serve(mesh_l, apps.mesh.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap()
        });
        nodes.push(Node { region: format!("r{i}"), ingress: ingress_url, mesh: mesh_url, mock: mocks[i].0.clone(), state });
    }
    let c = Cluster { nodes, client: reqwest::Client::new() };
    c.wait_until("every node sees every peer live", |c| {
        c.nodes.iter().all(|node| {
            let now = geoclass::now_ms();
            let mesh = node.state.mesh.lock().unwrap();
            mesh.peers.values().filter(|p| node.state.is_live(p, now)).count() == n - 1
        })
    })
    .await;
    c
}

impl Cluster {
    async fn wait_until(&self, what: &str, check: impl Fn(&Cluster) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !check(self) {
            assert!(Instant::now() < deadline, "timed out waiting until {what}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn set(&self, region: usize, mode: Mode) {
        self.nodes[region].mock.lock().unwrap().mode = mode;
    }

    fn requests(&self, region: usize) -> usize {
        self.nodes[region].mock.lock().unwrap().requests
    }

    async fn send(&self, via: usize, key: Option<&str>) -> (u16, reqwest::header::HeaderMap, String) {
        let mut body = serde_json::json!({"model": "gpt-test", "stream": true, "input": "hi"});
        if let Some(key) = key {
            body["prompt_cache_key"] = key.into();
        }
        let r = self
            .client
            .post(format!("{}/backend-api/codex/responses", self.nodes[via].ingress))
            .header("authorization", CANARY)
            .header("chatgpt-account-id", "acct-1")
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        let headers = r.headers().clone();
        (status, headers, r.text().await.unwrap())
    }
}

fn region_of(headers: &reqwest::header::HeaderMap) -> String {
    headers.get("x-geoclass-region").unwrap().to_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_before_output_is_retried_elsewhere_and_the_session_sticks() {
    // r0 is overwhelmingly preferred for new sessions.
    let c = cluster(&[1000.0, 1.0, 1.0]).await;
    c.set(0, Mode::Overloaded);
    let (status, headers, body) = c.send(0, Some("session-1")).await;
    assert_eq!(status, 200);
    let served = region_of(&headers);
    assert_ne!(served, "r0", "the overloaded region must not serve");
    assert_eq!(body.matches("response.created").count() / 2, 1, "the client sees exactly one attempt: {body}");
    assert!(body.contains(&format!("hello from {served}")));
    assert!(!body.contains("server_is_overloaded"));
    assert_eq!(headers.get("x-geoclass-attempts").unwrap(), "2");
    assert_eq!(c.requests(0), 1);
    let served_idx: usize = served[1..].parse().unwrap();

    // The account token reached the upstream unchanged; GeoClass headers did not.
    {
        let mock = c.nodes[served_idx].mock.lock().unwrap();
        assert_eq!(mock.authorizations, vec![CANARY.to_string()]);
        assert_eq!(mock.mesh_keys_seen, 0);
    }

    // r0 recovers; the session stays where it moved, even when entering through another node.
    c.set(0, Mode::Ok);
    let other = (0..3).find(|i| *i != 0 && *i != served_idx).unwrap();
    c.wait_until("the binding reaches every node", |c| {
        let key = geoclass::sha256_hex("session-1");
        c.nodes.iter().all(|n| {
            n.state.mesh.lock().unwrap().bindings.as_mut().unwrap().get(&key, geoclass::now_ms()).is_some_and(|b| b.region == served)
        })
    })
    .await;
    for via in [0, other, served_idx] {
        let (status, headers, _) = c.send(via, Some("session-1")).await;
        assert_eq!(status, 200);
        assert_eq!(region_of(&headers), served, "entered via node {via}");
        assert_eq!(headers.get("x-geoclass-reason").unwrap(), "bound");
    }

    // The bound region fails: the session moves again and sticks to its new region.
    c.set(served_idx, Mode::Overloaded);
    let (status, headers, _) = c.send(other, Some("session-1")).await;
    assert_eq!(status, 200);
    let moved_to = region_of(&headers);
    assert_ne!(moved_to, served);
    assert_eq!(headers.get("x-geoclass-reason").unwrap().to_str().unwrap(), "moved_failure");

    // Cluster statistics see the rescues from every node.
    let stats: serde_json::Value = c
        .client
        .get(format!("{}/admin/api/stats?scope=cluster&since=1h&bucket=minute&by=region", c.nodes[0].ingress))
        .bearer_auth(UI_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["scope"], "cluster");
    assert_eq!(stats["nodes"].as_array().unwrap().len(), 3);
    assert_eq!(stats["totals"]["rescued"], 2, "{stats}");
    assert_eq!(stats["totals"]["overload_hit"], 2);
    assert!(stats["totals"]["events"]["session_moved"].as_i64().is_some_and(|n| n >= 1), "{}", stats["totals"]);
    assert!(stats["totals"]["cached_input_tokens"].as_i64().unwrap() >= 80);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_errors_pass_through_without_retry() {
    let c = cluster(&[1000.0, 1.0, 1.0]).await;
    c.set(0, Mode::Quota);
    let (status, headers, body) = c.send(1, Some("session-q")).await;
    assert_eq!(status, 429);
    assert!(body.contains("usage_limit_reached"));
    assert_eq!(region_of(&headers), "r0");
    assert_eq!(c.requests(1) + c.requests(2), 0, "a quota error is never retried in another region");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_region_overloaded_returns_the_last_failure() {
    let c = cluster(&[1.0, 1.0, 1.0]).await;
    for i in 0..3 {
        c.set(i, Mode::Overloaded);
    }
    let (status, headers, body) = c.send(2, None).await;
    assert_eq!(status, 200);
    assert!(body.contains("server_is_overloaded"), "underclass must still see the capacity failure: {body}");
    assert_eq!(headers.get("x-geoclass-attempts").unwrap(), "3");
    for i in 0..3 {
        assert_eq!(c.requests(i), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_errors_retry_and_peers_must_present_the_mesh_key() {
    let c = cluster(&[1000.0, 1.0, 1.0]).await;
    c.set(0, Mode::Unavailable);
    let (status, headers, _) = c.send(0, None).await;
    assert_eq!(status, 200);
    assert_ne!(region_of(&headers), "r0");

    let unauthenticated = c.client.post(format!("{}/relay/backend-api/codex/responses", c.nodes[1].mesh)).body("{}").send().await.unwrap();
    assert_eq!(unauthenticated.status().as_u16(), 401);
    let wrong = c
        .client
        .get(format!("{}/mesh/bindings", c.nodes[1].mesh))
        .header("x-geoclass-mesh-key", "nope")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status().as_u16(), 401);
    let admin = c.client.get(format!("{}/admin/api/monitor", c.nodes[1].ingress)).send().await.unwrap();
    assert_eq!(admin.status().as_u16(), 401);
    let monitor: serde_json::Value = c
        .client
        .get(format!("{}/admin/api/monitor", c.nodes[1].ingress))
        .bearer_auth(UI_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(monitor["regions"].as_array().unwrap().len(), 3);
    assert!(!monitor.to_string().contains("canary"), "the monitor must never expose credentials");
    assert_eq!(c.nodes[1].region, "r1");
}
