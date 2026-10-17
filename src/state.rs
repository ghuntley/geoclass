//! Shared server state: config, the stats store, HTTP clients, and the mesh's soft state.
//!
//! Everything under `Mesh` is soft: it is rebuilt from gossip within seconds after a restart.
use crate::bindings::{BindingRecord, Bindings};
use crate::config::Config;
use crate::domains;
use crate::health::{self, ALL_MODELS, HealthTracker, LocalWindow, Observation, WindowCount};
use crate::placement::RegionView;
use crate::prior::{self, LearnedPrior};
use crate::store::{Event, HealthSample, Store};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Egress colos remembered for `NodeInfo::colo`.
const COLO_MEMORY: usize = 64;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

pub const MESH_KEY_HEADER: &str = "x-geoclass-mesh-key";
pub const FROM_NODE_HEADER: &str = "x-geoclass-from-node";
pub const FROM_REGION_HEADER: &str = "x-geoclass-from-region";
pub const REGION_HEADER: &str = "x-geoclass-region";
pub const NODE_HEADER: &str = "x-geoclass-node";
pub const ATTEMPTS_HEADER: &str = "x-geoclass-attempts";
pub const COLO_HEADER: &str = "x-geoclass-cf-colo";
pub const HEADERS_MS_HEADER: &str = "x-geoclass-headers-ms";
/// Missed gossip rounds after which a peer stops counting for routing.
pub const STALE_ROUNDS: i64 = 3;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub region: String,
    pub version: String,
    /// Whether this node can currently egress to the upstream.
    pub egress_ok: bool,
    /// The Cloudflare colo this node's egress reaches most often lately.
    #[serde(default)]
    pub colo: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Digest {
    pub node: NodeInfo,
    pub ts: i64,
    /// This node's own observations (as ingress) over the health window, by egress region.
    pub windows: Vec<WindowCount>,
    /// Bindings this node changed recently.
    pub bindings: Vec<BindingRecord>,
    pub inflight: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Peer {
    pub url: String,
    /// `tailscale` or `static`.
    pub source: String,
    pub host: String,
    pub info: Option<NodeInfo>,
    pub discovered_ms: i64,
    pub last_digest_ms: Option<i64>,
    #[serde(skip)]
    pub windows: Vec<WindowCount>,
    pub inflight: i64,
    pub stale_reported: bool,
    /// Whether this node already pulled the peer's full binding table.
    #[serde(skip)]
    pub synced: bool,
}

pub enum Target {
    Local,
    Peer { url: String, node_id: String },
}

#[derive(Default)]
pub struct Mesh {
    pub local: LocalWindow,
    pub peers: BTreeMap<String, Peer>,
    pub health: HealthTracker,
    pub bindings: Option<Bindings>,
    pub learned: LearnedPrior,
    pub history: domains::History,
    pub ttft_history: domains::History,
    /// Colos this node's own egress reached, most recent last.
    pub colos: VecDeque<String>,
    pub domains: BTreeMap<String, String>,
    pub last_minute: i64,
    pub last_weights: BTreeMap<String, f64>,
    pub egress_ok: bool,
}

pub struct AppState {
    pub cfg: Arc<Config>,
    pub store: Arc<Store>,
    /// Upstream egress client.
    pub client: reqwest::Client,
    /// Peer-to-peer client.
    pub mesh_client: reqwest::Client,
    pub mesh: Mutex<Mesh>,
    pub ui_token: String,
    pub tailscale: Option<crate::tailscale::LocalApi>,
    pub inflight: AtomicI64,
    pub inflight_by_region: Mutex<BTreeMap<String, i64>>,
    pub started_ms: i64,
    /// This node's mesh URL as peers reach it (config `advertise_url`, else the tailnet IP).
    pub self_url: Mutex<Option<String>>,
}

fn spread_score(seed: &str, id: &str) -> u64 {
    use sha2::{Digest as _, Sha256};
    let d = Sha256::digest(format!("{seed}\0{id}").as_bytes());
    u64::from_be_bytes(d[..8].try_into().unwrap())
}

impl AppState {
    pub fn new(cfg: Config, store: Arc<Store>, ui_token: String, tailscale: Option<crate::tailscale::LocalApi>) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?;
        let mesh_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| e.to_string())?;
        let mesh = Mesh {
            bindings: Some(Bindings::new(cfg.binding_cap, cfg.binding_ttl_ms)),
            domains: domains::assign(&cfg.regions, &domains::Signals::default(), false),
            egress_ok: true,
            ..Default::default()
        };
        let cfg_advertise = cfg.advertise_url.clone();
        Ok(Self {
            cfg: Arc::new(cfg),
            store,
            client,
            mesh_client,
            mesh: Mutex::new(mesh),
            ui_token,
            tailscale,
            inflight: AtomicI64::new(0),
            inflight_by_region: Mutex::new(BTreeMap::new()),
            started_ms: crate::now_ms(),
            self_url: Mutex::new(cfg_advertise),
        })
    }

    pub fn node_info(&self, mesh: &Mesh) -> NodeInfo {
        NodeInfo {
            node_id: self.cfg.node_id.clone(),
            region: self.cfg.region.clone(),
            version: VERSION.to_string(),
            egress_ok: mesh.egress_ok,
            colo: dominant(&mesh.colos),
        }
    }

    /// Remembers the colo this node's own egress just reached.
    pub fn note_egress_colo(&self, colo: Option<&str>) {
        let Some(colo) = colo else { return };
        let mut mesh = self.mesh.lock().unwrap();
        mesh.colos.push_back(colo.to_string());
        while mesh.colos.len() > COLO_MEMORY {
            mesh.colos.pop_front();
        }
    }

    /// Each region's colo: the most common `NodeInfo::colo` among this node (for its own region)
    /// and the live peers in that region.
    pub fn region_colos(&self, mesh: &Mesh, now: i64) -> BTreeMap<String, String> {
        let mut seen: BTreeMap<String, VecDeque<String>> = BTreeMap::new();
        if let Some(colo) = dominant(&mesh.colos) {
            seen.entry(self.cfg.region.clone()).or_default().push_back(colo);
        }
        for peer in mesh.peers.values().filter(|p| self.is_live(p, now)) {
            if let Some(info) = &peer.info
                && let Some(colo) = &info.colo {
                    seen.entry(info.region.clone()).or_default().push_back(colo.clone());
                }
        }
        seen.into_iter().filter_map(|(region, colos)| Some((region, dominant(&colos)?))).collect()
    }

    pub fn stale_after_ms(&self) -> i64 {
        STALE_ROUNDS * self.cfg.gossip_interval_ms
    }

    pub fn is_live(&self, peer: &Peer, now: i64) -> bool {
        peer.info.is_some() && peer.last_digest_ms.is_some_and(|t| now - t <= self.stale_after_ms())
    }

    /// Every region known from config or from live peers, sorted.
    pub fn region_names(&self, mesh: &Mesh, now: i64) -> Vec<String> {
        let mut names: BTreeSet<String> = self.cfg.regions.iter().map(|r| r.name.clone()).collect();
        for peer in mesh.peers.values().filter(|p| self.is_live(p, now)) {
            if let Some(info) = &peer.info {
                names.insert(info.region.clone());
            }
        }
        names.into_iter().collect()
    }

    fn region_config(&self, name: &str) -> crate::config::RegionConfig {
        self.cfg
            .regions
            .iter()
            .find(|r| r.name == name)
            .cloned()
            .unwrap_or_else(|| crate::config::RegionConfig { name: name.to_string(), ..Default::default() })
    }

    pub fn available(&self, mesh: &Mesh, region: &str, now: i64) -> bool {
        (region == self.cfg.region && mesh.egress_ok)
            || mesh
                .peers
                .values()
                .any(|p| self.is_live(p, now) && p.info.as_ref().is_some_and(|i| i.region == region && i.egress_ok))
    }

    pub fn weight(&self, mesh: &Mesh, region: &str, now: i64) -> f64 {
        prior::weight(&self.region_config(region), &mesh.learned, self.cfg.learned_prior_weight, now)
    }

    pub fn domain(&self, mesh: &Mesh, region: &str) -> String {
        mesh.domains.get(region).cloned().unwrap_or_else(|| region.to_string())
    }

    /// The placement snapshot for `model` at `now`.
    pub fn views(&self, mesh: &Mesh, model: &str, now: i64) -> Vec<RegionView> {
        self.region_names(mesh, now)
            .into_iter()
            .map(|name| RegionView {
                available: self.available(mesh, &name, now),
                state: mesh.health.state(&name, model),
                weight: self.weight(mesh, &name, now),
                domain: self.domain(mesh, &name),
                name,
            })
            .collect()
    }

    /// Which node egresses for `region`: this node for its own region, else a live peer in that
    /// region chosen by rendezvous on `spread` so one request's retries stay put.
    pub fn target(&self, mesh: &Mesh, region: &str, spread: &str, now: i64) -> Option<Target> {
        if region == self.cfg.region && mesh.egress_ok {
            return Some(Target::Local);
        }
        mesh.peers
            .values()
            .filter(|p| self.is_live(p, now))
            .filter_map(|p| p.info.as_ref().filter(|i| i.region == region && i.egress_ok).map(|i| (p, i)))
            .max_by_key(|(_, i)| spread_score(spread, &i.node_id))
            .map(|(p, i)| Target::Peer { url: p.url.clone(), node_id: i.node_id.clone() })
    }

    pub fn observe(&self, region: &str, model: &str, observation: Observation, now: i64) {
        self.mesh.lock().unwrap().local.observe(region, model, observation, now);
    }

    pub fn digest(&self, now: i64) -> Digest {
        let mut mesh = self.mesh.lock().unwrap();
        let windows = mesh.local.sums(now, self.cfg.thresholds.window_ms);
        let since = now - self.stale_after_ms() - self.cfg.gossip_interval_ms;
        let bindings = mesh.bindings.as_ref().map(|b| b.changed_since(since)).unwrap_or_default();
        let node = self.node_info(&mesh);
        Digest { node, ts: now, windows, bindings, inflight: self.inflight.load(Ordering::Relaxed) }
    }

    /// @cc [owner:ghuntley,label:mesh] digest-applies-to-sender
    /// `apply_digest` MUST ignore a digest from this node's own `node_id`, MUST record the digest
    /// against the peer it came from (by `node_id`, else by `url`), and MUST merge every binding
    /// it carries last-writer-wins.
    pub fn apply_digest(&self, url: Option<&str>, digest: Digest, now: i64) -> Option<Event> {
        if digest.node.node_id == self.cfg.node_id {
            return None;
        }
        let mut mesh = self.mesh.lock().unwrap();
        if let Some(bindings) = mesh.bindings.as_mut() {
            for record in digest.bindings.iter().cloned() {
                bindings.merge(record, now);
            }
        }
        let existing = mesh
            .peers
            .iter()
            .find(|(_, p)| p.info.as_ref().is_some_and(|i| i.node_id == digest.node.node_id))
            .map(|(k, _)| k.clone())
            .or_else(|| url.filter(|u| mesh.peers.contains_key(*u)).map(str::to_string));
        let mut joined = None;
        let key = match existing {
            Some(key) => key,
            None => {
                // A peer we have not discovered yet (e.g. it found us first); remember it by URL
                // only when we know how to reach it.
                let url = url?;
                mesh.peers.insert(url.to_string(), Peer {
                    url: url.to_string(),
                    source: "gossip".into(),
                    host: digest.node.node_id.clone(),
                    info: None,
                    discovered_ms: now,
                    last_digest_ms: None,
                    windows: Vec::new(),
                    inflight: 0,
                    stale_reported: false,
                    synced: false,
                });
                url.to_string()
            }
        };
        let peer = mesh.peers.get_mut(&key).unwrap();
        if peer.info.is_none() || peer.last_digest_ms.is_none() || peer.stale_reported {
            joined = Some(self.event("peer_joined", |e| {
                e.region = Some(digest.node.region.clone());
                e.detail = Some(digest.node.node_id.clone());
            }));
        }
        peer.info = Some(digest.node);
        peer.last_digest_ms = Some(now);
        peer.windows = digest.windows;
        peer.inflight = digest.inflight;
        peer.stale_reported = false;
        joined
    }

    pub fn event(&self, kind: &str, fill: impl FnOnce(&mut Event)) -> Event {
        let mut e = Event {
            ts: crate::now_ms(),
            kind: kind.to_string(),
            node: Some(self.cfg.node_id.clone()),
            node_region: Some(self.cfg.region.clone()),
            ..Default::default()
        };
        fill(&mut e);
        e
    }

    /// One gossip tick: refresh health from every live node's window, report region transitions
    /// and stale peers, and once a minute feed the learned prior, the failure-domain history, and
    /// the health samples. Returns events and samples to persist.
    pub fn tick(&self, now: i64) -> (Vec<Event>, Vec<HealthSample>) {
        let mut mesh = self.mesh.lock().unwrap();
        let own = mesh.local.sums(now, self.cfg.thresholds.window_ms);
        let mut events = Vec::new();
        let stale_after = self.stale_after_ms();
        for peer in mesh.peers.values_mut() {
            let stale = peer.last_digest_ms.is_some_and(|t| now - t > stale_after);
            if stale && !peer.stale_reported {
                peer.stale_reported = true;
                let region = peer.info.as_ref().map(|i| i.region.clone());
                events.push(self.event("gossip_stale", |e| {
                    e.region = region;
                    e.detail = Some(peer.host.clone());
                }));
            }
        }
        let live: Vec<Vec<WindowCount>> = mesh.peers.values().filter(|p| self.is_live(p, now)).map(|p| p.windows.clone()).collect();
        let merged = health::merge(std::iter::once(own.as_slice()).chain(live.iter().map(Vec::as_slice)));
        let transitions = mesh.health.update(merged, &self.cfg.thresholds);
        for t in transitions {
            let domain = self.domain(&mesh, &t.region);
            events.push(self.event(&format!("region_{}", t.to.as_str()), |e| {
                e.region = Some(t.region.clone());
                e.model = Some(t.model.clone());
                e.domain = Some(domain);
                e.detail = Some(format!("{} -> {}", t.from.as_str(), t.to.as_str()));
            }));
        }
        let mut samples = Vec::new();
        let minute = now.div_euclid(60_000);
        if minute > mesh.last_minute {
            mesh.last_minute = minute;
            let regions = self.region_names(&mesh, now);
            let mut rates: BTreeMap<String, Option<f64>> = BTreeMap::new();
            let mut ttft: BTreeMap<String, Option<f64>> = BTreeMap::new();
            for region in &regions {
                let counts = mesh.health.counts(region, ALL_MODELS);
                let rate = (counts.attempts > 0).then(|| counts.rate());
                if let Some(rate) = rate {
                    mesh.learned.observe(region, rate, now);
                }
                rates.insert(region.clone(), rate);
                ttft.insert(region.clone(), (counts.ttft_n > 0).then(|| counts.ttft_sum_ms as f64 / counts.ttft_n as f64));
            }
            mesh.history.record(minute, &rates);
            mesh.ttft_history.record(minute, &ttft);
            let signals = domains::Signals {
                failures: mesh.history.series(),
                ttft: mesh.ttft_history.series(),
                colos: self.region_colos(&mesh, now),
            };
            let assigned = domains::assign(&self.cfg.regions, &signals, self.cfg.domain_learning);
            for (region, domain) in &assigned {
                if mesh.domains.get(region).is_some_and(|old| old != domain) {
                    events.push(self.event("domain_changed", |e| {
                        e.region = Some(region.clone());
                        e.domain = Some(domain.clone());
                        e.detail = mesh.domains.get(region).cloned();
                    }));
                }
            }
            mesh.domains = assigned;
            for region in &regions {
                let weight = self.weight(&mesh, region, now);
                if let Some(previous) = mesh.last_weights.get(region)
                    && (weight - previous).abs() > 0.25 * previous.max(1e-9) {
                        events.push(self.event("prior_shift", |e| {
                            e.region = Some(region.clone());
                            e.detail = Some(format!("{previous:.3} -> {weight:.3}"));
                        }));
                    }
                mesh.last_weights.insert(region.clone(), weight);
            }
            let keys: Vec<(String, String)> = mesh.health.merged().keys().cloned().collect();
            for (region, model) in keys {
                let counts = mesh.health.counts(&region, &model);
                samples.push(HealthSample {
                    ts: minute * 60_000,
                    domain: self.domain(&mesh, &region),
                    attempts: counts.attempts,
                    overloaded: counts.overloaded,
                    errors: counts.errors,
                    state: mesh.health.state(&region, &model).as_str().to_string(),
                    weight: self.weight(&mesh, &region, now),
                    ttft_mean_ms: (counts.ttft_n > 0).then(|| counts.ttft_sum_ms / counts.ttft_n),
                    ttft_p50_ms: counts.ttft_hist.p50(),
                    ttft_p95_ms: counts.ttft_hist.p95(),
                    region,
                    model,
                });
            }
            if let Some(bindings) = mesh.bindings.as_mut() {
                bindings.prune(now);
            }
        }
        (events, samples)
    }

    pub fn persist(&self, events: Vec<Event>, samples: Vec<HealthSample>) {
        for event in &events {
            crate::logging::log_event(event);
            self.store.insert_event(event);
        }
        if !samples.is_empty()
            && let Err(error) = self.store.insert_health_samples(&samples) {
                tracing::error!(error = %error, "health_samples.persist_failed");
            }
    }

    pub fn inflight_add(&self, region: &str, delta: i64) {
        self.inflight.fetch_add(delta, Ordering::Relaxed);
        let mut by = self.inflight_by_region.lock().unwrap();
        let v = by.entry(region.to_string()).or_insert(0);
        *v += delta;
        if *v <= 0 {
            by.remove(region);
        }
    }
}

/// The most common entry; ties go to the one seen most recently.
pub fn dominant(items: &VecDeque<String>) -> Option<String> {
    let mut counts: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for (i, item) in items.iter().enumerate() {
        let e = counts.entry(item.as_str()).or_default();
        e.0 += 1;
        e.1 = i;
    }
    counts.into_iter().max_by_key(|(_, (n, last))| (*n, *last)).map(|(k, _)| k.to_string())
}

/// Decrements in-flight counters exactly once when dropped.
pub struct InflightGuard {
    pub state: Arc<AppState>,
    pub region: String,
}

impl InflightGuard {
    pub fn new(state: Arc<AppState>, region: &str) -> Self {
        state.inflight_add(region, 1);
        Self { state, region: region.to_string() }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.state.inflight_add(&self.region, -1);
    }
}
