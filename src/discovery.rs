//! Peer discovery: the tailnet (LocalAPI `status`, filtered by tag) or a static peer list.
use crate::state::{AppState, Peer};
use std::sync::Arc;

/// @cc [owner:ghuntley,label:mesh] discovery-reconciles-peers
/// `reconcile` MUST add every discovered candidate URL not yet known, MUST remove peers from the
/// same discovery source that are no longer discovered (recording `peer_lost`), MUST NOT add this
/// node itself, and MUST leave peers learned through gossip untouched.
pub fn reconcile(state: &AppState, source: &str, candidates: &[(String, String)], now: i64) -> Vec<crate::store::Event> {
    let mut events = Vec::new();
    let mut mesh = state.mesh.lock().unwrap();
    let wanted: std::collections::BTreeSet<&str> = candidates.iter().map(|(_, url)| url.as_str()).collect();
    let lost: Vec<String> = mesh
        .peers
        .iter()
        .filter(|(url, p)| p.source == source && !wanted.contains(url.as_str()))
        .map(|(url, _)| url.clone())
        .collect();
    for url in lost {
        if let Some(peer) = mesh.peers.remove(&url) {
            let region = peer.info.as_ref().map(|i| i.region.clone());
            events.push(state.event("peer_lost", |e| {
                e.region = region;
                e.detail = Some(peer.host.clone());
            }));
        }
    }
    for (host, url) in candidates {
        if mesh.peers.contains_key(url) {
            continue;
        }
        mesh.peers.insert(url.clone(), Peer {
            url: url.clone(),
            source: source.to_string(),
            host: host.clone(),
            info: None,
            discovered_ms: now,
            last_digest_ms: None,
            windows: Vec::new(),
            inflight: 0,
            stale_reported: false,
            synced: false,
        });
    }
    events
}

/// Discovers peers every `discovery_interval_ms` (15 s by default) (Tailscale LocalAPI or the static list) and fetches their info.
pub async fn discovery_loop(state: Arc<AppState>) {
    let mut self_ip: Option<String> = None;
    loop {
        let now = crate::now_ms();
        let (source, candidates) = match &state.tailscale {
            Some(api) => match api.status().await {
                Ok(status) => {
                    self_ip = status
                        .self_status
                        .as_ref()
                        .and_then(|s| s.tailscale_ips.iter().find(|ip| !ip.contains(':')).cloned());
                    let peers = crate::tailscale::tagged_peers(&status, &state.cfg.tailscale_tag)
                        .into_iter()
                        .map(|(host, ip)| (host, format!("http://{ip}:{}", state.cfg.mesh_port)))
                        .collect::<Vec<_>>();
                    ("tailscale", Some(peers))
                }
                Err(error) => {
                    tracing::warn!(error = %error, "discovery.tailscale_failed");
                    ("tailscale", None)
                }
            },
            None => (
                "static",
                Some(
                    state
                        .cfg
                        .peers
                        .iter()
                        .map(|u| u.trim_end_matches('/').to_string())
                        .filter(|u| crate::mesh::valid_peer_url(u))
                        .map(|u| (u.clone(), u))
                        .collect(),
                ),
            ),
        };
        if let Some(candidates) = candidates {
            let events = reconcile(&state, source, &candidates, now);
            state.persist(events, vec![]);
        }
        let unknown: Vec<String> = {
            let mesh = state.mesh.lock().unwrap();
            mesh.peers.values().filter(|p| p.info.is_none()).map(|p| p.url.clone()).collect()
        };
        for url in unknown {
            match crate::mesh::fetch_info(&state, &url).await {
                Ok(info) if info.node_id == state.cfg.node_id => {
                    state.mesh.lock().unwrap().peers.remove(&url);
                }
                Ok(info) => {
                    if let Some(peer) = state.mesh.lock().unwrap().peers.get_mut(&url) {
                        peer.host = info.node_id.clone();
                        peer.info = Some(info);
                    }
                }
                Err(error) => tracing::debug!(peer = %url, error = %error, "discovery.info_failed"),
            }
        }
        let advertised = state
            .cfg
            .advertise_url
            .clone()
            .or_else(|| self_ip.as_ref().map(|ip| format!("http://{ip}:{}", state.cfg.mesh_port)));
        *state.self_url.lock().unwrap() = advertised;
        tokio::time::sleep(std::time::Duration::from_millis(state.cfg.discovery_interval_ms as u64)).await;
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::store::Store;

    fn state() -> AppState {
        let cfg = Config::from_toml("region = \"a\"\nnode_id = \"me\"", None).unwrap();
        AppState::new(cfg, Arc::new(Store::in_memory().unwrap()), "t".into(), None).unwrap()
    }

    #[test]
    fn reconcile_adds_and_removes_by_source() {
        let s = state();
        let found = vec![("x".to_string(), "http://10.0.0.1:9091".to_string()), ("y".to_string(), "http://10.0.0.2:9091".to_string())];
        assert!(reconcile(&s, "tailscale", &found, 0).is_empty());
        assert_eq!(s.mesh.lock().unwrap().peers.len(), 2);
        let events = reconcile(&s, "tailscale", &found[..1], 1);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "peer_lost");
        assert!(reconcile(&s, "static", &[], 2).is_empty(), "other sources are untouched");
        assert_eq!(s.mesh.lock().unwrap().peers.len(), 1);
    }
}
