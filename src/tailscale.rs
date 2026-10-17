//! Minimal client for the local `tailscaled` LocalAPI (over its Unix socket).
//!
//! GeoClass uses two calls: `status` to list tailnet peers carrying the GeoClass tag, and
//! `whois` to check that an inbound mesh connection comes from such a peer.
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

const ORIGIN: &str = "http://local-tailscaled.sock";

#[derive(Clone)]
pub struct LocalApi {
    client: reqwest::Client,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct PeerStatus {
    #[serde(rename = "HostName", default)]
    pub host_name: String,
    #[serde(rename = "DNSName", default)]
    pub dns_name: String,
    #[serde(rename = "TailscaleIPs", default)]
    pub tailscale_ips: Vec<String>,
    #[serde(rename = "Online", default)]
    pub online: bool,
    #[serde(rename = "Tags", default)]
    pub tags: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Status {
    #[serde(rename = "Self", default)]
    pub self_status: Option<PeerStatus>,
    #[serde(rename = "Peer", default)]
    pub peer: Option<BTreeMap<String, PeerStatus>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct WhoIsNode {
    #[serde(rename = "Tags", default)]
    tags: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct WhoIs {
    #[serde(rename = "Node", default)]
    node: Option<WhoIsNode>,
}

impl LocalApi {
    pub fn new(socket: PathBuf) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .unix_socket(socket)
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| format!("tailscale localapi client: {e}"))?;
        Ok(Self { client })
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, String> {
        let response = self
            .client
            .get(format!("{ORIGIN}{path}"))
            .header("Sec-Tailscale", "localapi")
            .send()
            .await
            .map_err(|e| format!("tailscale localapi: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("tailscale localapi {path}: HTTP {}", response.status().as_u16()));
        }
        response.json().await.map_err(|e| format!("tailscale localapi {path}: {e}"))
    }

    pub async fn status(&self) -> Result<Status, String> {
        self.get("/localapi/v0/status").await
    }

    /// Tags of the tailnet node owning `addr` (`ip:port`); `Ok(vec![])` for an untagged node.
    pub async fn whois_tags(&self, addr: &str) -> Result<Vec<String>, String> {
        let who: WhoIs = self.get(&format!("/localapi/v0/whois?addr={addr}")).await?;
        Ok(who.node.and_then(|n| n.tags).unwrap_or_default())
    }
}

/// @cc [owner:ghuntley,label:mesh] tagged-online-peers
/// `tagged_peers` MUST return exactly the online peers whose tags include `tag`, each with its
/// first IPv4 tailnet address (or first address when none is IPv4); it MUST NOT return this node.
pub fn tagged_peers(status: &Status, tag: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = status
        .peer
        .iter()
        .flatten()
        .filter(|(_, p)| p.online && p.tags.as_ref().is_some_and(|t| t.iter().any(|t| t == tag)))
        .filter_map(|(_, p)| {
            let ip = p.tailscale_ips.iter().find(|ip| !ip.contains(':')).or_else(|| p.tailscale_ips.first())?;
            let name = if p.host_name.is_empty() { p.dns_name.clone() } else { p.host_name.clone() };
            Some((name, ip.clone()))
        })
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_online_tagged_peers() {
        let status: Status = serde_json::from_str(r#"{
            "Self": {"HostName": "me", "TailscaleIPs": ["100.64.0.1"], "Online": true, "Tags": ["tag:geoclass"]},
            "Peer": {
                "a": {"HostName": "eu-1", "TailscaleIPs": ["fd7a::2", "100.64.0.2"], "Online": true, "Tags": ["tag:geoclass"]},
                "b": {"HostName": "laptop", "TailscaleIPs": ["100.64.0.3"], "Online": true},
                "c": {"HostName": "eu-2", "TailscaleIPs": ["100.64.0.4"], "Online": false, "Tags": ["tag:geoclass"]},
                "d": {"HostName": "ci", "TailscaleIPs": ["100.64.0.5"], "Online": true, "Tags": ["tag:other"]}
            }
        }"#).unwrap();
        assert_eq!(tagged_peers(&status, "tag:geoclass"), vec![("eu-1".to_string(), "100.64.0.2".to_string())]);
    }
}
