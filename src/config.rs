//! Configuration: `config.toml` plus `GEOCLASS_*` environment overrides.
//!
//! Regions are operator data. Nothing in this crate knows any region by name; a region is
//! whatever string the operator puts in `region` and `[[regions]]`.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const SYSTEM_MONITOR_SOCKET: &str = "/run/geoclass/monitor.sock";
pub const DEFAULT_UPSTREAM: &str = "https://chatgpt.com";
pub const DEFAULT_TAILSCALE_SOCKET: &str = "/run/tailscale/tailscaled.sock";
pub const DEFAULT_TAILSCALE_TAG: &str = "tag:geoclass";

/// Per-region operator data. Everything except `name` is optional.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct RegionConfig {
    pub name: String,
    /// Regions sharing a failure domain are known to hit the same upstream serving pool; retries
    /// leave a failed domain before trying another region in it.
    #[serde(default)]
    pub failure_domain: Option<String>,
    /// Static multiplier on every hour's weight (default 1).
    #[serde(default)]
    pub weight: Option<f64>,
    /// 24 weights, one per UTC hour, for weekdays (and weekends unless `weekend` is set).
    #[serde(default)]
    pub hourly: Option<Vec<f64>>,
    /// 24 weights, one per UTC hour, for Saturday and Sunday (UTC).
    #[serde(default)]
    pub weekend: Option<Vec<f64>>,
    /// Shorthand when `hourly` is absent: the region's UTC offset, its local peak window
    /// `[start, end)` in hours, and the weight to use inside it (default 0.5; 1 outside).
    #[serde(default)]
    pub utc_offset_hours: Option<f64>,
    #[serde(default)]
    pub peak_hours: Option<[u8; 2]>,
    #[serde(default)]
    pub peak_weight: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct Thresholds {
    /// Sliding health window length.
    pub window_ms: i64,
    /// Overload rate at or above which a region turns red.
    pub red_rate: f64,
    /// Overload rate at or below which a red region turns green again.
    pub green_rate: f64,
    /// Attempts needed in the window before the rate counts.
    pub min_samples: i64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self { window_ms: 5 * 60_000, red_rate: 0.10, green_rate: 0.03, min_samples: 5 }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Discovery {
    Tailscale,
    Static,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(default)]
struct FileConfig {
    region: Option<String>,
    node_id: Option<String>,
    ingress_bind: Option<String>,
    mesh_bind: Option<String>,
    mesh_port: Option<u16>,
    advertise_url: Option<String>,
    upstream: Option<String>,
    discovery: Option<Discovery>,
    tailscale_socket: Option<String>,
    tailscale_tag: Option<String>,
    peers: Option<Vec<String>>,
    mesh_key: Option<String>,
    ui_token: Option<String>,
    stats_retention_days: Option<i64>,
    gossip_interval_ms: Option<i64>,
    discovery_interval_ms: Option<i64>,
    retry_budget: Option<usize>,
    retry_deadline_ms: Option<i64>,
    gate_max_bytes: Option<usize>,
    binding_ttl_ms: Option<i64>,
    binding_cap: Option<usize>,
    learned_prior_weight: Option<f64>,
    domain_learning: Option<bool>,
    thresholds: Option<Thresholds>,
    regions: Option<Vec<RegionConfig>>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub region: String,
    pub node_id: String,
    pub ingress_bind: String,
    pub mesh_bind: String,
    /// Port peers use to reach this node's mesh listener (defaults to `mesh_bind`'s port).
    pub mesh_port: u16,
    /// This node's mesh URL as peers should reach it (default: its tailnet IPv4 + `mesh_port`).
    pub advertise_url: Option<String>,
    pub upstream: String,
    pub discovery: Discovery,
    pub tailscale_socket: PathBuf,
    pub tailscale_tag: String,
    pub peers: Vec<String>,
    pub mesh_key: Option<String>,
    pub ui_token: Option<String>,
    pub stats_retention_days: i64,
    pub gossip_interval_ms: i64,
    pub discovery_interval_ms: i64,
    /// Most regions one request may try (0 means every known region).
    pub retry_budget: usize,
    pub retry_deadline_ms: i64,
    pub gate_max_bytes: usize,
    pub binding_ttl_ms: i64,
    pub binding_cap: usize,
    /// How far learned overload history may pull an hour's weight away from the configured prior
    /// (0 = configured prior only, 1 = learned only).
    pub learned_prior_weight: f64,
    pub domain_learning: bool,
    pub thresholds: Thresholds,
    pub regions: Vec<RegionConfig>,
    pub data_dir: PathBuf,
    pub config_path: PathBuf,
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/"))
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn env_parse<T: std::str::FromStr>(name: &str) -> Option<T> {
    env(name).and_then(|v| v.parse().ok())
}

pub fn default_config_path() -> PathBuf {
    home_dir().join(".config/geoclass/config.toml")
}

pub fn default_data_dir() -> PathBuf {
    home_dir().join(".local/share/geoclass")
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| env("HOSTNAME"))
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "geoclass".to_string())
}

impl Config {
    /// @cc [owner:ghuntley,label:config] region-required-and-opaque
    /// `load` MUST fail unless this node's region is a non-empty string from `--region`,
    /// `GEOCLASS_REGION`, or `region` in the config file, MUST reject duplicate `[[regions]]`
    /// names and malformed hour tables, and MUST add an empty `[[regions]]` entry for this
    /// node's region when the operator did not list it. Region names MUST NOT be interpreted.
    pub fn load(region_override: Option<String>) -> Result<Self, String> {
        let config_path = env("GEOCLASS_CONFIG").map(PathBuf::from).unwrap_or_else(default_config_path);
        let file: FileConfig = match std::fs::read_to_string(&config_path) {
            Ok(text) => toml::from_str(&text).map_err(|e| format!("{}: {e}", config_path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => FileConfig::default(),
            Err(e) => return Err(format!("{}: {e}", config_path.display())),
        };
        Self::from_parts(file, region_override, config_path)
    }

    /// Parses a config document directly (tests and `geoclass check-config`).
    pub fn from_toml(text: &str, region_override: Option<String>) -> Result<Self, String> {
        let file: FileConfig = toml::from_str(text).map_err(|e| e.to_string())?;
        Self::from_parts(file, region_override, PathBuf::from("<inline>"))
    }

    fn from_parts(file: FileConfig, region_override: Option<String>, config_path: PathBuf) -> Result<Self, String> {
        let region = region_override
            .filter(|r| !r.trim().is_empty())
            .or_else(|| env("GEOCLASS_REGION"))
            .or(file.region)
            .map(|r| r.trim().to_string())
            .filter(|r| !r.is_empty())
            .ok_or("this node's region is not set: pass --region, set GEOCLASS_REGION, or set region in config.toml")?;
        let mesh_bind = env("GEOCLASS_MESH_BIND").or(file.mesh_bind).unwrap_or_else(|| "0.0.0.0:9091".to_string());
        let bind_port = mesh_bind.rsplit(':').next().and_then(|p| p.parse::<u16>().ok()).unwrap_or(9091);
        let discovery = match env("GEOCLASS_DISCOVERY").as_deref() {
            Some("tailscale") => Discovery::Tailscale,
            Some("static") => Discovery::Static,
            Some(other) => return Err(format!("GEOCLASS_DISCOVERY must be tailscale or static, not {other:?}")),
            None => file.discovery.unwrap_or(Discovery::Tailscale),
        };
        let peers = env("GEOCLASS_PEERS")
            .map(|v| v.split(',').map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).collect())
            .or(file.peers)
            .unwrap_or_default();
        let mut regions = file.regions.unwrap_or_default();
        let mut seen = std::collections::BTreeSet::new();
        for r in &regions {
            if r.name.trim().is_empty() {
                return Err("every [[regions]] entry needs a name".into());
            }
            if !seen.insert(r.name.clone()) {
                return Err(format!("region {:?} is listed twice", r.name));
            }
            for (field, table) in [("hourly", &r.hourly), ("weekend", &r.weekend)] {
                if let Some(table) = table
                    && (table.len() != 24 || table.iter().any(|w| !w.is_finite() || *w < 0.0)) {
                        return Err(format!("region {:?}: {field} must be 24 non-negative numbers", r.name));
                    }
            }
            if let Some([start, end]) = r.peak_hours
                && (start > 24 || end > 24) {
                    return Err(format!("region {:?}: peak_hours must be within 0..=24", r.name));
                }
        }
        if !regions.iter().any(|r| r.name == region) {
            regions.push(RegionConfig { name: region.clone(), ..Default::default() });
        }
        let thresholds = file.thresholds.unwrap_or_default();
        if !(0.0..=1.0).contains(&thresholds.green_rate)
            || !(0.0..=1.0).contains(&thresholds.red_rate)
            || thresholds.green_rate > thresholds.red_rate
        {
            return Err("thresholds: need 0 <= green_rate <= red_rate <= 1".into());
        }
        Ok(Self {
            region,
            node_id: env("GEOCLASS_NODE_ID").or(file.node_id).unwrap_or_else(hostname),
            ingress_bind: env("GEOCLASS_INGRESS").or(file.ingress_bind).unwrap_or_else(|| "127.0.0.1:9090".to_string()),
            mesh_port: env_parse("GEOCLASS_MESH_PORT").or(file.mesh_port).unwrap_or(bind_port),
            mesh_bind,
            advertise_url: env("GEOCLASS_ADVERTISE_URL").or(file.advertise_url).map(|u| u.trim_end_matches('/').to_string()),
            upstream: env("GEOCLASS_UPSTREAM")
                .or(file.upstream)
                .unwrap_or_else(|| DEFAULT_UPSTREAM.to_string())
                .trim_end_matches('/')
                .to_string(),
            discovery,
            tailscale_socket: env("GEOCLASS_TAILSCALE_SOCKET")
                .or(file.tailscale_socket)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_TAILSCALE_SOCKET)),
            tailscale_tag: env("GEOCLASS_TAILSCALE_TAG").or(file.tailscale_tag).unwrap_or_else(|| DEFAULT_TAILSCALE_TAG.to_string()),
            peers,
            mesh_key: env("GEOCLASS_MESH_KEY").or(file.mesh_key),
            ui_token: env("GEOCLASS_UI_TOKEN").or(file.ui_token),
            stats_retention_days: env_parse("GEOCLASS_STATS_RETENTION_DAYS")
                .or(file.stats_retention_days)
                .unwrap_or(crate::stats::DEFAULT_RETENTION_DAYS)
                .max(1),
            gossip_interval_ms: file.gossip_interval_ms.unwrap_or(2_000).max(100),
            discovery_interval_ms: file.discovery_interval_ms.unwrap_or(15_000).max(100),
            retry_budget: file.retry_budget.unwrap_or(0),
            retry_deadline_ms: file.retry_deadline_ms.unwrap_or(120_000).max(1_000),
            gate_max_bytes: file.gate_max_bytes.unwrap_or(256 * 1024).max(1024),
            binding_ttl_ms: file.binding_ttl_ms.unwrap_or(24 * 3_600_000).max(60_000),
            binding_cap: file.binding_cap.unwrap_or(100_000).max(1),
            learned_prior_weight: file.learned_prior_weight.unwrap_or(0.5).clamp(0.0, 1.0),
            domain_learning: file.domain_learning.unwrap_or(true),
            thresholds,
            regions,
            data_dir: env("GEOCLASS_DATA_DIR").map(PathBuf::from).unwrap_or_else(default_data_dir),
            config_path,
        })
    }

    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("stats.db")
    }

    /// @cc [owner:ghuntley,label:cli] monitor-socket-location
    /// The server MUST honor a nonempty `GEOCLASS_MONITOR_SOCKET` and otherwise place its local
    /// monitor socket beside the SQLite database.
    pub fn monitor_socket_path(&self) -> PathBuf {
        env("GEOCLASS_MONITOR_SOCKET").map(PathBuf::from).unwrap_or_else(|| self.data_dir.join("monitor.sock"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_is_required_and_added_to_the_region_list() {
        assert!(Config::from_toml("", None).is_err());
        let cfg = Config::from_toml("region = \"anything-at-all\"", None).unwrap();
        assert_eq!(cfg.region, "anything-at-all");
        assert_eq!(cfg.regions.len(), 1);
        assert_eq!(cfg.regions[0].name, "anything-at-all");
    }

    #[test]
    fn override_wins_and_regions_are_validated() {
        let text = r#"
            region = "a"
            [[regions]]
            name = "a"
            failure_domain = "pool-1"
            [[regions]]
            name = "b"
            hourly = [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
        "#;
        let cfg = Config::from_toml(text, Some("b".into())).unwrap();
        assert_eq!(cfg.region, "b");
        assert_eq!(cfg.regions.len(), 2);
        assert!(Config::from_toml("region = \"a\"\n[[regions]]\nname = \"a\"\n[[regions]]\nname = \"a\"", None).is_err());
        assert!(Config::from_toml("region = \"a\"\n[[regions]]\nname = \"a\"\nhourly = [1, 2]", None).is_err());
        assert!(Config::from_toml("region = \"a\"\n[thresholds]\nred_rate = 0.01\ngreen_rate = 0.5", None).is_err());
    }

    #[test]
    fn mesh_port_follows_bind() {
        let cfg = Config::from_toml("region = \"a\"\nmesh_bind = \"100.64.0.1:7000\"", None).unwrap();
        assert_eq!(cfg.mesh_port, 7000);
    }
}
