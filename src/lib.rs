pub mod affinity;
pub mod bindings;
pub mod config;
pub mod correlation;
pub mod discovery;
pub mod domains;
pub mod gate;
pub mod health;
pub mod ingress;
pub mod logging;
pub mod mesh;
pub mod metrics;
pub mod monitor;
pub mod placement;
pub mod prior;
pub mod relay;
pub mod server;
pub mod sse;
pub mod state;
pub mod stats;
pub mod stats_api;
pub mod stats_cli;
pub mod store;
pub mod tailscale;
pub mod top;
pub mod ui;

/// Wall-clock milliseconds since the Unix epoch. Only the async edges call this; the pure core
/// (`placement`, `health`, `bindings`, `domains`, `prior`, `gate`, `stats`) takes time as a parameter.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Lowercase hex SHA-256. Session keys and account ids are only ever stored, logged, or gossiped
/// in this form.
pub fn sha256_hex(value: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(value.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}
