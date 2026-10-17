//! Server assembly: state, background loops, and the three routers (ingress, mesh, monitor socket).
use crate::config::{Config, Discovery};
use crate::state::{self, AppState};
use crate::store::Store;
use crate::{correlation, ingress, mesh, metrics, monitor, relay, stats_api, ui};
use axum::Router;
use std::sync::Arc;

/// How often stats retention pruning and finished-day rollups run.
const STATS_TICK: std::time::Duration = std::time::Duration::from_secs(900);

pub struct Routers {
    /// Loopback listener for underclass: the admin UI/API plus the upstream fallback.
    pub ingress: Router,
    /// Tailnet listener: mesh, relay, metrics, and the admin UI/API.
    pub mesh: Router,
    /// Local Unix socket: the read-only monitor snapshot only.
    pub local: Router,
}

/// Builds the state and spawns discovery, gossip, and stats maintenance. Needs a Tokio runtime.
pub fn start(cfg: Config, store: Arc<Store>, ui_token: String) -> Result<Arc<AppState>, String> {
    let tailscale = match cfg.discovery {
        Discovery::Tailscale => Some(crate::tailscale::LocalApi::new(cfg.tailscale_socket.clone())?),
        Discovery::Static => None,
    };
    if cfg.mesh_key.as_deref().is_none_or(str::is_empty) {
        tracing::warn!("no mesh key configured (GEOCLASS_MESH_KEY): running as a single-region node");
    }
    let state = Arc::new(AppState::new(cfg, store.clone(), ui_token, tailscale)?);
    state.persist(vec![state.event("node_started", |e| e.detail = Some(state::VERSION.to_string()))], vec![]);
    tokio::spawn(crate::discovery::discovery_loop(state.clone()));
    tokio::spawn(mesh::gossip_loop(state.clone()));
    let retention_days = state.cfg.stats_retention_days;
    tokio::spawn(async move {
        loop {
            let store = store.clone();
            match tokio::task::spawn_blocking(move || stats_api::maintain(&store, crate::now_ms(), retention_days)).await {
                Ok(Ok((pruned, rolled))) if pruned > 0 || rolled > 0 => tracing::info!(pruned, rolled, retention_days, "stats.maintained"),
                Ok(Ok(_)) => {}
                Ok(Err(error)) => tracing::error!(error = %error, "stats.maintenance_failed"),
                Err(error) => tracing::error!(error = %error, "stats.maintenance_failed"),
            }
            tokio::time::sleep(STATS_TICK).await;
        }
    });
    Ok(state)
}

/// @cc [owner:ghuntley,label:security] listener-surfaces
/// The ingress router MUST expose only the admin UI, the token-gated admin API, and the upstream
/// fallback; the mesh router MUST expose `/mesh/*` and `/relay/*` only behind `require_mesh`,
/// plus `/metrics` and the token-gated admin API; the local router MUST expose only `/monitor`.
pub fn routers(state: Arc<AppState>) -> Routers {
    let admin = Router::new()
        .route("/admin/api/monitor", axum::routing::get(monitor::snapshot))
        .route("/admin/api/stats", axum::routing::get(ui::stats))
        .route("/admin/api/health", axum::routing::get(ui::health))
        .route("/admin/api/routing", axum::routing::get(ui::routing))
        .route("/admin/api/peers", axum::routing::get(mesh::peers))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), ui::require_ui_token))
        .route("/", axum::routing::get(ui::index));
    let ingress = admin
        .clone()
        .fallback(ingress::ingress)
        .layer(axum::middleware::from_fn(correlation::middleware))
        .with_state(state.clone());
    let mesh_routes = Router::new()
        .route("/mesh/info", axum::routing::get(mesh::info))
        .route("/mesh/gossip", axum::routing::post(mesh::gossip))
        .route("/mesh/bindings", axum::routing::get(mesh::bindings))
        .route("/mesh/peers", axum::routing::get(mesh::peers))
        .route("/mesh/stats", axum::routing::get(mesh::stats))
        .route("/relay", axum::routing::any(relay::relay_handler))
        .route("/relay/{*path}", axum::routing::any(relay::relay_handler))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), mesh::require_mesh))
        .route("/metrics", axum::routing::get(metrics::metrics));
    let mesh = admin
        .merge(mesh_routes)
        .layer(axum::middleware::from_fn(correlation::middleware))
        .with_state(state.clone());
    let local = Router::new().route("/monitor", axum::routing::get(monitor::snapshot)).with_state(state);
    Routers { ingress, mesh, local }
}
