use clap::{Parser, Subcommand};
use geoclass::{config, logging, server, store};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "geoclass", about = "geo-aware, prompt-cache-sticky egress router for OpenAI capacity errors")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, value_enum, default_value = "auto", global = true)]
    log_format: logging::LogFormat,
    /// config file (default: $GEOCLASS_CONFIG, else ~/.config/geoclass/config.toml)
    #[arg(long, global = true)]
    config: Option<std::path::PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    /// run a mesh node
    Serve {
        /// this node's region (any name; regions are operator-defined)
        #[arg(long)]
        region: Option<String>,
    },
    /// watch live routing in a read-only terminal dashboard
    Top {
        #[arg(long, help = "server URL (requires GEOCLASS_UI_TOKEN)")]
        url: Option<String>,
    },
    /// show routing, rescue, cache, and capacity statistics
    Stats {
        #[clap(flatten)]
        args: geoclass::stats_cli::StatsArgs,
    },
    /// validate the configuration and print the resolved regions
    CheckConfig {
        #[arg(long)]
        region: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();
    if let Some(path) = &cli.config {
        // SAFETY: still single-threaded; nothing has read the environment concurrently yet.
        unsafe { std::env::set_var("GEOCLASS_CONFIG", path) };
    }
    if matches!(cli.command, None | Some(Command::Serve { .. })) {
        logging::init(cli.log_format);
    }
    let result = match cli.command {
        Some(Command::Serve { region }) => serve(region),
        None => serve(None),
        Some(Command::Top { url }) => geoclass::top::run(url),
        Some(Command::Stats { args }) => geoclass::stats_cli::run(&args),
        Some(Command::CheckConfig { region }) => check_config(region),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn check_config(region: Option<String>) -> Result<(), String> {
    let cfg = config::Config::load(region)?;
    let now = geoclass::now_ms();
    println!("node {} in region {} ({})", cfg.node_id, cfg.region, cfg.config_path.display());
    println!("discovery {:?}, upstream {}, mesh {}, ingress {}", cfg.discovery, cfg.upstream, cfg.mesh_bind, cfg.ingress_bind);
    let domains = geoclass::domains::assign(&cfg.regions, &geoclass::domains::Signals::default(), false);
    for r in &cfg.regions {
        let day = now - now.rem_euclid(geoclass::stats::DAY_MS);
        let hours: Vec<String> = (0..24)
            .map(|h| format!("{:.2}", geoclass::prior::configured_weight(r, day + h * geoclass::stats::HOUR_MS)))
            .collect();
        println!("region {} (domain {}): weight now {:.2}; today by UTC hour [{}]", r.name, domains[&r.name], geoclass::prior::configured_weight(r, now), hours.join(" "));
    }
    if cfg.mesh_key.is_none() {
        println!("warning: no mesh key (GEOCLASS_MESH_KEY); this node will not mesh");
    }
    Ok(())
}

fn serve(region: Option<String>) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(|e| e.to_string())?;
    runtime.block_on(async_serve(region)).map_err(|e| e.to_string())
}

/// @cc [owner:ghuntley,label:security] tokens-minted-once
/// The admin UI token MUST be minted on first run, persisted to the store, and reused on every
/// subsequent start; an explicitly configured token MUST take precedence. Token and mesh key
/// values MUST NOT be written to diagnostics.
async fn async_serve(region: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::Config::load(region)?;
    let store = Arc::new(store::Store::open(&cfg.db_path())?);
    let ui_token = match cfg.ui_token.clone() {
        Some(token) if !token.is_empty() => token,
        _ => match store.config_get("ui_token") {
            Some(token) => token,
            None => {
                let token = uuid::Uuid::new_v4().simple().to_string();
                store.config_set("ui_token", &token);
                token
            }
        },
    };
    let ingress_bind = cfg.ingress_bind.clone();
    let mesh_bind = cfg.mesh_bind.clone();
    let monitor_path = cfg.monitor_socket_path();
    let state = server::start(cfg, store, ui_token)?;
    let apps = server::routers(state.clone());
    let ingress_listener = tokio::net::TcpListener::bind(&ingress_bind).await?;
    let mesh_listener = tokio::net::TcpListener::bind(&mesh_bind).await?;
    let monitor_socket = bind_monitor_socket(&monitor_path)?;
    println!("geoclass node {} (region {})", state.cfg.node_id, state.cfg.region);
    println!("ingress: http://{ingress_bind}  (UNDERCLASS_CODEX_UPSTREAM=http://{ingress_bind}/backend-api/codex/responses)");
    println!("mesh:    http://{mesh_bind}");
    println!("web ui:  http://{ingress_bind}/");
    tokio::try_join!(
        axum::serve(ingress_listener, apps.ingress),
        axum::serve(mesh_listener, apps.mesh.into_make_service_with_connect_info::<std::net::SocketAddr>()),
        axum::serve(monitor_socket, apps.local),
    )?;
    Ok(())
}

/// @cc [owner:ghuntley,label:security] local-monitor-socket
/// The local monitor socket MUST be a Unix socket with mode 0666 so local users can read its
/// limited monitor endpoint. A stale socket MAY be replaced only when no server accepts it;
/// non-socket paths and live sockets MUST remain untouched.
fn bind_monitor_socket(path: &Path) -> std::io::Result<tokio::net::UnixListener> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = match tokio::net::UnixListener::bind(path) {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            if !path.symlink_metadata()?.file_type().is_socket() {
                return Err(error);
            }
            match std::os::unix::net::UnixStream::connect(path) {
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)?;
                    tokio::net::UnixListener::bind(path)?
                }
                _ => return Err(error),
            }
        }
        Err(error) => return Err(error),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
    Ok(listener)
}
