//! `geoclass top` (`gtop`): read-only live terminal monitor.
use crate::monitor::MonitorSnapshot;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table};
use std::io::IsTerminal;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

const TEXT: Color = Color::Rgb(222, 231, 242);
const MUTED: Color = Color::Rgb(139, 154, 176);
const CYAN: Color = Color::Rgb(92, 216, 229);
const GREEN: Color = Color::Rgb(111, 217, 151);
const YELLOW: Color = Color::Rgb(246, 196, 101);
const RED: Color = Color::Rgb(245, 111, 125);

fn panel(title: &str) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
        .style(Style::default().fg(TEXT))
}

fn state_color(state: &str) -> Color {
    match state {
        "green" => GREEN,
        "amber" => YELLOW,
        _ => RED,
    }
}

/// Removes control characters so remote strings cannot drive the terminal.
pub fn display(value: &str) -> String {
    value.chars().filter(|c| !c.is_control()).take(80).collect()
}

fn ms(v: Option<i64>) -> String {
    match v {
        None => "–".into(),
        Some(v) if v >= 1000 => format!("{:.1}s", v as f64 / 1000.0),
        Some(v) => format!("{v}ms"),
    }
}

pub enum Target {
    Socket(PathBuf),
    Http { url: String, token: String },
}

fn is_socket(path: &Path) -> bool {
    path.symlink_metadata().is_ok_and(|m| m.file_type().is_socket())
}

/// @cc [owner:ghuntley,label:security] cli-local-before-token
/// Without `--url`, CLI commands MUST prefer the local monitor socket (for `top`) and otherwise
/// use `GEOCLASS_UI_TOKEN`, config, or the local store's minted token against the local ingress.
/// With `--url`, the token MUST come from `GEOCLASS_UI_TOKEN` so a stored token is never sent to
/// a remote host.
pub fn admin_http_target(url_override: Option<String>) -> Result<(String, String), String> {
    let config = crate::config::Config::load(Some("cli".into())).map_err(|e| e.to_string())?;
    let url = match &url_override {
        Some(url) => url.trim_end_matches('/').to_string(),
        None => format!("http://{}", config.ingress_bind.replace("0.0.0.0", "127.0.0.1")),
    };
    let parsed = reqwest::Url::parse(&url).map_err(|_| "invalid server URL")?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() || !matches!(parsed.path(), "" | "/") {
        return Err("server URL must be an HTTP(S) origin".into());
    }
    let env_token = std::env::var("GEOCLASS_UI_TOKEN").ok().filter(|s| !s.is_empty());
    let token = if url_override.is_some() {
        env_token
    } else {
        env_token
            .or(config.ui_token.clone())
            .or_else(|| crate::store::Store::open(&config.db_path()).ok().and_then(|s| s.config_get("ui_token")))
    }
    .ok_or("admin token unavailable; set GEOCLASS_UI_TOKEN")?;
    Ok((url, token))
}

fn target(url_override: Option<String>) -> Result<Target, String> {
    if url_override.is_none() {
        let config = crate::config::Config::load(Some("cli".into())).map_err(|e| e.to_string())?;
        for path in [config.monitor_socket_path(), PathBuf::from(crate::config::SYSTEM_MONITOR_SOCKET)] {
            if is_socket(&path) {
                return Ok(Target::Socket(path));
            }
        }
    }
    let (url, token) = admin_http_target(url_override)?;
    Ok(Target::Http { url, token })
}

/// @cc [owner:ghuntley,label:cli] top-observes-only
/// `run` MUST obtain data only through GET requests to the monitor socket or the authenticated
/// admin API and MUST restore terminal state on every exit.
pub fn run(url_override: Option<String>) -> Result<(), String> {
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        return Err("geoclass top requires an interactive terminal".into());
    }
    let target = target(url_override)?;
    let mut builder = reqwest::blocking::Client::builder().timeout(Duration::from_secs(2)).redirect(reqwest::redirect::Policy::none());
    let (endpoint, token) = match &target {
        Target::Socket(path) => {
            builder = builder.unix_socket(path.clone());
            ("http://localhost/monitor".to_string(), None)
        }
        Target::Http { url, token } => (format!("{url}/admin/api/monitor"), Some(token.clone())),
    };
    let client = builder.build().map_err(|e| format!("HTTP client: {e}"))?;
    let (tx, rx) = mpsc::channel::<Result<MonitorSnapshot, String>>();
    std::thread::spawn(move || {
        loop {
            let mut req = client.get(&endpoint);
            if let Some(token) = &token {
                req = req.bearer_auth(token);
            }
            let result = req
                .send()
                .map_err(|_| "server unavailable".to_string())
                .and_then(|r| if r.status().is_success() { r.json().map_err(|e| e.to_string()) } else { Err(format!("HTTP {}", r.status().as_u16())) });
            if tx.send(result).is_err() {
                return;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    let mut terminal = ratatui::init();
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            ratatui::restore();
        }
    }
    let _restore = Restore;
    let mut snapshot: Option<MonitorSnapshot> = None;
    let mut error: Option<String> = None;
    loop {
        while let Ok(update) = rx.try_recv() {
            match update {
                Ok(s) => {
                    snapshot = Some(s);
                    error = None;
                }
                Err(e) => error = Some(e),
            }
        }
        terminal.draw(|f| draw(f, snapshot.as_ref(), error.as_deref())).map_err(|e| e.to_string())?;
        if event::poll(Duration::from_millis(250)).map_err(|e| e.to_string())?
            && let Event::Key(key) = event::read().map_err(|e| e.to_string())?
                && key.kind == KeyEventKind::Press
                    && (matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL)))
                {
                    return Ok(());
                }
    }
}

pub fn draw(frame: &mut ratatui::Frame<'_>, snapshot: Option<&MonitorSnapshot>, error: Option<&str>) {
    let area = frame.area();
    let Some(s) = snapshot else {
        let msg = error.unwrap_or("connecting…");
        frame.render_widget(Paragraph::new(msg).block(panel(" geoclass ")), area);
        return;
    };
    let regions_height = (s.regions.len() as u16 + 3).min(12);
    let peers_height = (s.peers.len() as u16 + 3).clamp(3, 10);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(regions_height),
            Constraint::Length(4),
            Constraint::Length(peers_height),
            Constraint::Min(5),
        ])
        .split(area);
    let header = Line::from(vec![
        Span::styled(" geoclass ", Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
        Span::styled(
            format!(
                "{} · region {} · v{} · {} in flight · {} sessions bound{}",
                display(&s.node),
                display(&s.region),
                display(&s.version),
                s.inflight,
                s.bindings_total,
                error.map(|e| format!(" · {e}")).unwrap_or_default()
            ),
            Style::default().fg(MUTED),
        ),
    ]);
    frame.render_widget(Paragraph::new(header), chunks[0]);

    let rows = s.regions.iter().map(|r| {
        Row::new(vec![
            Cell::from(format!("{}{}", display(&r.name), if r.local { " *" } else { "" })),
            Cell::from(r.state.clone()).style(Style::default().fg(state_color(&r.state))),
            Cell::from(display(&r.domain)),
            Cell::from(if r.available { "yes" } else { "no" }),
            Cell::from(format!("{:.2}", r.weight)),
            Cell::from(r.attempts.to_string()),
            Cell::from(format!("{:.1}%", r.rate * 100.0)),
            Cell::from(format!("{}/{}", ms(r.ttft_p50_ms), ms(r.ttft_p95_ms))),
            Cell::from(r.bindings.to_string()),
            Cell::from(r.inflight.to_string()),
        ])
    });
    let table = Table::new(rows, [
        Constraint::Length(14),
        Constraint::Length(6),
        Constraint::Length(12),
        Constraint::Length(5),
        Constraint::Length(7),
        Constraint::Length(8),
        Constraint::Length(7),
        Constraint::Length(15),
        Constraint::Length(9),
        Constraint::Length(6),
    ])
    .header(Row::new(["region", "state", "domain", "up", "weight", "attempts", "fail", "ttft p50/p95", "sessions", "live"]).style(Style::default().fg(MUTED)))
    .block(panel(" regions (5 min window) "));
    frame.render_widget(table, chunks[1]);

    let retried: Vec<u64> = (0..60)
        .map(|i| s.minute_bins.values().map(|b| b.get(i).map(|x| x.1).unwrap_or(0)).sum::<i64>() as u64)
        .collect();
    let attempts: Vec<u64> = (0..60)
        .map(|i| s.minute_bins.values().map(|b| b.get(i).map(|x| x.0).unwrap_or(0)).sum::<i64>() as u64)
        .collect();
    let spark_chunks = Layout::default().direction(Direction::Horizontal).constraints([Constraint::Percentage(50), Constraint::Percentage(50)]).split(chunks[2]);
    frame.render_widget(Sparkline::default().data(&attempts).style(Style::default().fg(GREEN)).block(panel(" attempts / min ")), spark_chunks[0]);
    frame.render_widget(Sparkline::default().data(&retried).style(Style::default().fg(RED)).block(panel(" retried away / min ")), spark_chunks[1]);

    let peer_rows = s.peers.iter().map(|p| {
        Row::new(vec![
            Cell::from(display(p.node_id.as_deref().unwrap_or(&p.host))),
            Cell::from(display(p.region.as_deref().unwrap_or("–"))),
            Cell::from(if p.live { "live" } else { "stale" }).style(Style::default().fg(if p.live { GREEN } else { RED })),
            Cell::from(p.gossip_age_ms.map(|a| format!("{}s", a / 1000)).unwrap_or_else(|| "–".into())),
            Cell::from(p.inflight.to_string()),
            Cell::from(display(&p.url)),
        ])
    });
    let peers = Table::new(peer_rows, [Constraint::Length(18), Constraint::Length(12), Constraint::Length(6), Constraint::Length(8), Constraint::Length(6), Constraint::Min(10)])
        .header(Row::new(["node", "region", "", "gossip", "live", "url"]).style(Style::default().fg(MUTED)))
        .block(panel(" peers "));
    frame.render_widget(peers, chunks[3]);

    let recent = s.recent.iter().map(|a| {
        let color = match a.gate.as_str() {
            "retried" | "exhausted" | "abandoned" => RED,
            "passthrough" => YELLOW,
            _ => TEXT,
        };
        Row::new(vec![
            Cell::from(display(&a.request_id.chars().take(8).collect::<String>())),
            Cell::from(a.attempt_no.to_string()),
            Cell::from(display(&a.model)),
            Cell::from(display(&a.session)),
            Cell::from(display(&a.egress_region)),
            Cell::from(display(&a.reason)),
            Cell::from(a.status.to_string()),
            Cell::from(display(&a.gate)).style(Style::default().fg(color)),
            Cell::from(display(a.error_code.as_deref().unwrap_or(""))),
            Cell::from(ms(a.ttft_ms)),
        ])
    });
    let recent = Table::new(recent, [
        Constraint::Length(9),
        Constraint::Length(2),
        Constraint::Length(14),
        Constraint::Length(13),
        Constraint::Length(10),
        Constraint::Length(14),
        Constraint::Length(4),
        Constraint::Length(11),
        Constraint::Length(20),
        Constraint::Length(7),
    ])
    .header(Row::new(["request", "#", "model", "session", "egress", "reason", "http", "gate", "code", "ttft"]).style(Style::default().fg(MUTED)))
    .block(panel(" recent attempts · q quits "));
    frame.render_widget(recent, chunks[4]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::{MonitorAttempt, MonitorRegion};
    use ratatui::backend::TestBackend;

    #[test]
    fn renders_at_small_and_standard_sizes() {
        let snapshot = MonitorSnapshot {
            node: "n1".into(),
            region: "a".into(),
            regions: vec![MonitorRegion { name: "a".into(), state: "red".into(), domain: "a".into(), local: true, ..Default::default() }],
            recent: vec![MonitorAttempt { request_id: "0123456789".into(), gate: "retried".into(), ..Default::default() }],
            ..Default::default()
        };
        for (w, h) in [(120, 40), (60, 16)] {
            let mut terminal = ratatui::Terminal::new(TestBackend::new(w, h)).unwrap();
            terminal.draw(|f| draw(f, Some(&snapshot), None)).unwrap();
            terminal.draw(|f| draw(f, None, Some("server unavailable"))).unwrap();
        }
        assert_eq!(display("a\x1b[2Jb"), "a[2Jb");
    }
}
