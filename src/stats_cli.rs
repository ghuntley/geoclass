//! `geoclass stats`: print routing, rescue, cache, and capacity statistics from a running node.
use crate::stats::{self, DAY_MS, Totals};
use crate::stats_api::StatsResponse;
use clap::Args;
use std::time::Duration;

#[derive(Args, Debug)]
pub struct StatsArgs {
    #[arg(long, default_value = "24h", help = "window, e.g. 6h, 7d, 90d, 2y")]
    pub since: String,
    #[arg(long, help = "minute, hour, or day (default: hour up to 2 days, else day)")]
    pub bucket: Option<String>,
    #[arg(long, default_value = "total", help = "total, region, ingress, model, region_model, account, or domain")]
    pub by: String,
    #[arg(long, default_value = "cluster", help = "cluster (every live node) or local (this node)")]
    pub scope: String,
    #[arg(long, help = "print the raw JSON response")]
    pub json: bool,
    #[arg(long, help = "server URL (requires GEOCLASS_UI_TOKEN)")]
    pub url: Option<String>,
}

fn pct(v: Option<f64>) -> String {
    v.map(|v| format!("{:.1}%", v * 100.0)).unwrap_or_else(|| "–".into())
}

fn ms(v: Option<i64>) -> String {
    v.map(|v| format!("{v}ms")).unwrap_or_else(|| "–".into())
}

fn events(t: &Totals) -> String {
    ["region_red", "session_moved", "all_regions_overloaded", "peer_lost"]
        .iter()
        .filter_map(|k| t.events.get(*k).map(|v| format!("{k}={v}")))
        .collect::<Vec<_>>()
        .join(" ")
}

fn row(label: &str, t: &Totals) -> String {
    format!(
        "{:<20} {:>8} {:>8} {:>7} {:>7} {:>7} {:>7} {:>12} {:>10} {:>8} {:>5} {:>6}  {}",
        label,
        t.requests,
        t.attempts,
        t.retried,
        t.rescued,
        pct(t.rescue_rate()),
        pct(t.cache_hit_ratio()),
        t.input_tokens,
        t.output_tokens,
        ms(t.mean_ttft_ms()),
        t.peak_concurrency,
        format!("{:.2}", t.mean_concurrency_milli as f64 / 1000.0),
        events(t)
    )
}

const HEADER: &str = "                     requests attempts retried rescued  rescue   cache    tokens in tokens out     ttft  peak   mean  events";

/// Renders a response as the plain-text report.
pub fn render(r: &StatsResponse) -> String {
    let mut out = String::new();
    let fmt_ts = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).map(|d| d.format("%Y-%m-%d %H:%M").to_string()).unwrap_or_default();
    out.push_str(&format!("{} → {} UTC · {} buckets · scope {} · {} node(s)\n", fmt_ts(r.from_ms), fmt_ts(r.to_ms), r.bucket, r.scope, r.nodes.len()));
    for n in r.nodes.iter().filter(|n| !n.ok) {
        out.push_str(&format!("  unreachable: {} ({}): {}\n", n.node, n.region, n.error.as_deref().unwrap_or("")));
    }
    out.push_str(HEADER);
    out.push('\n');
    for b in &r.buckets {
        if b.totals.attempts > 0 || !b.totals.events.is_empty() {
            out.push_str(&row(&fmt_ts(b.start_ms), &b.totals));
            out.push('\n');
        }
    }
    out.push_str(&row("total", &r.totals));
    out.push('\n');
    out.push_str(&format!(
        "cache hit: stayed {} · moved {}   gate hold mean {}   gates {:?}\n",
        pct(r.totals.stayed_cache_hit_ratio()),
        pct(r.totals.moved_cache_hit_ratio()),
        ms(r.totals.mean_held_ms()),
        r.totals.gates
    ));
    if !r.series.is_empty() {
        out.push_str(&format!("\nby {}\n{HEADER}\n", r.by.as_str()));
        // Node-level events (startup, peers) have no region/model; they are already in the total.
        for s in r.series.iter().filter(|s| !(s.key == "(none)" && s.totals.attempts == 0)) {
            out.push_str(&row(&s.label.chars().take(20).collect::<String>(), &s.totals));
            out.push('\n');
        }
    }
    if !r.relay.is_empty() {
        out.push_str("\nrelayed for other regions\n");
        for x in &r.relay {
            out.push_str(&format!(
                "  from {:<14} requests {:>8} errors {:>6} mean {:>8} to-headers {:>8}\n",
                x.from_region,
                x.requests,
                x.errors,
                ms(Some(x.mean_duration_ms)),
                ms(x.mean_headers_ms)
            ));
        }
    }
    if !r.distinct_exact {
        out.push_str("\nrequests and sessions are summed across nodes (upper bounds)\n");
    }
    out
}

/// @cc [owner:ghuntley,label:cli] stats-cli-via-admin-api
/// `geoclass stats` MUST act only through a running node's authenticated admin API.
pub fn run(args: &StatsArgs) -> Result<(), String> {
    let span = stats::parse_since(&args.since).ok_or("--since must look like 24h, 90d, or 2y")?;
    let bucket = args.bucket.clone().unwrap_or_else(|| if span <= 2 * DAY_MS { "hour".into() } else { "day".into() });
    let (url, token) = crate::top::admin_http_target(args.url.clone())?;
    let endpoint = reqwest::Url::parse_with_params(
        &format!("{url}/admin/api/stats"),
        [("since", args.since.as_str()), ("bucket", bucket.as_str()), ("by", args.by.as_str()), ("scope", args.scope.as_str())],
    )
    .map_err(|e| format!("stats url: {e}"))?;
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let response = client.get(endpoint).bearer_auth(token).send().map_err(|_| "server unavailable".to_string())?;
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body: serde_json::Value = response.json().unwrap_or_default();
        return Err(format!("{} (HTTP {status})", body["message"].as_str().unwrap_or("request failed")));
    }
    let text = response.text().map_err(|e| e.to_string())?;
    if args.json {
        println!("{text}");
        return Ok(());
    }
    let parsed: StatsResponse = serde_json::from_str(&text).map_err(|e| format!("invalid response: {e}"))?;
    print!("{}", render(&parsed));
    Ok(())
}
