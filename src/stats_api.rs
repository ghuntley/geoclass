//! Long-range statistics: the local query, the cluster fan-out, and the retention/rollup job.
//! Adapted from underclass `stats_api.rs`.
use crate::stats::{self, Bucket, Dim, Series, Totals};
use crate::store::{RelaySummary, Store};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

const MAX_ROLLUPS_PER_PASS: usize = 800;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StatsParams {
    /// Window ending at `to_ms` (default now), e.g. `24h`, `90d`, `2y`. Ignored with `from_ms`.
    pub since: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    /// `minute`, `hour` (default), or `day`.
    pub bucket: Option<String>,
    /// `total` (default), `region`, `ingress`, `model`, `region_model`, `account`, or `domain`.
    pub by: Option<String>,
    /// `local` (default) or `cluster`.
    pub scope: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeriesOut {
    pub key: String,
    pub label: String,
    pub totals: Totals,
    pub buckets: Vec<Bucket>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node: String,
    pub region: String,
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatsResponse {
    pub from_ms: i64,
    pub to_ms: i64,
    pub bucket: String,
    pub bucket_ms: i64,
    pub by: Dim,
    pub scope: String,
    pub retention_days: i64,
    pub live_inflight: i64,
    pub totals: Totals,
    pub buckets: Vec<Bucket>,
    pub series: Vec<SeriesOut>,
    /// Requests this node relayed for others, by the requester's region.
    #[serde(default)]
    pub relay: Vec<RelaySummary>,
    /// Nodes that answered (cluster scope) or this node (local scope).
    #[serde(default)]
    pub nodes: Vec<NodeStatus>,
    /// False when request and session counts are sums over nodes (upper bounds).
    #[serde(default = "yes")]
    pub distinct_exact: bool,
}

fn yes() -> bool {
    true
}

/// @cc [owner:ghuntley,label:accounting] stats-query-bounds
/// `query` MUST reject windows longer than 2 days for minute buckets, 31 days for hour buckets,
/// and the retention period plus one day for day buckets; buckets MUST align to their size (day
/// buckets to UTC midnight), and day buckets MUST read stored rollups only for finished days,
/// computing unfinished days from raw rows. Window totals MUST use exact distinct request and
/// session counts.
pub fn query(store: &Store, params: &StatsParams, now: i64, live_inflight: i64, retention_days: i64) -> Result<StatsResponse, String> {
    let (from, to, bucket, bucket_ms, dim) = window(params, now, retention_days)?;
    let total = series(store, from, to, bucket_ms, Dim::Total, now).map_err(|e| e.to_string())?;
    let mut totals_out = window_totals(store, from, to, bucket_ms, Dim::Total, &total).map_err(|e| e.to_string())?;
    let (totals, buckets) = match totals_out.pop() {
        Some(series) => (series.totals, series.buckets),
        None => (Totals::default(), empty_buckets(from, to, bucket_ms)),
    };
    let series = if dim == Dim::Total {
        Vec::new()
    } else {
        let by = series(store, from, to, bucket_ms, dim, now).map_err(|e| e.to_string())?;
        window_totals(store, from, to, bucket_ms, dim, &by).map_err(|e| e.to_string())?
    };
    Ok(StatsResponse {
        from_ms: from,
        to_ms: to,
        bucket,
        bucket_ms,
        by: dim,
        scope: "local".into(),
        retention_days,
        live_inflight,
        totals,
        buckets,
        series,
        relay: store.relay_summary(from, to).map_err(|e| e.to_string())?,
        nodes: Vec::new(),
        distinct_exact: true,
    })
}

/// Resolves and validates the query window: `(from, to, bucket name, bucket ms, dim)`.
pub fn window(params: &StatsParams, now: i64, retention_days: i64) -> Result<(i64, i64, String, i64, Dim), String> {
    let bucket = params.bucket.as_deref().unwrap_or("hour");
    let (bucket_ms, max_window) = match bucket {
        "minute" => (stats::MINUTE_MS, 2 * stats::DAY_MS),
        "hour" => (stats::HOUR_MS, 31 * stats::DAY_MS),
        "day" => (stats::DAY_MS, (retention_days + 1) * stats::DAY_MS),
        _ => return Err("bucket must be minute, hour, or day".into()),
    };
    let dim = match params.by.as_deref() {
        None => Dim::Total,
        Some(by) => Dim::parse(by).ok_or("by must be total, region, ingress, model, region_model, account, or domain")?,
    };
    // Half-open window: `now + 1` keeps rows written in the current millisecond.
    let to = params.to_ms.unwrap_or(now + 1).min(now + 1);
    let mut from = match (params.from_ms, params.since.as_deref()) {
        (Some(from), _) => from,
        (None, since) => {
            let span = stats::parse_since(since.unwrap_or("24h")).ok_or("since must look like 24h, 90d, or 2y")?;
            to.saturating_sub(span)
        }
    };
    from = from.div_euclid(bucket_ms) * bucket_ms;
    if from >= to {
        return Err("window is empty".into());
    }
    if to - from > max_window + bucket_ms {
        return Err(format!(
            "window too long for {bucket} buckets (max {} days); use a coarser bucket",
            max_window / stats::DAY_MS
        ));
    }
    Ok((from, to, bucket.to_string(), bucket_ms, dim))
}

fn empty_buckets(from: i64, to: i64, bucket_ms: i64) -> Vec<Bucket> {
    (0..stats::bucket_count(from, to, bucket_ms))
        .map(|i| Bucket { start_ms: from + i as i64 * bucket_ms, totals: Totals::default() })
        .collect()
}

fn window_totals(store: &Store, from: i64, to: i64, bucket_ms: i64, dim: Dim, series: &[Series]) -> rusqlite::Result<Vec<SeriesOut>> {
    let distinct: HashMap<String, (i64, i64)> = store
        .stats_distinct(from, to, dim)?
        .into_iter()
        .map(|(key, requests, sessions)| (key, (requests, sessions)))
        .collect();
    Ok(series
        .iter()
        .map(|s| {
            let mut totals = combine_buckets(&s.buckets, bucket_ms, to);
            let (requests, sessions) = distinct.get(&s.key).copied().unwrap_or((0, 0));
            totals.requests = requests;
            totals.sessions = sessions;
            SeriesOut { key: s.key.clone(), label: s.label.clone(), totals, buckets: s.buckets.clone() }
        })
        .collect())
}

fn combine_buckets(buckets: &[Bucket], bucket_ms: i64, to: i64) -> Totals {
    let parts: Vec<(&Totals, i64)> = buckets.iter().map(|b| (&b.totals, (b.start_ms + bucket_ms).min(to) - b.start_ms)).collect();
    stats::combine(&parts)
}

/// The last UTC day (exclusive) whose rollup is final at `now`.
fn final_day_end(now: i64) -> i64 {
    (now - stats::MAX_ATTEMPT_MS).div_euclid(stats::DAY_MS)
}

fn series(store: &Store, from: i64, to: i64, bucket_ms: i64, dim: Dim, now: i64) -> rusqlite::Result<Vec<Series>> {
    if bucket_ms != stats::DAY_MS {
        return store.stats_series(from, to, bucket_ms, dim);
    }
    let first_day = from.div_euclid(stats::DAY_MS);
    let end_day = (to + stats::DAY_MS - 1).div_euclid(stats::DAY_MS);
    let final_end = final_day_end(now).clamp(first_day, end_day);
    let rolled = store.stats_rolled_days(first_day, final_end)?;
    for day in first_day..final_end {
        if !rolled.contains(&day) {
            store.rollup_stats_day(day, now)?;
        }
    }
    let count = (end_day - first_day) as usize;
    let mut merged: BTreeMap<String, Series> = BTreeMap::new();
    let mut place = |key: String, label: String, day: i64, totals: Totals| {
        let series = merged.entry(key.clone()).or_insert_with(|| Series {
            key,
            label: label.clone(),
            buckets: empty_buckets(from, to, stats::DAY_MS),
        });
        series.label = label;
        if let Some(bucket) = usize::try_from(day - first_day).ok().filter(|i| *i < count) {
            series.buckets[bucket].totals = totals;
        }
    };
    for (day, key, label, totals) in store.stats_daily(first_day, final_end, dim)? {
        if dim == Dim::Total || totals != Totals::default() {
            place(key, label, day, totals);
        }
    }
    for day in final_end..end_day {
        let day_from = day * stats::DAY_MS;
        let day_to = (day_from + stats::DAY_MS).min(to);
        for live in store.stats_series(day_from, day_to, stats::DAY_MS, dim)? {
            if let Some(bucket) = live.buckets.into_iter().next() {
                place(live.key, live.label, day, bucket.totals);
            }
        }
    }
    Ok(merged.into_values().collect())
}

/// @cc [owner:ghuntley,label:accounting] cluster-stats-merge
/// `merge_cluster` MUST merge only responses for the same window and bucket, MUST combine the
/// total buckets and every series with `stats::merge_nodes`, MUST recompute window totals from
/// the merged buckets, and MUST mark the result `distinct_exact = false` when more than one node
/// contributed (summed distinct counts are upper bounds).
pub fn merge_cluster(parts: Vec<StatsResponse>, nodes: Vec<NodeStatus>) -> Option<StatsResponse> {
    let first = parts.first()?.clone();
    let same: Vec<StatsResponse> = parts
        .into_iter()
        .filter(|p| p.from_ms == first.from_ms && p.to_ms == first.to_ms && p.bucket_ms == first.bucket_ms && p.by == first.by)
        .collect();
    let as_series = |p: &StatsResponse| vec![Series { key: stats::TOTAL_KEY.into(), label: stats::TOTAL_KEY.into(), buckets: p.buckets.clone() }];
    let totals_series = stats::merge_nodes(&same.iter().map(as_series).collect::<Vec<_>>());
    let buckets = totals_series.into_iter().next().map(|s| s.buckets).unwrap_or_default();
    let by_series = stats::merge_nodes(
        &same
            .iter()
            .map(|p| p.series.iter().map(|s| Series { key: s.key.clone(), label: s.label.clone(), buckets: s.buckets.clone() }).collect())
            .collect::<Vec<_>>(),
    );
    let exact = same.len() <= 1;
    let mut totals = combine_buckets(&buckets, first.bucket_ms, first.to_ms);
    if exact {
        totals.requests = first.totals.requests;
        totals.sessions = first.totals.sessions;
    } else {
        totals.requests = same.iter().map(|p| p.totals.requests).sum();
        totals.sessions = same.iter().map(|p| p.totals.sessions).sum();
    }
    let series = by_series
        .into_iter()
        .map(|s| {
            let mut t = combine_buckets(&s.buckets, first.bucket_ms, first.to_ms);
            t.requests = same.iter().flat_map(|p| &p.series).filter(|o| o.key == s.key).map(|o| o.totals.requests).sum();
            t.sessions = same.iter().flat_map(|p| &p.series).filter(|o| o.key == s.key).map(|o| o.totals.sessions).sum();
            SeriesOut { key: s.key, label: s.label, totals: t, buckets: s.buckets }
        })
        .collect();
    let mut relay: BTreeMap<String, RelaySummary> = BTreeMap::new();
    for r in same.iter().flat_map(|p| &p.relay) {
        let entry = relay.entry(r.from_region.clone()).or_insert_with(|| RelaySummary { from_region: r.from_region.clone(), ..Default::default() });
        let n = entry.requests + r.requests;
        if n > 0 {
            entry.mean_duration_ms = (entry.mean_duration_ms * entry.requests + r.mean_duration_ms * r.requests) / n;
        }
        entry.requests = n;
        entry.errors += r.errors;
        entry.bytes += r.bytes;
        entry.mean_headers_ms = entry.mean_headers_ms.or(r.mean_headers_ms);
    }
    Some(StatsResponse {
        scope: "cluster".into(),
        live_inflight: same.iter().map(|p| p.live_inflight).sum(),
        totals,
        buckets,
        series,
        relay: relay.into_values().collect(),
        nodes,
        distinct_exact: exact,
        ..first
    })
}

/// @cc [owner:ghuntley,label:persistence] stats-maintenance
/// `maintain` MUST prune everything older than `retention_days` and then roll up every finished
/// UTC day inside the retention window that has data and no stored rollup (bounded per pass).
pub fn maintain(store: &Store, now: i64, retention_days: i64) -> rusqlite::Result<(usize, usize)> {
    let cutoff = now - retention_days * stats::DAY_MS;
    let pruned = store.prune_stats(cutoff)?;
    let Some(earliest) = store.stats_earliest_ts()? else {
        return Ok((pruned, 0));
    };
    let first_day = earliest.max(cutoff).div_euclid(stats::DAY_MS);
    let end = final_day_end(now);
    if first_day >= end {
        return Ok((pruned, 0));
    }
    let rolled = store.stats_rolled_days(first_day, end)?;
    let mut done = 0;
    for day in (first_day..end).filter(|d| !rolled.contains(d)).take(MAX_ROLLUPS_PER_PASS) {
        store.rollup_stats_day(day, now)?;
        done += 1;
    }
    Ok((pruned, done))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{AttemptRecord, Event};

    fn row(request: &str, ts: i64, region: &str, gate: &str) -> AttemptRecord {
        AttemptRecord {
            request_id: request.into(), ts, duration_ms: Some(5000), endpoint: "/r".into(), model: "m".into(),
            session: Some(format!("s-{request}")), ingress_region: "a".into(), egress_region: region.into(),
            egress_node: "n".into(), failure_domain: region.into(), placement_reason: "new".into(), status: 200,
            gate_result: gate.into(), input_tokens: Some(10), output_tokens: Some(2), ..Default::default()
        }
    }

    #[test]
    fn day_query_mixes_rollups_and_live_days() {
        let store = Store::in_memory().unwrap();
        let day = stats::DAY_MS;
        store.insert_attempt(&row("a", day + 1000, "x", "output")).unwrap();
        store.insert_attempt(&row("b", day + 2000, "y", "retried")).unwrap();
        store.insert_attempt(&row("c", 3 * day + 10, "x", "output")).unwrap();
        store.insert_event(&Event { ts: day + 2500, kind: "region_red".into(), region: Some("y".into()), ..Default::default() });
        let now = 3 * day + 100;
        let params = StatsParams { since: Some("3d".into()), bucket: Some("day".into()), by: Some("region".into()), ..Default::default() };
        let response = query(&store, &params, now, 0, 730).unwrap();
        assert_eq!(response.buckets.len(), 4);
        assert_eq!(response.totals.requests, 3);
        assert_eq!(response.totals.peak_concurrency, 2);
        assert_eq!(response.totals.events.get("region_red"), Some(&1));
        assert_eq!(response.totals.gates.get("retried"), Some(&1));
        assert!(store.stats_rolled_days(1, 2).unwrap().contains(&1));
        let again = query(&store, &params, now, 0, 730).unwrap();
        assert_eq!(again.totals, response.totals);
        let y = response.series.iter().find(|s| s.key == "y").unwrap();
        assert_eq!((y.totals.attempts, y.totals.events.get("region_red")), (1, Some(&1)));
    }

    #[test]
    fn query_rejects_oversized_windows() {
        let store = Store::in_memory().unwrap();
        let params = StatsParams { since: Some("40d".into()), bucket: Some("hour".into()), ..Default::default() };
        assert!(query(&store, &params, 100 * stats::DAY_MS, 0, 730).is_err());
        let params = StatsParams { by: Some("nope".into()), ..Default::default() };
        assert!(query(&store, &params, 100 * stats::DAY_MS, 0, 730).is_err());
    }

    #[test]
    fn cluster_merge_sums_nodes() {
        let now = 10 * stats::HOUR_MS;
        let params = StatsParams { since: Some("2h".into()), by: Some("region".into()), ..Default::default() };
        let a = Store::in_memory().unwrap();
        a.insert_attempt(&row("r1", now - 1000, "x", "output")).unwrap();
        let b = Store::in_memory().unwrap();
        b.insert_attempt(&row("r2", now - 2000, "y", "output")).unwrap();
        b.insert_attempt(&row("r3", now - 3000, "x", "output")).unwrap();
        let parts = vec![query(&a, &params, now, 1, 730).unwrap(), query(&b, &params, now, 2, 730).unwrap()];
        let merged = merge_cluster(parts, vec![]).unwrap();
        assert_eq!(merged.totals.attempts, 3);
        assert_eq!(merged.totals.requests, 3);
        assert_eq!(merged.live_inflight, 3);
        assert!(!merged.distinct_exact);
        assert_eq!(merged.series.iter().find(|s| s.key == "x").unwrap().totals.attempts, 2);
        assert_eq!(merged.scope, "cluster");
    }

    #[test]
    fn maintain_prunes_and_rolls_up() {
        let store = Store::in_memory().unwrap();
        let day = stats::DAY_MS;
        store.insert_attempt(&row("old", 10, "x", "output")).unwrap();
        store.insert_attempt(&row("new", 5 * day, "x", "output")).unwrap();
        let (pruned, rolled) = maintain(&store, 7 * day, 3).unwrap();
        assert_eq!((pruned, rolled), (1, 1));
        assert_eq!(maintain(&store, 7 * day, 3).unwrap(), (0, 0));
    }
}
