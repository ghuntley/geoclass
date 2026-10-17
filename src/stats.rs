//! Pure aggregation for long-range statistics (adapted from underclass `stats.rs`).
//!
//! The store runs the SQL aggregates (counts, tallies, attempt intervals); this module turns them
//! into per-bucket series, sweeps attempt intervals for concurrency, and merges finished-day
//! rollups and other nodes' answers. Synchronous; time is plain `i64` milliseconds.
use crate::hist::Hist;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const MINUTE_MS: i64 = 60_000;
pub const HOUR_MS: i64 = 60 * MINUTE_MS;
pub const DAY_MS: i64 = 24 * HOUR_MS;
/// Longest attempt the stats look back for when counting concurrency, and how long after a UTC
/// day ends before its rollup is final (rows are written when an attempt finishes).
pub const MAX_ATTEMPT_MS: i64 = 6 * HOUR_MS;
pub const DEFAULT_RETENTION_DAYS: i64 = 730;
/// Key of the single series for `Dim::Total`.
pub const TOTAL_KEY: &str = "*";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dim {
    Total,
    /// Egress region.
    Region,
    /// Ingress region (where the request entered the mesh).
    Ingress,
    Model,
    /// Egress region and model.
    RegionModel,
    /// SHA-256 of the ChatGPT account id.
    Account,
    /// Failure domain of the egress region at the time of the attempt.
    Domain,
}

impl Dim {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Region => "region",
            Self::Ingress => "ingress",
            Self::Model => "model",
            Self::RegionModel => "region_model",
            Self::Account => "account",
            Self::Domain => "domain",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::all().into_iter().find(|d| d.as_str() == value || (value == "region-model" && *d == Self::RegionModel))
    }

    pub fn all() -> [Self; 7] {
        [Self::Total, Self::Region, Self::Ingress, Self::Model, Self::RegionModel, Self::Account, Self::Domain]
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    /// Distinct client requests (failover attempts share a request id).
    pub requests: i64,
    /// Upstream attempts.
    pub attempts: i64,
    /// Distinct prompt-cache keys.
    pub sessions: i64,
    pub input_tokens: i64,
    #[serde(default)]
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    /// Input and cached input tokens of attempts whose session had just moved region; their
    /// cache-hit ratio is what a move costs.
    #[serde(default)]
    pub moved_input_tokens: i64,
    #[serde(default)]
    pub moved_cached_input_tokens: i64,
    /// Successful attempts whose token usage was not reported.
    pub unmeasured: i64,
    /// Attempts discarded by the gate and retried elsewhere.
    #[serde(default)]
    pub retried: i64,
    /// Requests whose first capacity failure was seen (each counted once, on that attempt).
    #[serde(default)]
    pub overload_hit: i64,
    /// Requests that hit a capacity failure and were then served by another region.
    #[serde(default)]
    pub rescued: i64,
    /// Time to first output and time held in the gate, summed over attempts that reported them.
    #[serde(default)]
    pub ttft_sum_ms: i64,
    #[serde(default)]
    pub ttft_n: i64,
    #[serde(default)]
    pub held_sum_ms: i64,
    #[serde(default)]
    pub held_n: i64,
    /// Time the relay hop added, summed over attempts sent through a peer.
    #[serde(default)]
    pub hop_sum_ms: i64,
    #[serde(default)]
    pub hop_n: i64,
    /// Distributions of time to first output, gate hold, and relay hop.
    #[serde(default)]
    pub ttft_hist: Hist,
    #[serde(default)]
    pub held_hist: Hist,
    #[serde(default)]
    pub hop_hist: Hist,
    /// Most upstream attempts in flight at once.
    pub peak_concurrency: i64,
    /// Time-weighted mean of attempts in flight, times 1000.
    pub mean_concurrency_milli: i64,
    /// Attempt outcomes: stream outcomes (`completed`, `failed`, ...), `http_<status>`,
    /// `network_error`, or `unknown` for rows recorded before outcomes existed.
    #[serde(default)]
    pub outcomes: BTreeMap<String, i64>,
    /// Events by kind (`region_red`, `session_moved`, `peer_lost`, ...).
    #[serde(default)]
    pub events: BTreeMap<String, i64>,
    /// Gate results (`committed`, `retried`, `overflow`, `passthrough`, ...).
    #[serde(default)]
    pub gates: BTreeMap<String, i64>,
    /// Placement reasons (`bound`, `new`, `moved_red`, ...).
    #[serde(default)]
    pub reasons: BTreeMap<String, i64>,
}

impl Totals {
    /// Rescued ÷ overload-hit requests, when any request hit a capacity failure.
    pub fn rescue_rate(&self) -> Option<f64> {
        (self.overload_hit > 0).then(|| self.rescued as f64 / self.overload_hit as f64)
    }

    /// Cached ÷ total input tokens.
    pub fn cache_hit_ratio(&self) -> Option<f64> {
        (self.input_tokens > 0).then(|| self.cached_input_tokens as f64 / self.input_tokens as f64)
    }

    /// Cache-hit ratio for attempts right after a move.
    pub fn moved_cache_hit_ratio(&self) -> Option<f64> {
        (self.moved_input_tokens > 0).then(|| self.moved_cached_input_tokens as f64 / self.moved_input_tokens as f64)
    }

    /// Cache-hit ratio for every other attempt.
    pub fn stayed_cache_hit_ratio(&self) -> Option<f64> {
        let input = self.input_tokens - self.moved_input_tokens;
        (input > 0).then(|| (self.cached_input_tokens - self.moved_cached_input_tokens) as f64 / input as f64)
    }

    pub fn mean_ttft_ms(&self) -> Option<i64> {
        (self.ttft_n > 0).then(|| self.ttft_sum_ms / self.ttft_n)
    }

    pub fn mean_held_ms(&self) -> Option<i64> {
        (self.held_n > 0).then(|| self.held_sum_ms / self.held_n)
    }

    pub fn mean_hop_ms(&self) -> Option<i64> {
        (self.hop_n > 0).then(|| self.hop_sum_ms / self.hop_n)
    }
}

/// One SQL aggregate row for a bucket index and series key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CountRow {
    pub bucket: i64,
    pub key: String,
    pub label: String,
    pub requests: i64,
    pub attempts: i64,
    pub sessions: i64,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    pub moved_input_tokens: i64,
    pub moved_cached_input_tokens: i64,
    pub unmeasured: i64,
    pub retried: i64,
    pub overload_hit: i64,
    pub rescued: i64,
    pub ttft_sum_ms: i64,
    pub ttft_n: i64,
    pub held_sum_ms: i64,
    pub held_n: i64,
    pub hop_sum_ms: i64,
    pub hop_n: i64,
}

/// One latency-histogram bucket count (`index` per `hist::bucket_of`) for a bucket index and
/// series key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistRow {
    pub bucket: i64,
    pub key: String,
    pub label: String,
    pub index: usize,
    pub count: i64,
}

/// A named count (an outcome or event kind) for a bucket index and series key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TallyRow {
    pub bucket: i64,
    pub key: String,
    pub label: String,
    pub name: String,
    pub count: i64,
}

/// One upstream attempt's lifetime, `[start, end)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interval {
    pub key: String,
    pub start: i64,
    pub end: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bucket {
    pub start_ms: i64,
    #[serde(flatten)]
    pub totals: Totals,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Series {
    pub key: String,
    pub label: String,
    pub buckets: Vec<Bucket>,
}

pub fn bucket_count(from: i64, to: i64, bucket_ms: i64) -> usize {
    if to <= from || bucket_ms <= 0 {
        return 0;
    }
    ((to - from + bucket_ms - 1) / bucket_ms) as usize
}

/// @cc [owner:ghuntley,label:accounting] concurrency-sweep
/// For each bucket `[from + i*bucket_ms, min(from + (i+1)*bucket_ms, to))`, `concurrency` MUST
/// report the maximum number of half-open intervals covering any instant in the bucket and the
/// time-weighted mean coverage times 1000 (floored); intervals outside the bucket MUST NOT count.
pub fn concurrency(intervals: &[(i64, i64)], from: i64, to: i64, bucket_ms: i64) -> Vec<(i64, i64)> {
    let count = bucket_count(from, to, bucket_ms);
    let mut points: Vec<(i64, i64)> = Vec::with_capacity(intervals.len() * 2);
    for &(start, end) in intervals {
        if end > start {
            points.push((start, 1));
            points.push((end, -1));
        }
    }
    // Half-open intervals: an attempt ending at t is gone before one starting at t arrives.
    points.sort_unstable();
    let mut out = Vec::with_capacity(count);
    let mut index = 0;
    let mut active: i64 = 0;
    for bucket in 0..count {
        let bucket_start = from + bucket as i64 * bucket_ms;
        let bucket_end = (bucket_start + bucket_ms).min(to);
        while index < points.len() && points[index].0 <= bucket_start {
            active += points[index].1;
            index += 1;
        }
        let mut peak = active;
        let mut area: i128 = 0;
        let mut previous = bucket_start;
        while index < points.len() && points[index].0 < bucket_end {
            let (at, delta) = points[index];
            area += i128::from(active) * i128::from(at - previous);
            previous = at;
            active += delta;
            peak = peak.max(active);
            index += 1;
        }
        area += i128::from(active) * i128::from(bucket_end - previous);
        let mean_milli = (area * 1000 / i128::from(bucket_end - bucket_start)) as i64;
        out.push((peak, mean_milli));
    }
    out
}

/// @cc [owner:ghuntley,label:accounting] stats-assemble-partition
/// `assemble` MUST place every count, tally, histogram row, and interval only in the series named by its key and
/// in the bucket it reports, MUST emit one bucket per `bucket_count(from, to, bucket_ms)` for every
/// series that has any row, and MUST order series by key.
pub fn assemble(
    from: i64,
    to: i64,
    bucket_ms: i64,
    counts: &[CountRow],
    tallies: &Tallies,
    intervals: &[Interval],
) -> Vec<Series> {
    let count = bucket_count(from, to, bucket_ms);
    let mut series: BTreeMap<String, Series> = BTreeMap::new();
    for row in counts {
        if let Some(totals) = bucket_mut(&mut series, count, from, bucket_ms, &row.key, &row.label, row.bucket) {
            add_counts(totals, row);
        }
    }
    for (rows, pick) in [
        (&tallies.ttft, (|t: &mut Totals| &mut t.ttft_hist) as fn(&mut Totals) -> &mut Hist),
        (&tallies.held, |t: &mut Totals| &mut t.held_hist),
        (&tallies.hop, |t: &mut Totals| &mut t.hop_hist),
    ] {
        for row in rows {
            if let Some(totals) = bucket_mut(&mut series, count, from, bucket_ms, &row.key, &row.label, row.bucket)
                && let Some(slot) = pick(totals).0.get_mut(row.index) {
                    *slot += row.count;
                }
        }
    }
    for (rows, pick) in [
        (&tallies.outcomes, (|t: &mut Totals| &mut t.outcomes) as fn(&mut Totals) -> &mut BTreeMap<String, i64>),
        (&tallies.events, |t: &mut Totals| &mut t.events),
        (&tallies.gates, |t: &mut Totals| &mut t.gates),
        (&tallies.reasons, |t: &mut Totals| &mut t.reasons),
    ] {
        for row in rows {
            if let Some(totals) = bucket_mut(&mut series, count, from, bucket_ms, &row.key, &row.label, row.bucket) {
                *pick(totals).entry(row.name.clone()).or_default() += row.count;
            }
        }
    }
    let mut by_key: BTreeMap<&str, Vec<(i64, i64)>> = BTreeMap::new();
    for interval in intervals {
        by_key.entry(interval.key.as_str()).or_default().push((interval.start, interval.end));
    }
    for (key, spans) in by_key {
        let Some(series) = series.get_mut(key) else { continue };
        for (bucket, (peak, mean)) in series.buckets.iter_mut().zip(concurrency(&spans, from, to, bucket_ms)) {
            bucket.totals.peak_concurrency = peak;
            bucket.totals.mean_concurrency_milli = mean;
        }
    }
    series.into_values().collect()
}

/// Named-count and histogram rows for `assemble`.
#[derive(Clone, Debug, Default)]
pub struct Tallies {
    pub outcomes: Vec<TallyRow>,
    pub events: Vec<TallyRow>,
    pub gates: Vec<TallyRow>,
    pub reasons: Vec<TallyRow>,
    pub ttft: Vec<HistRow>,
    pub held: Vec<HistRow>,
    pub hop: Vec<HistRow>,
}

fn add_counts(totals: &mut Totals, row: &CountRow) {
    totals.requests += row.requests;
    totals.attempts += row.attempts;
    totals.sessions += row.sessions;
    totals.input_tokens += row.input_tokens;
    totals.cached_input_tokens += row.cached_input_tokens;
    totals.output_tokens += row.output_tokens;
    totals.moved_input_tokens += row.moved_input_tokens;
    totals.moved_cached_input_tokens += row.moved_cached_input_tokens;
    totals.unmeasured += row.unmeasured;
    totals.retried += row.retried;
    totals.overload_hit += row.overload_hit;
    totals.rescued += row.rescued;
    totals.ttft_sum_ms += row.ttft_sum_ms;
    totals.ttft_n += row.ttft_n;
    totals.held_sum_ms += row.held_sum_ms;
    totals.held_n += row.held_n;
    totals.hop_sum_ms += row.hop_sum_ms;
    totals.hop_n += row.hop_n;
}

fn add_maps(into: &mut BTreeMap<String, i64>, from: &BTreeMap<String, i64>) {
    for (name, count) in from {
        *into.entry(name.clone()).or_default() += count;
    }
}

fn bucket_mut<'a>(
    series: &'a mut BTreeMap<String, Series>,
    count: usize,
    from: i64,
    bucket_ms: i64,
    key: &str,
    label: &str,
    bucket: i64,
) -> Option<&'a mut Totals> {
    let index = usize::try_from(bucket).ok().filter(|i| *i < count)?;
    let series = series.entry(key.to_string()).or_insert_with(|| Series {
        key: key.to_string(),
        label: label.to_string(),
        buckets: (0..count)
            .map(|i| Bucket { start_ms: from + i as i64 * bucket_ms, totals: Totals::default() })
            .collect(),
    });
    if series.label.is_empty() || series.label == series.key {
        series.label = label.to_string();
    }
    Some(&mut series.buckets[index].totals)
}

/// @cc [owner:ghuntley,label:accounting] stats-combine
/// `combine` MUST sum every additive field, named count, and histogram bucket, take the maximum peak, and weight
/// mean concurrency by each part's span; a sum of distinct counts (requests, sessions) is an
/// upper bound that callers MUST replace with an exact distinct count when one is available.
pub fn combine(parts: &[(&Totals, i64)]) -> Totals {
    let mut out = Totals::default();
    let mut area: i128 = 0;
    let mut span: i128 = 0;
    for (totals, part_span) in parts {
        out.requests += totals.requests;
        out.attempts += totals.attempts;
        out.sessions += totals.sessions;
        out.input_tokens += totals.input_tokens;
        out.cached_input_tokens += totals.cached_input_tokens;
        out.output_tokens += totals.output_tokens;
        out.moved_input_tokens += totals.moved_input_tokens;
        out.moved_cached_input_tokens += totals.moved_cached_input_tokens;
        out.unmeasured += totals.unmeasured;
        out.retried += totals.retried;
        out.overload_hit += totals.overload_hit;
        out.rescued += totals.rescued;
        out.ttft_sum_ms += totals.ttft_sum_ms;
        out.ttft_n += totals.ttft_n;
        out.held_sum_ms += totals.held_sum_ms;
        out.held_n += totals.held_n;
        out.hop_sum_ms += totals.hop_sum_ms;
        out.hop_n += totals.hop_n;
        out.ttft_hist.add(&totals.ttft_hist);
        out.held_hist.add(&totals.held_hist);
        out.hop_hist.add(&totals.hop_hist);
        out.peak_concurrency = out.peak_concurrency.max(totals.peak_concurrency);
        add_maps(&mut out.outcomes, &totals.outcomes);
        add_maps(&mut out.events, &totals.events);
        add_maps(&mut out.gates, &totals.gates);
        add_maps(&mut out.reasons, &totals.reasons);
        area += i128::from(totals.mean_concurrency_milli) * i128::from(*part_span);
        span += i128::from(*part_span);
    }
    if span > 0 {
        out.mean_concurrency_milli = (area / span) as i64;
    }
    out
}

/// @cc [owner:ghuntley,label:accounting] cluster-merge
/// `merge_nodes` MUST combine the same bucket from every node with `combine` over equal spans,
/// except that peak and mean concurrency MUST be summed (nodes run attempts at the same time),
/// and MUST keep every series key that any node reported.
pub fn merge_nodes(nodes: &[Vec<Series>]) -> Vec<Series> {
    let mut merged: BTreeMap<String, Series> = BTreeMap::new();
    for series in nodes.iter().flatten() {
        match merged.get_mut(&series.key) {
            None => {
                merged.insert(series.key.clone(), series.clone());
            }
            Some(existing) => {
                for (into, from) in existing.buckets.iter_mut().zip(&series.buckets) {
                    let peak = into.totals.peak_concurrency + from.totals.peak_concurrency;
                    let mean = into.totals.mean_concurrency_milli + from.totals.mean_concurrency_milli;
                    into.totals = combine(&[(&into.totals, 1), (&from.totals, 1)]);
                    into.totals.peak_concurrency = peak;
                    into.totals.mean_concurrency_milli = mean;
                }
            }
        }
    }
    merged.into_values().collect()
}

/// Parses a window like `90m`, `24h`, `7d`, or `2y` into milliseconds.
pub fn parse_since(value: &str) -> Option<i64> {
    let value = value.trim();
    let split = value.find(|c: char| !c.is_ascii_digit())?;
    let amount: i64 = value[..split].parse().ok().filter(|n| *n > 0)?;
    let unit = match &value[split..] {
        "m" => MINUTE_MS,
        "h" => HOUR_MS,
        "d" => DAY_MS,
        "w" => 7 * DAY_MS,
        "y" => 365 * DAY_MS,
        _ => return None,
    };
    amount.checked_mul(unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrency_counts_overlap_per_bucket() {
        let spans = [(0, 10), (5, 15), (10, 20)];
        let buckets = concurrency(&spans, 0, 20, 10);
        assert_eq!(buckets[0], (2, 1500));
        assert_eq!(buckets[1], (2, 1500));
    }

    #[test]
    fn back_to_back_attempts_do_not_overlap() {
        assert_eq!(concurrency(&[(0, 5), (5, 10)], 0, 10, 10), vec![(1, 1000)]);
    }

    #[test]
    fn interval_started_before_window_counts() {
        assert_eq!(concurrency(&[(-100, 5)], 0, 10, 10), vec![(1, 500)]);
    }

    #[test]
    fn combine_weights_mean_by_span() {
        let a = Totals { mean_concurrency_milli: 1000, peak_concurrency: 2, attempts: 1, ..Default::default() };
        let b = Totals { mean_concurrency_milli: 4000, peak_concurrency: 5, attempts: 2, ..Default::default() };
        let out = combine(&[(&a, 3), (&b, 1)]);
        assert_eq!((out.attempts, out.peak_concurrency, out.mean_concurrency_milli), (3, 5, 1750));
    }

    #[test]
    fn since_units() {
        assert_eq!(parse_since("24h"), Some(DAY_MS));
        assert_eq!(parse_since("730d"), Some(730 * DAY_MS));
        assert_eq!(parse_since("2y"), Some(730 * DAY_MS));
        assert_eq!(parse_since("0d"), None);
        assert_eq!(parse_since("d"), None);
        assert_eq!(parse_since("5x"), None);
    }
}
