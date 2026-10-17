//! Append-only SQLite statistics store (one file per node).
//!
//! Routing never reads this store: placement uses only gossip. Losing the file loses history, not
//! routing state. Schema and rollups follow underclass ADR 0021.
use crate::stats::{self, CountRow, Dim, Interval, Series, Tallies, TallyRow, Totals};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

/// One upstream attempt as seen by the ingress node.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub request_id: String,
    pub attempt_no: i64,
    /// Attempt start; `[ts, ts + duration_ms)` is its lifetime.
    pub ts: i64,
    pub duration_ms: Option<i64>,
    pub endpoint: String,
    pub model: String,
    /// SHA-256 of the prompt cache key.
    pub session: Option<String>,
    /// SHA-256 of the ChatGPT account id.
    pub account: Option<String>,
    pub ingress_region: String,
    pub egress_region: String,
    pub egress_node: String,
    pub failure_domain: String,
    pub cf_colo: Option<String>,
    pub placement_reason: String,
    /// HTTP status from upstream; 0 when no response arrived.
    pub status: u16,
    /// `output`, `terminal`, `overflow`, `eof` (committed), `retried`, `passthrough`
    /// (non-retryable error returned as-is), `exhausted` (every region failed; this failure was
    /// returned), or `abandoned` (retry deadline passed).
    pub gate_result: String,
    pub stream_outcome: Option<String>,
    pub error_code: Option<String>,
    pub saw_output: Option<bool>,
    pub ttft_ms: Option<i64>,
    pub held_ms: Option<i64>,
    pub input_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    /// 1 on the first attempt of a request that hit a capacity failure.
    pub overload_hit: bool,
    /// 1 on the committed attempt of a request that earlier hit a capacity failure.
    pub rescued: bool,
}

/// One request this node egressed on behalf of another node.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayRecord {
    pub ts: i64,
    pub request_id: String,
    pub from_node: String,
    pub from_region: String,
    pub status: u16,
    pub duration_ms: i64,
    pub bytes: i64,
    pub headers_ms: Option<i64>,
    pub cf_colo: Option<String>,
    pub error: Option<String>,
}

pub const EVENT_KINDS: [&str; 13] = [
    "region_red",
    "region_amber",
    "region_green",
    "session_moved",
    "geo_retry_succeeded",
    "all_regions_overloaded",
    "retry_deadline",
    "peer_joined",
    "peer_lost",
    "gossip_stale",
    "prior_shift",
    "domain_changed",
    "node_started",
];

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub ts: i64,
    pub kind: String,
    pub region: Option<String>,
    pub model: Option<String>,
    pub domain: Option<String>,
    pub from_region: Option<String>,
    pub to_region: Option<String>,
    /// Node that recorded the event, and its region.
    pub node: Option<String>,
    pub node_region: Option<String>,
    pub request_id: Option<String>,
    pub detail: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HealthSample {
    /// Minute start (ms).
    pub ts: i64,
    pub region: String,
    pub model: String,
    pub domain: String,
    pub attempts: i64,
    pub overloaded: i64,
    pub errors: i64,
    pub state: String,
    pub weight: f64,
    pub ttft_mean_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RelaySummary {
    pub from_region: String,
    pub requests: i64,
    pub errors: i64,
    pub mean_duration_ms: i64,
    pub mean_headers_ms: Option<i64>,
    pub bytes: i64,
}

fn schema() -> &'static str {
    r#"
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT NOT NULL,
        attempt_no INTEGER NOT NULL,
        ts INTEGER NOT NULL,
        duration_ms INTEGER,
        endpoint TEXT NOT NULL,
        model TEXT NOT NULL,
        session TEXT,
        account TEXT,
        ingress_region TEXT NOT NULL,
        egress_region TEXT NOT NULL,
        egress_node TEXT NOT NULL,
        failure_domain TEXT NOT NULL,
        cf_colo TEXT,
        placement_reason TEXT NOT NULL,
        status INTEGER NOT NULL,
        gate_result TEXT NOT NULL,
        stream_outcome TEXT,
        error_code TEXT,
        saw_output INTEGER,
        ttft_ms INTEGER,
        held_ms INTEGER,
        input_tokens INTEGER,
        cached_input_tokens INTEGER,
        output_tokens INTEGER,
        overload_hit INTEGER NOT NULL DEFAULT 0,
        rescued INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS attempts_ts ON attempts(ts, id);
    CREATE INDEX IF NOT EXISTS attempts_dims ON attempts(egress_region, model, ts);
    CREATE TABLE IF NOT EXISTS relay_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        request_id TEXT NOT NULL,
        from_node TEXT NOT NULL,
        from_region TEXT NOT NULL,
        status INTEGER NOT NULL,
        duration_ms INTEGER NOT NULL,
        bytes INTEGER NOT NULL,
        headers_ms INTEGER,
        cf_colo TEXT,
        error TEXT
    );
    CREATE INDEX IF NOT EXISTS relay_attempts_ts ON relay_attempts(ts);
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        kind TEXT NOT NULL,
        region TEXT,
        model TEXT,
        domain TEXT,
        from_region TEXT,
        to_region TEXT,
        node TEXT,
        node_region TEXT,
        request_id TEXT,
        detail TEXT,
        error_code TEXT
    );
    CREATE INDEX IF NOT EXISTS events_ts ON events(ts);
    CREATE TABLE IF NOT EXISTS health_samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        region TEXT NOT NULL,
        model TEXT NOT NULL,
        domain TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        overloaded INTEGER NOT NULL,
        errors INTEGER NOT NULL,
        state TEXT NOT NULL,
        weight REAL NOT NULL,
        ttft_mean_ms INTEGER,
        UNIQUE (ts, region, model)
    );
    CREATE INDEX IF NOT EXISTS health_samples_ts ON health_samples(ts);
    CREATE TABLE IF NOT EXISTS stats_daily (
        day INTEGER NOT NULL,
        dim TEXT NOT NULL,
        key TEXT NOT NULL,
        label TEXT NOT NULL,
        totals TEXT NOT NULL,
        computed_at INTEGER NOT NULL,
        PRIMARY KEY (day, dim, key)
    );
    "#
}

fn attempt_dim_sql(dim: Dim) -> (&'static str, &'static str) {
    match dim {
        Dim::Total => ("'*'", "'*'"),
        Dim::Region => ("egress_region", "egress_region"),
        Dim::Ingress => ("ingress_region", "ingress_region"),
        Dim::Model => ("model", "model"),
        Dim::RegionModel => ("egress_region || ' / ' || model", "egress_region || ' / ' || model"),
        Dim::Account => ("COALESCE(account,'(none)')", "COALESCE(substr(account,1,12),'(none)')"),
        Dim::Domain => ("failure_domain", "failure_domain"),
    }
}

fn event_dim_sql(dim: Dim) -> &'static str {
    match dim {
        Dim::Total => "'*'",
        Dim::Region => "COALESCE(region,'(none)')",
        Dim::Ingress => "COALESCE(node_region,'(none)')",
        Dim::Model => "COALESCE(model,'(none)')",
        Dim::RegionModel => "COALESCE(region,'(none)') || ' / ' || COALESCE(model,'*')",
        Dim::Account => "'(none)'",
        Dim::Domain => "COALESCE(domain,'(none)')",
    }
}

pub struct Store {
    conn: Mutex<Connection>,
}

impl Store {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(schema())?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(schema())?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn config_get(&self, key: &str) -> Option<String> {
        self.conn
            .lock()
            .unwrap()
            .query_row("SELECT value FROM config WHERE key = ?1", params![key], |r| r.get(0))
            .ok()
    }

    pub fn config_set(&self, key: &str, value: &str) {
        let _ = self.conn.lock().unwrap().execute(
            "INSERT INTO config (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        );
    }

    pub fn insert_attempt(&self, r: &AttemptRecord) -> rusqlite::Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO attempts (request_id, attempt_no, ts, duration_ms, endpoint, model, session, account, \
             ingress_region, egress_region, egress_node, failure_domain, cf_colo, placement_reason, status, gate_result, \
             stream_outcome, error_code, saw_output, ttft_ms, held_ms, input_tokens, cached_input_tokens, output_tokens, \
             overload_hit, rescued) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26)",
            params![
                r.request_id, r.attempt_no, r.ts, r.duration_ms, r.endpoint, r.model, r.session, r.account,
                r.ingress_region, r.egress_region, r.egress_node, r.failure_domain, r.cf_colo, r.placement_reason,
                r.status, r.gate_result, r.stream_outcome, r.error_code, r.saw_output, r.ttft_ms, r.held_ms,
                r.input_tokens, r.cached_input_tokens, r.output_tokens, r.overload_hit, r.rescued
            ],
        )?;
        Ok(())
    }

    pub fn insert_relay(&self, r: &RelayRecord) -> rusqlite::Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO relay_attempts (ts, request_id, from_node, from_region, status, duration_ms, bytes, headers_ms, cf_colo, error) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![r.ts, r.request_id, r.from_node, r.from_region, r.status, r.duration_ms, r.bytes, r.headers_ms, r.cf_colo, r.error],
        )?;
        Ok(())
    }

    pub fn insert_event(&self, e: &Event) {
        let result = self.conn.lock().unwrap().execute(
            "INSERT INTO events (ts, kind, region, model, domain, from_region, to_region, node, node_region, request_id, detail, error_code) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![e.ts, e.kind, e.region, e.model, e.domain, e.from_region, e.to_region, e.node, e.node_region, e.request_id, e.detail, e.error_code],
        );
        if let Err(error) = result {
            tracing::error!(error = %error, kind = %e.kind, "event.persist_failed");
        }
    }

    pub fn insert_health_samples(&self, samples: &[HealthSample]) -> rusqlite::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        for s in samples {
            tx.execute(
                "INSERT OR REPLACE INTO health_samples (ts, region, model, domain, attempts, overloaded, errors, state, weight, ttft_mean_ms) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![s.ts, s.region, s.model, s.domain, s.attempts, s.overloaded, s.errors, s.state, s.weight, s.ttft_mean_ms],
            )?;
        }
        tx.commit()
    }

    /// Health samples in `[from, to)` for `model` (`*` for region aggregates), oldest first.
    pub fn health_samples(&self, from: i64, to: i64, model: &str) -> rusqlite::Result<Vec<HealthSample>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT ts, region, model, domain, attempts, overloaded, errors, state, weight, ttft_mean_ms FROM health_samples \
             WHERE ts >= ?1 AND ts < ?2 AND model = ?3 ORDER BY ts, region",
        )?;
        let rows = stmt.query_map(params![from, to, model], |r| {
            Ok(HealthSample {
                ts: r.get(0)?, region: r.get(1)?, model: r.get(2)?, domain: r.get(3)?, attempts: r.get(4)?,
                overloaded: r.get(5)?, errors: r.get(6)?, state: r.get(7)?, weight: r.get(8)?, ttft_mean_ms: r.get(9)?,
            })
        })?;
        rows.collect()
    }

    pub fn recent_attempts(&self, limit: usize) -> rusqlite::Result<Vec<AttemptRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT request_id, attempt_no, ts, duration_ms, endpoint, model, session, account, ingress_region, egress_region, \
             egress_node, failure_domain, cf_colo, placement_reason, status, gate_result, stream_outcome, error_code, saw_output, \
             ttft_ms, held_ms, input_tokens, cached_input_tokens, output_tokens, overload_hit, rescued FROM attempts ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok(AttemptRecord {
                request_id: r.get(0)?, attempt_no: r.get(1)?, ts: r.get(2)?, duration_ms: r.get(3)?, endpoint: r.get(4)?,
                model: r.get(5)?, session: r.get(6)?, account: r.get(7)?, ingress_region: r.get(8)?, egress_region: r.get(9)?,
                egress_node: r.get(10)?, failure_domain: r.get(11)?, cf_colo: r.get(12)?, placement_reason: r.get(13)?,
                status: r.get(14)?, gate_result: r.get(15)?, stream_outcome: r.get(16)?, error_code: r.get(17)?,
                saw_output: r.get(18)?, ttft_ms: r.get(19)?, held_ms: r.get(20)?, input_tokens: r.get(21)?,
                cached_input_tokens: r.get(22)?, output_tokens: r.get(23)?, overload_hit: r.get(24)?, rescued: r.get(25)?,
            })
        })?;
        rows.collect()
    }

    pub fn recent_events(&self, limit: usize) -> rusqlite::Result<Vec<Event>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT ts, kind, region, model, domain, from_region, to_region, node, node_region, request_id, detail, error_code \
             FROM events ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok(Event {
                ts: r.get(0)?, kind: r.get(1)?, region: r.get(2)?, model: r.get(3)?, domain: r.get(4)?, from_region: r.get(5)?,
                to_region: r.get(6)?, node: r.get(7)?, node_region: r.get(8)?, request_id: r.get(9)?, detail: r.get(10)?,
                error_code: r.get(11)?,
            })
        })?;
        rows.collect()
    }

    /// @cc [owner:ghuntley,label:accounting] monitor-bins-bounded
    /// `minute_bins` MUST count only attempts with `ts` in the trailing 60 minute buckets ending at
    /// `now_ms`, per egress region, as `(attempts, retried)` pairs, with exactly 60 pairs per region.
    pub fn minute_bins(&self, now_ms: i64) -> rusqlite::Result<std::collections::BTreeMap<String, Vec<(i64, i64)>>> {
        let first_minute = now_ms.div_euclid(60_000) - 59;
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT egress_region, CASE WHEN ts >= 0 THEN ts / 60000 ELSE (ts - 59999) / 60000 END AS minute, COUNT(*), \
             SUM(CASE WHEN gate_result = 'retried' THEN 1 ELSE 0 END) FROM attempts WHERE ts >= ?1 AND ts <= ?2 GROUP BY egress_region, minute",
        )?;
        let mut out: std::collections::BTreeMap<String, Vec<(i64, i64)>> = Default::default();
        let rows = stmt.query_map(params![first_minute * 60_000, now_ms], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?))
        })?;
        for row in rows {
            let (region, minute, attempts, retried) = row?;
            let bins = out.entry(region).or_insert_with(|| vec![(0, 0); 60]);
            if let Some(slot) = usize::try_from(minute - first_minute).ok().filter(|i| *i < 60) {
                bins[slot] = (attempts, retried);
            }
        }
        Ok(out)
    }

    pub fn relay_summary(&self, from: i64, to: i64) -> rusqlite::Result<Vec<RelaySummary>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT from_region, COUNT(*), SUM(CASE WHEN status = 0 OR status >= 500 THEN 1 ELSE 0 END), \
             CAST(AVG(duration_ms) AS INTEGER), CAST(AVG(headers_ms) AS INTEGER), COALESCE(SUM(bytes),0) \
             FROM relay_attempts WHERE ts >= ?1 AND ts < ?2 GROUP BY from_region ORDER BY from_region",
        )?;
        let rows = stmt.query_map(params![from, to], |r| {
            Ok(RelaySummary {
                from_region: r.get(0)?, requests: r.get(1)?, errors: r.get(2)?, mean_duration_ms: r.get(3)?,
                mean_headers_ms: r.get(4)?, bytes: r.get(5)?,
            })
        })?;
        rows.collect()
    }

    /// Per-bucket counts from `attempts` with `ts` in `[from, to)`.
    pub fn stats_counts(&self, from: i64, to: i64, bucket_ms: i64, dim: Dim) -> rusqlite::Result<Vec<CountRow>> {
        let (key, label) = attempt_dim_sql(dim);
        let sql = format!(
            "SELECT (ts - ?1) / ?3 AS b, {key} AS k, MAX({label}), COUNT(DISTINCT request_id), COUNT(*), COUNT(DISTINCT session), \
             COALESCE(SUM(input_tokens),0), COALESCE(SUM(cached_input_tokens),0), COALESCE(SUM(output_tokens),0), \
             COALESCE(SUM(CASE WHEN placement_reason LIKE 'moved%' THEN input_tokens ELSE 0 END),0), \
             COALESCE(SUM(CASE WHEN placement_reason LIKE 'moved%' THEN cached_input_tokens ELSE 0 END),0), \
             SUM(CASE WHEN status BETWEEN 200 AND 299 AND gate_result IN ('output','terminal','overflow','eof') \
                 AND (input_tokens IS NULL OR output_tokens IS NULL) THEN 1 ELSE 0 END), \
             SUM(CASE WHEN gate_result = 'retried' THEN 1 ELSE 0 END), SUM(overload_hit), SUM(rescued), \
             COALESCE(SUM(ttft_ms),0), COUNT(ttft_ms), COALESCE(SUM(held_ms),0), COUNT(held_ms) \
             FROM attempts WHERE ts >= ?1 AND ts < ?2 GROUP BY b, k"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![from, to, bucket_ms], |r| {
            Ok(CountRow {
                bucket: r.get(0)?, key: r.get(1)?, label: r.get(2)?, requests: r.get(3)?, attempts: r.get(4)?,
                sessions: r.get(5)?, input_tokens: r.get(6)?, cached_input_tokens: r.get(7)?, output_tokens: r.get(8)?,
                moved_input_tokens: r.get(9)?, moved_cached_input_tokens: r.get(10)?, unmeasured: r.get(11)?,
                retried: r.get(12)?, overload_hit: r.get(13)?, rescued: r.get(14)?, ttft_sum_ms: r.get(15)?,
                ttft_n: r.get(16)?, held_sum_ms: r.get(17)?, held_n: r.get(18)?,
            })
        })?;
        rows.collect()
    }

    fn tally(&self, sql: &str, from: i64, to: i64, bucket_ms: i64) -> rusqlite::Result<Vec<TallyRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(params![from, to, bucket_ms], |r| {
            Ok(TallyRow { bucket: r.get(0)?, key: r.get(1)?, label: r.get(2)?, name: r.get(3)?, count: r.get(4)? })
        })?;
        rows.collect()
    }

    fn attempt_tally(&self, from: i64, to: i64, bucket_ms: i64, dim: Dim, name: &str) -> rusqlite::Result<Vec<TallyRow>> {
        let (key, label) = attempt_dim_sql(dim);
        let sql = format!(
            "SELECT (ts - ?1) / ?3 AS b, {key} AS k, MAX({label}), {name} AS o, COUNT(*) \
             FROM attempts WHERE ts >= ?1 AND ts < ?2 GROUP BY b, k, o"
        );
        self.tally(&sql, from, to, bucket_ms)
    }

    pub fn stats_tallies(&self, from: i64, to: i64, bucket_ms: i64, dim: Dim) -> rusqlite::Result<Tallies> {
        let outcome = "COALESCE(stream_outcome, CASE WHEN status = 0 THEN 'network_error' WHEN status BETWEEN 200 AND 299 THEN 'unknown' ELSE 'http_' || status END)";
        let key = event_dim_sql(dim);
        let events_sql = format!(
            "SELECT (ts - ?1) / ?3 AS b, {key} AS k, MAX({key}), kind, COUNT(*) FROM events WHERE ts >= ?1 AND ts < ?2 GROUP BY b, k, kind"
        );
        Ok(Tallies {
            outcomes: self.attempt_tally(from, to, bucket_ms, dim, outcome)?,
            events: self.tally(&events_sql, from, to, bucket_ms)?,
            gates: self.attempt_tally(from, to, bucket_ms, dim, "gate_result")?,
            reasons: self.attempt_tally(from, to, bucket_ms, dim, "placement_reason")?,
        })
    }

    /// Lifetimes of attempts overlapping `[from, to)`; attempts started more than
    /// `stats::MAX_ATTEMPT_MS` before `from` are not considered.
    pub fn stats_intervals(&self, from: i64, to: i64, dim: Dim) -> rusqlite::Result<Vec<Interval>> {
        let (key, _) = attempt_dim_sql(dim);
        let sql = format!(
            "SELECT {key}, ts, ts + MAX(duration_ms, 1) FROM attempts \
             WHERE ts >= ?1 AND ts < ?2 AND duration_ms IS NOT NULL AND ts + MAX(duration_ms, 1) > ?3"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![from - stats::MAX_ATTEMPT_MS, to, from], |r| {
            Ok(Interval { key: r.get(0)?, start: r.get(1)?, end: r.get(2)? })
        })?;
        rows.collect()
    }

    /// @cc [owner:ghuntley,label:accounting] stats-series-raw
    /// `stats_series` MUST aggregate exactly the attempts and events with `ts` in `[from, to)` into
    /// `bucket_ms` buckets per `dim` key, with concurrency from attempt lifetimes.
    pub fn stats_series(&self, from: i64, to: i64, bucket_ms: i64, dim: Dim) -> rusqlite::Result<Vec<Series>> {
        let counts = self.stats_counts(from, to, bucket_ms, dim)?;
        let tallies = self.stats_tallies(from, to, bucket_ms, dim)?;
        let intervals = self.stats_intervals(from, to, dim)?;
        Ok(stats::assemble(from, to, bucket_ms, &counts, &tallies, &intervals))
    }

    /// Exact distinct request and session counts per `dim` key over `[from, to)`.
    pub fn stats_distinct(&self, from: i64, to: i64, dim: Dim) -> rusqlite::Result<Vec<(String, i64, i64)>> {
        let (key, _) = attempt_dim_sql(dim);
        let sql = format!(
            "SELECT {key} AS k, COUNT(DISTINCT request_id), COUNT(DISTINCT session) FROM attempts \
             WHERE ts >= ?1 AND ts < ?2 GROUP BY k"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![from, to], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        rows.collect()
    }

    /// Stored finished-day rollups for days in `[from_day, to_day)`: `(day, key, label, totals)`.
    pub fn stats_daily(&self, from_day: i64, to_day: i64, dim: Dim) -> rusqlite::Result<Vec<(i64, String, String, Totals)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT day, key, label, totals FROM stats_daily WHERE day >= ?1 AND day < ?2 AND dim = ?3 ORDER BY day, key",
        )?;
        let rows = stmt.query_map(params![from_day, to_day, dim.as_str()], |r| {
            let totals: String = r.get(3)?;
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, serde_json::from_str(&totals).unwrap_or_default()))
        })?;
        rows.collect()
    }

    /// Days in `[from_day, to_day)` that already have a stored rollup.
    pub fn stats_rolled_days(&self, from_day: i64, to_day: i64) -> rusqlite::Result<std::collections::BTreeSet<i64>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT day FROM stats_daily WHERE day >= ?1 AND day < ?2 AND dim = 'total'")?;
        let rows = stmt.query_map(params![from_day, to_day], |r| r.get(0))?;
        rows.collect()
    }

    /// @cc [owner:ghuntley,label:accounting] stats-daily-rollup
    /// `rollup_stats_day` MUST store, for every dim, exactly what `stats_series` reports for that UTC
    /// day as one bucket, and MUST always store a `total` row (zeros when idle) so the day is marked
    /// rolled up.
    pub fn rollup_stats_day(&self, day: i64, now_ms: i64) -> rusqlite::Result<()> {
        let from = day * stats::DAY_MS;
        let to = from + stats::DAY_MS;
        let mut rows: Vec<(Dim, String, String, Totals)> = Vec::new();
        for dim in Dim::all() {
            for series in self.stats_series(from, to, stats::DAY_MS, dim)? {
                let totals = series.buckets.into_iter().next().map(|b| b.totals).unwrap_or_default();
                rows.push((dim, series.key, series.label, totals));
            }
        }
        if !rows.iter().any(|(dim, ..)| *dim == Dim::Total) {
            rows.push((Dim::Total, stats::TOTAL_KEY.into(), stats::TOTAL_KEY.into(), Totals::default()));
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM stats_daily WHERE day = ?1", params![day])?;
        for (dim, key, label, totals) in rows {
            tx.execute(
                "INSERT INTO stats_daily (day, dim, key, label, totals, computed_at) VALUES (?1,?2,?3,?4,?5,?6)",
                params![day, dim.as_str(), key, label, serde_json::to_string(&totals).unwrap_or_default(), now_ms],
            )?;
        }
        tx.commit()
    }

    /// Earliest `ts` across attempts and events.
    pub fn stats_earliest_ts(&self) -> rusqlite::Result<Option<i64>> {
        self.conn.lock().unwrap().query_row(
            "SELECT MIN(t) FROM (SELECT MIN(ts) AS t FROM attempts UNION ALL SELECT MIN(ts) FROM events)",
            [],
            |r| r.get(0),
        )
    }

    /// @cc [owner:ghuntley,label:persistence] stats-retention
    /// `prune_stats` MUST delete every attempt, relay attempt, event, and health sample with
    /// `ts < cutoff_ms` and every daily rollup for a day ending at or before `cutoff_ms`, in
    /// bounded batches, and MUST NOT delete anything newer.
    pub fn prune_stats(&self, cutoff_ms: i64) -> rusqlite::Result<usize> {
        let mut deleted = 0;
        for table in ["attempts", "relay_attempts", "events", "health_samples"] {
            loop {
                let n = self.conn.lock().unwrap().execute(
                    &format!("DELETE FROM {table} WHERE id IN (SELECT id FROM {table} WHERE ts < ?1 LIMIT 5000)"),
                    params![cutoff_ms],
                )?;
                deleted += n;
                if n == 0 {
                    break;
                }
            }
        }
        deleted += self.conn.lock().unwrap().execute(
            "DELETE FROM stats_daily WHERE (day + 1) * ?1 <= ?2",
            params![stats::DAY_MS, cutoff_ms],
        )?;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn attempt(request: &str, no: i64, ts: i64, region: &str, gate: &str) -> AttemptRecord {
        AttemptRecord {
            request_id: request.into(), attempt_no: no, ts, duration_ms: Some(100), endpoint: "/backend-api/codex/responses".into(),
            model: "m".into(), session: Some(format!("s-{request}")), account: None, ingress_region: "a".into(),
            egress_region: region.into(), egress_node: format!("{region}-1"), failure_domain: region.into(),
            placement_reason: "new".into(), status: 200, gate_result: gate.into(), ..Default::default()
        }
    }

    #[test]
    fn counts_rescues_and_moved_tokens() {
        let store = Store::in_memory().unwrap();
        let mut first = attempt("r1", 0, 1_000, "a", "retried");
        first.overload_hit = true;
        store.insert_attempt(&first).unwrap();
        let mut second = attempt("r1", 1, 2_000, "b", "output");
        second.rescued = true;
        second.placement_reason = "moved_failure".into();
        second.input_tokens = Some(100);
        second.cached_input_tokens = Some(10);
        second.output_tokens = Some(5);
        store.insert_attempt(&second).unwrap();
        let series = store.stats_series(0, 60_000, 60_000, Dim::Total).unwrap();
        let t = &series[0].buckets[0].totals;
        assert_eq!((t.requests, t.attempts, t.retried, t.overload_hit, t.rescued), (1, 2, 1, 1, 1));
        assert_eq!(t.rescue_rate(), Some(1.0));
        assert_eq!((t.moved_input_tokens, t.moved_cached_input_tokens), (100, 10));
        assert_eq!(t.gates.get("retried"), Some(&1));
        assert_eq!(t.reasons.get("moved_failure"), Some(&1));
        let by_region = store.stats_series(0, 60_000, 60_000, Dim::Region).unwrap();
        assert_eq!(by_region.len(), 2);
    }

    #[test]
    fn minute_bins_are_per_region() {
        let store = Store::in_memory().unwrap();
        store.insert_attempt(&attempt("r1", 0, 120_000, "a", "retried")).unwrap();
        store.insert_attempt(&attempt("r1", 1, 120_500, "b", "output")).unwrap();
        let bins = store.minute_bins(150_000).unwrap();
        assert_eq!(bins["a"][59], (1, 1));
        assert_eq!(bins["b"][59], (1, 0));
        assert_eq!(bins["a"].len(), 60);
    }
}
