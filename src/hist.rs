//! Fixed-bucket latency histogram: mergeable by addition, so percentiles survive gossip and the
//! cluster stats fan-out. Pure and synchronous.
use serde::{Deserialize, Serialize};

/// Upper bounds (ms, inclusive) of every bucket but the last, which holds everything larger.
pub const BOUNDS_MS: [i64; 20] =
    [5, 10, 25, 50, 100, 200, 300, 500, 750, 1_000, 1_500, 2_000, 3_000, 5_000, 7_500, 10_000, 15_000, 20_000, 30_000, 60_000];
pub const BUCKETS: usize = BOUNDS_MS.len() + 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hist(pub [i64; BUCKETS]);

/// Bucket index for a latency; negative values count as 0.
pub fn bucket_of(ms: i64) -> usize {
    BOUNDS_MS.iter().position(|b| ms.max(0) <= *b).unwrap_or(BOUNDS_MS.len())
}

impl Hist {
    pub fn record(&mut self, ms: i64) {
        self.0[bucket_of(ms)] += 1;
    }

    pub fn add(&mut self, other: &Hist) {
        for (a, b) in self.0.iter_mut().zip(other.0) {
            *a += b;
        }
    }

    pub fn count(&self) -> i64 {
        self.0.iter().sum()
    }

    /// @cc [owner:ghuntley,label:accounting] histogram-percentile
    /// `percentile(q)` MUST return `None` for an empty histogram and otherwise the upper bound of
    /// the first bucket at which the cumulative count reaches `ceil(q * count)` (at least 1), using
    /// the last finite bound for the overflow bucket. It MUST be monotone in `q`.
    pub fn percentile(&self, q: f64) -> Option<i64> {
        let n = self.count();
        if n <= 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * n as f64).ceil() as i64).max(1);
        let mut seen = 0;
        for (i, c) in self.0.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(BOUNDS_MS[i.min(BOUNDS_MS.len() - 1)]);
            }
        }
        Some(BOUNDS_MS[BOUNDS_MS.len() - 1])
    }

    pub fn p50(&self) -> Option<i64> {
        self.percentile(0.5)
    }

    pub fn p95(&self) -> Option<i64> {
        self.percentile(0.95)
    }
}

/// SQL expression giving `bucket_of(column)` for a non-null integer column.
pub fn sql_bucket(column: &str) -> String {
    let mut out = String::from("CASE");
    for (i, b) in BOUNDS_MS.iter().enumerate() {
        out.push_str(&format!(" WHEN {column} <= {b} THEN {i}"));
    }
    out.push_str(&format!(" ELSE {} END", BOUNDS_MS.len()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_bucket_upper_bounds() {
        let mut h = Hist::default();
        assert_eq!(h.p50(), None);
        for ms in [10, 40, 90, 90, 4_000] {
            h.record(ms);
        }
        assert_eq!(h.p50(), Some(100));
        assert_eq!(h.p95(), Some(5_000));
        h.record(1_000_000);
        assert_eq!(h.percentile(1.0), Some(60_000));
        assert_eq!(bucket_of(-5), 0);
        assert_eq!(bucket_of(5), 0);
        assert_eq!(bucket_of(25), 2);
        assert_eq!(bucket_of(26), 3);
    }

    #[test]
    fn serializes_as_a_plain_array() {
        let mut h = Hist::default();
        h.record(30);
        let json = serde_json::to_string(&h).unwrap();
        assert!(json.starts_with("[0,0,0,1,"));
        assert_eq!(serde_json::from_str::<Hist>(&json).unwrap(), h);
    }
}
