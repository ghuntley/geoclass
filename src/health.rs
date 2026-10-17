//! Region health from attempt outcomes.
//!
//! Each node counts the attempts it made (as ingress) per egress region and model in a sliding
//! window, gossips the window sums, and sums the latest window from every live node. Each
//! `(region, model)` — and each region across all models (`ALL_MODELS`) — gets a green/amber/red
//! state with hysteresis. Pure and synchronous; time is a parameter.
use crate::config::Thresholds;
use crate::hist::Hist;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

/// Model key for a region's all-model aggregate.
pub const ALL_MODELS: &str = "*";
const BUCKET_MS: i64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    pub attempts: i64,
    /// Capacity failures before output.
    pub overloaded: i64,
    /// Transport failures and 5xx before output.
    pub errors: i64,
    /// Sum and count of time-to-first-output for committed attempts (ms).
    pub ttft_sum_ms: i64,
    pub ttft_n: i64,
    /// Time-to-first-output distribution for committed attempts.
    #[serde(default)]
    pub ttft_hist: Hist,
}

impl Counts {
    pub fn add(&mut self, other: &Counts) {
        self.attempts += other.attempts;
        self.overloaded += other.overloaded;
        self.errors += other.errors;
        self.ttft_sum_ms += other.ttft_sum_ms;
        self.ttft_n += other.ttft_n;
        self.ttft_hist.add(&other.ttft_hist);
    }

    pub fn failures(&self) -> i64 {
        self.overloaded + self.errors
    }

    /// Failure rate in `[0, 1]`; 0 with no attempts.
    pub fn rate(&self) -> f64 {
        if self.attempts <= 0 { 0.0 } else { (self.failures() as f64 / self.attempts as f64).clamp(0.0, 1.0) }
    }
}

/// One observed attempt outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Observation {
    Committed { ttft_ms: Option<i64> },
    Overloaded,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowCount {
    pub region: String,
    pub model: String,
    pub counts: Counts,
}

/// Counts keyed by `(region, model)`.
pub type RegionModelCounts = BTreeMap<(String, String), Counts>;

/// This node's own observations in time buckets.
#[derive(Default)]
pub struct LocalWindow {
    buckets: VecDeque<(i64, RegionModelCounts)>,
}

impl LocalWindow {
    pub fn observe(&mut self, region: &str, model: &str, observation: Observation, now: i64) {
        let bucket = now.div_euclid(BUCKET_MS);
        if self.buckets.back().is_none_or(|(b, _)| *b != bucket) {
            self.buckets.push_back((bucket, BTreeMap::new()));
        }
        let map = &mut self.buckets.back_mut().unwrap().1;
        for model in [model, ALL_MODELS] {
            let counts = map.entry((region.to_string(), model.to_string())).or_default();
            counts.attempts += 1;
            match observation {
                Observation::Committed { ttft_ms } => {
                    if let Some(ttft) = ttft_ms {
                        counts.ttft_sum_ms += ttft.max(0);
                        counts.ttft_n += 1;
                        counts.ttft_hist.record(ttft);
                    }
                }
                Observation::Overloaded => counts.overloaded += 1,
                Observation::Error => counts.errors += 1,
            }
        }
    }

    /// Sums over buckets newer than `now - window_ms`; older buckets are dropped.
    pub fn sums(&mut self, now: i64, window_ms: i64) -> Vec<WindowCount> {
        let oldest = (now - window_ms).div_euclid(BUCKET_MS);
        while self.buckets.front().is_some_and(|(b, _)| *b <= oldest) {
            self.buckets.pop_front();
        }
        let mut total: BTreeMap<(String, String), Counts> = BTreeMap::new();
        for (_, map) in &self.buckets {
            for (key, counts) in map {
                total.entry(key.clone()).or_default().add(counts);
            }
        }
        total.into_iter().map(|((region, model), counts)| WindowCount { region, model, counts }).collect()
    }
}

/// Sums window counts reported by every live node.
pub fn merge<'a>(windows: impl IntoIterator<Item = &'a [WindowCount]>) -> BTreeMap<(String, String), Counts> {
    let mut out: BTreeMap<(String, String), Counts> = BTreeMap::new();
    for window in windows {
        for wc in window {
            out.entry((wc.region.clone(), wc.model.clone())).or_default().add(&wc.counts);
        }
    }
    out
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    #[default]
    Green,
    Amber,
    Red,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Green => "green",
            Self::Amber => "amber",
            Self::Red => "red",
        }
    }
}

/// @cc [owner:ghuntley,label:routing] health-hysteresis
/// With at least `min_samples` attempts, a rate at or above `red_rate` MUST be red, and a red
/// state MUST stay red until the rate is at or below `green_rate`. With fewer samples, a red
/// state MUST stay red while any failure remains in the window and MUST turn green once none do.
/// A non-red state MUST be amber when it has failures above `green_rate` (or any failure with too
/// few samples) and green otherwise.
pub fn next_state(previous: State, counts: &Counts, t: &Thresholds) -> State {
    let rate = counts.rate();
    if counts.attempts < t.min_samples {
        return match (previous, counts.failures() > 0) {
            (State::Red, true) => State::Red,
            (_, true) => State::Amber,
            (_, false) => State::Green,
        };
    }
    if rate >= t.red_rate {
        State::Red
    } else if previous == State::Red {
        if rate <= t.green_rate { State::Green } else { State::Red }
    } else if rate > t.green_rate {
        State::Amber
    } else {
        State::Green
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transition {
    pub region: String,
    pub model: String,
    pub from: State,
    pub to: State,
}

/// Per `(region, model)` states, advanced from the merged window on every gossip tick.
#[derive(Default)]
pub struct HealthTracker {
    states: BTreeMap<(String, String), State>,
    merged: BTreeMap<(String, String), Counts>,
}

impl HealthTracker {
    pub fn update(&mut self, merged: BTreeMap<(String, String), Counts>, t: &Thresholds) -> Vec<Transition> {
        let mut transitions = Vec::new();
        let keys: std::collections::BTreeSet<(String, String)> = merged.keys().chain(self.states.keys()).cloned().collect();
        for key in keys {
            let counts = merged.get(&key).copied().unwrap_or_default();
            let previous = self.states.get(&key).copied().unwrap_or_default();
            let next = next_state(previous, &counts, t);
            if next != previous {
                transitions.push(Transition { region: key.0.clone(), model: key.1.clone(), from: previous, to: next });
            }
            if next == State::Green && counts.attempts == 0 {
                self.states.remove(&key);
            } else {
                self.states.insert(key, next);
            }
        }
        self.merged = merged;
        transitions
    }

    /// The state routing uses for `model` in `region`: the model's own state when the window has
    /// any attempt for it, else the region's all-model state.
    pub fn state(&self, region: &str, model: &str) -> State {
        let own = (region.to_string(), model.to_string());
        if self.merged.get(&own).is_some_and(|c| c.attempts > 0) || self.states.contains_key(&own) {
            self.states.get(&own).copied().unwrap_or_default()
        } else {
            self.states.get(&(region.to_string(), ALL_MODELS.to_string())).copied().unwrap_or_default()
        }
    }

    pub fn counts(&self, region: &str, model: &str) -> Counts {
        self.merged.get(&(region.to_string(), model.to_string())).copied().unwrap_or_default()
    }

    pub fn merged(&self) -> &BTreeMap<(String, String), Counts> {
        &self.merged
    }

    pub fn states(&self) -> &BTreeMap<(String, String), State> {
        &self.states
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> Thresholds {
        Thresholds { window_ms: 300_000, red_rate: 0.10, green_rate: 0.03, min_samples: 5 }
    }

    fn c(attempts: i64, overloaded: i64) -> Counts {
        Counts { attempts, overloaded, ..Default::default() }
    }

    #[test]
    fn hysteresis() {
        assert_eq!(next_state(State::Green, &c(10, 1), &t()), State::Red);
        assert_eq!(next_state(State::Red, &c(100, 5), &t()), State::Red);
        assert_eq!(next_state(State::Red, &c(100, 3), &t()), State::Green);
        assert_eq!(next_state(State::Green, &c(100, 5), &t()), State::Amber);
        assert_eq!(next_state(State::Green, &c(100, 3), &t()), State::Green);
        assert_eq!(next_state(State::Red, &c(2, 1), &t()), State::Red);
        assert_eq!(next_state(State::Red, &c(0, 0), &t()), State::Green);
        assert_eq!(next_state(State::Green, &c(1, 1), &t()), State::Amber);
    }

    #[test]
    fn window_ages_out_and_merges_across_nodes() {
        let mut w = LocalWindow::default();
        w.observe("a", "m", Observation::Overloaded, 0);
        w.observe("a", "m", Observation::Committed { ttft_ms: Some(40) }, 5_000);
        let sums = w.sums(5_000, 60_000);
        let m = sums.iter().find(|s| s.model == "m").unwrap();
        assert_eq!((m.counts.attempts, m.counts.overloaded, m.counts.ttft_n), (2, 1, 1));
        assert_eq!(m.counts.ttft_hist.p50(), Some(50));
        assert!(sums.iter().any(|s| s.model == ALL_MODELS));
        assert!(w.sums(120_000, 60_000).is_empty());
        let other = vec![WindowCount { region: "a".into(), model: "m".into(), counts: c(3, 0) }];
        let merged = merge([sums.as_slice(), other.as_slice()]);
        assert_eq!(merged[&("a".to_string(), "m".to_string())].attempts, 5);
    }

    #[test]
    fn model_state_falls_back_to_region_aggregate() {
        let mut h = HealthTracker::default();
        let mut merged = BTreeMap::new();
        merged.insert(("a".to_string(), "m1".to_string()), c(10, 5));
        merged.insert(("a".to_string(), ALL_MODELS.to_string()), c(10, 5));
        let transitions = h.update(merged, &t());
        assert_eq!(transitions.len(), 2);
        assert_eq!(h.state("a", "m1"), State::Red);
        assert_eq!(h.state("a", "m2"), State::Red, "no data for m2: the region aggregate decides");
        assert_eq!(h.state("b", "m1"), State::Green);
    }
}
