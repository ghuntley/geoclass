//! Placement: which region an attempt egresses from.
//!
//! - A bound session stays in its region until that region turns red, becomes unavailable, or
//!   fails this request.
//! - Everything else (new sessions, moved sessions, unkeyed requests) goes to a weighted
//!   rendezvous hash over available, non-red regions, preferring failure domains this request
//!   has not tried yet. Weights are the chase-the-sun prior, scaled down for amber regions.
//!
//! Pure and synchronous: decisions depend only on the arguments, so every node that shares a
//! snapshot makes the same choice.
use crate::health::State;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq)]
pub struct RegionView {
    pub name: String,
    pub domain: String,
    /// At least one live node can egress from this region.
    pub available: bool,
    pub state: State,
    /// Chase-the-sun prior weight (>= 0).
    pub weight: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// The session's binding was honoured.
    Bound,
    /// A session without a binding was placed.
    New,
    /// The bound region is red.
    MovedRed,
    /// The bound region failed this request.
    MovedFailure,
    /// No live node serves the bound region.
    MovedUnavailable,
    /// A request without a session.
    Unkeyed,
    /// A later attempt for a request whose session is not bound (or has no session).
    Retry,
}

impl Reason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bound => "bound",
            Self::New => "new",
            Self::MovedRed => "moved_red",
            Self::MovedFailure => "moved_failure",
            Self::MovedUnavailable => "moved_unavailable",
            Self::Unkeyed => "unkeyed",
            Self::Retry => "retry",
        }
    }

    pub fn is_move(self) -> bool {
        matches!(self, Self::MovedRed | Self::MovedFailure | Self::MovedUnavailable)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub region: String,
    pub reason: Reason,
}

pub struct Request<'a> {
    /// Hashed session key, when the request has a session.
    pub session: Option<&'a str>,
    /// Spreads unkeyed requests (the request id).
    pub spread: &'a str,
    /// The session's bound region, if any.
    pub binding: Option<&'a str>,
    /// Regions this request already tried, in order.
    pub tried: &'a [String],
}

fn unit(seed: &str, region: &str) -> f64 {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    h.update([0]);
    h.update(region.as_bytes());
    let d = h.finalize();
    let x = u64::from_be_bytes(d[..8].try_into().unwrap());
    ((x >> 11) as f64 + 0.5) / (1u64 << 53) as f64
}

fn health_factor(state: State) -> f64 {
    match state {
        State::Green => 1.0,
        State::Amber => 0.5,
        State::Red => 0.25,
    }
}

/// Weighted rendezvous score: the highest score wins, and each region wins in proportion to its
/// weight across seeds.
pub fn score(seed: &str, region: &RegionView) -> f64 {
    let w = (region.weight.max(0.0) * health_factor(region.state)).max(1e-9);
    w / -unit(seed, &region.name).ln()
}

/// @cc [owner:ghuntley,label:routing] sticky-until-trouble
/// When the request has a binding to a region that is available and not already tried by this
/// request, `choose` MUST return that region with `Reason::Bound` if it is not red, or if every
/// available untried region is red. Otherwise it MUST
/// return an available, untried region, preferring non-red regions and then regions whose
/// failure domain this request has not tried, chosen by the highest `score`; the reason MUST name
/// why a bound session moved (`MovedFailure`, `MovedUnavailable`, `MovedRed`). It MUST return
/// `None` only when every available region was tried, and the result MUST NOT depend on the order
/// of `regions`.
pub fn choose(regions: &[RegionView], req: &Request) -> Option<Choice> {
    let tried = |name: &str| req.tried.iter().any(|t| t == name);
    if let Some(bound) = req.binding
        && let Some(view) = regions.iter().find(|r| r.name == bound)
            && view.available && view.state != State::Red && !tried(bound) {
                return Some(Choice { region: bound.to_string(), reason: Reason::Bound });
            }
    let pool: Vec<&RegionView> = regions.iter().filter(|r| r.available && !tried(&r.name)).collect();
    if pool.is_empty() {
        return None;
    }
    let healthy: Vec<&RegionView> = pool.iter().copied().filter(|r| r.state != State::Red).collect();
    if healthy.is_empty() {
        // Everywhere is red: a bound session keeps its warm cache rather than moving sideways.
        if let Some(bound) = req.binding.filter(|b| pool.iter().any(|r| r.name == *b)) {
            return Some(Choice { region: bound.to_string(), reason: Reason::Bound });
        }
    }
    let healthy = if healthy.is_empty() { pool } else { healthy };
    let tried_domains: Vec<&str> = regions.iter().filter(|r| tried(&r.name)).map(|r| r.domain.as_str()).collect();
    let fresh: Vec<&RegionView> = healthy.iter().copied().filter(|r| !tried_domains.contains(&r.domain.as_str())).collect();
    let candidates = if fresh.is_empty() { healthy } else { fresh };
    let seed = req.session.unwrap_or(req.spread);
    let best = candidates
        .into_iter()
        .max_by(|a, b| score(seed, a).total_cmp(&score(seed, b)).then_with(|| b.name.cmp(&a.name)))?;
    let reason = match req.binding {
        Some(bound) if tried(bound) => Reason::MovedFailure,
        Some(bound) => match regions.iter().find(|r| r.name == bound) {
            Some(view) if view.available => Reason::MovedRed,
            _ => Reason::MovedUnavailable,
        },
        None if !req.tried.is_empty() => Reason::Retry,
        None if req.session.is_some() => Reason::New,
        None => Reason::Unkeyed,
    };
    Some(Choice { region: best.name.clone(), reason })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(name: &str, state: State) -> RegionView {
        RegionView { name: name.into(), domain: name.into(), available: true, state, weight: 1.0 }
    }

    fn req<'a>(session: Option<&'a str>, binding: Option<&'a str>, tried: &'a [String]) -> Request<'a> {
        Request { session, spread: "req-1", binding, tried }
    }

    #[test]
    fn bound_sessions_stay_until_trouble() {
        let regions = [view("a", State::Amber), view("b", State::Green)];
        let c = choose(&regions, &req(Some("s"), Some("a"), &[])).unwrap();
        assert_eq!(c, Choice { region: "a".into(), reason: Reason::Bound });
        let regions = [view("a", State::Red), view("b", State::Green)];
        assert_eq!(choose(&regions, &req(Some("s"), Some("a"), &[])).unwrap(), Choice { region: "b".into(), reason: Reason::MovedRed });
        let tried = vec!["a".to_string()];
        let regions = [view("a", State::Green), view("b", State::Green)];
        assert_eq!(choose(&regions, &req(Some("s"), Some("a"), &tried)).unwrap(), Choice { region: "b".into(), reason: Reason::MovedFailure });
        let mut gone = view("a", State::Green);
        gone.available = false;
        let regions = [gone, view("b", State::Green)];
        assert_eq!(choose(&regions, &req(Some("s"), Some("a"), &[])).unwrap().reason, Reason::MovedUnavailable);
    }

    #[test]
    fn retries_leave_the_failed_domain_first() {
        let mut regions = vec![view("a", State::Green), view("b", State::Green), view("c", State::Green)];
        regions[1].domain = "a".into();
        let tried = vec!["a".to_string()];
        for seed in 0..50 {
            let spread = format!("r{seed}");
            let c = choose(&regions, &Request { session: None, spread: &spread, binding: None, tried: &tried }).unwrap();
            assert_eq!(c.region, "c");
            assert_eq!(c.reason, Reason::Retry);
        }
    }

    #[test]
    fn all_red_still_tries_and_exhaustion_is_none() {
        let regions = [view("a", State::Red), view("b", State::Red)];
        assert!(choose(&regions, &req(Some("s"), None, &[])).is_some());
        let tried = vec!["a".to_string(), "b".to_string()];
        assert!(choose(&regions, &req(Some("s"), None, &tried)).is_none());
    }

    #[test]
    fn weights_steer_new_sessions() {
        let mut heavy = view("a", State::Green);
        heavy.weight = 9.0;
        let regions = [heavy, view("b", State::Green)];
        let wins = (0..2000)
            .filter(|i| choose(&regions, &req(Some(&format!("s{i}")), None, &[])).unwrap().region == "a")
            .count();
        assert!((1650..1950).contains(&wins), "expected ~90% for weight 9:1, got {wins}");
    }
}
