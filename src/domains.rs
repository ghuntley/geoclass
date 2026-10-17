//! Failure domains: groups of egress regions that share an upstream serving pool.
//!
//! Operators group regions they already know share a pool (`failure_domain`). On top of that,
//! regions whose per-minute failure rates rise and fall together are merged: if one is
//! overloaded the other almost certainly is too, so a retry should leave the whole domain first.
//! Learning only ever merges; it never splits an operator group. Pure and synchronous.
use crate::config::RegionConfig;
use std::collections::{BTreeMap, VecDeque};

const HISTORY_MINUTES: usize = 360;
const MIN_SHARED: usize = 30;
const MIN_EPISODE_MINUTES: usize = 3;
const EPISODE_RATE: f64 = 0.10;
const MERGE_CORRELATION: f64 = 0.8;

/// Per-region per-minute failure rates (`None` when the region saw no attempts that minute).
#[derive(Default)]
pub struct History {
    minutes: VecDeque<i64>,
    rates: BTreeMap<String, VecDeque<Option<f64>>>,
}

impl History {
    /// Records one minute's rates. Regions absent from `rates` get `None` for that minute.
    pub fn record(&mut self, minute: i64, rates: &BTreeMap<String, Option<f64>>) {
        if self.minutes.back().is_some_and(|m| *m >= minute) {
            return;
        }
        self.minutes.push_back(minute);
        let len = self.minutes.len();
        for (region, rate) in rates {
            let series = self.rates.entry(region.clone()).or_default();
            while series.len() + 1 < len {
                series.push_back(None);
            }
            series.push_back(*rate);
        }
        for series in self.rates.values_mut() {
            while series.len() < len {
                series.push_back(None);
            }
        }
        while self.minutes.len() > HISTORY_MINUTES {
            self.minutes.pop_front();
            for series in self.rates.values_mut() {
                series.pop_front();
            }
        }
    }

    pub fn series(&self) -> BTreeMap<String, Vec<Option<f64>>> {
        self.rates.iter().map(|(k, v)| (k.clone(), v.iter().copied().collect())).collect()
    }
}

/// Pearson correlation over minutes where both series have a value, if there are enough.
pub fn correlation(a: &[Option<f64>], b: &[Option<f64>]) -> Option<f64> {
    let pairs: Vec<(f64, f64)> = a.iter().zip(b).filter_map(|(x, y)| Some(((*x)?, (*y)?))).collect();
    if pairs.len() < MIN_SHARED {
        return None;
    }
    let episodes = |pick: fn(&(f64, f64)) -> f64| pairs.iter().filter(|p| pick(p) >= EPISODE_RATE).count();
    if episodes(|p| p.0) < MIN_EPISODE_MINUTES || episodes(|p| p.1) < MIN_EPISODE_MINUTES {
        return None;
    }
    let n = pairs.len() as f64;
    let (mx, my) = (pairs.iter().map(|p| p.0).sum::<f64>() / n, pairs.iter().map(|p| p.1).sum::<f64>() / n);
    let (mut sxy, mut sxx, mut syy) = (0.0, 0.0, 0.0);
    for (x, y) in &pairs {
        sxy += (x - mx) * (y - my);
        sxx += (x - mx).powi(2);
        syy += (y - my).powi(2);
    }
    (sxx > 0.0 && syy > 0.0).then(|| sxy / (sxx * syy).sqrt())
}

fn find(parent: &mut BTreeMap<String, String>, x: &str) -> String {
    let p = parent.get(x).cloned().unwrap_or_else(|| x.to_string());
    if p == x {
        return p;
    }
    let root = find(parent, &p);
    parent.insert(x.to_string(), root.clone());
    root
}

fn union(parent: &mut BTreeMap<String, String>, a: &str, b: &str) {
    let (ra, rb) = (find(parent, a), find(parent, b));
    if ra != rb {
        let (keep, drop) = if ra < rb { (ra, rb) } else { (rb, ra) };
        parent.insert(drop, keep);
    }
}

/// @cc [owner:ghuntley,label:routing] failure-domains-only-merge
/// `assign` MUST put two regions in the same domain whenever the operator gave them the same
/// `failure_domain`, MUST additionally merge regions whose `correlation` is at least 0.8 when
/// `learn` is set, and MUST name each domain by the smallest configured domain label (or region
/// name, for regions without one) among its members. It MUST NOT treat region names specially.
pub fn assign(regions: &[RegionConfig], history: &BTreeMap<String, Vec<Option<f64>>>, learn: bool) -> BTreeMap<String, String> {
    let label = |r: &RegionConfig| r.failure_domain.clone().unwrap_or_else(|| r.name.clone());
    let mut names: Vec<String> = regions.iter().map(|r| r.name.clone()).collect();
    for name in history.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    let mut parent: BTreeMap<String, String> = names.iter().map(|n| (n.clone(), n.clone())).collect();
    let mut by_label: BTreeMap<String, String> = BTreeMap::new();
    for r in regions {
        match by_label.get(&label(r)) {
            Some(first) => {
                let first = first.clone();
                union(&mut parent, &first, &r.name)
            }
            None => {
                by_label.insert(label(r), r.name.clone());
            }
        }
    }
    if learn {
        for (i, a) in names.iter().enumerate() {
            for b in &names[i + 1..] {
                if let (Some(sa), Some(sb)) = (history.get(a), history.get(b))
                    && correlation(sa, sb).is_some_and(|c| c >= MERGE_CORRELATION) {
                        union(&mut parent, a, b);
                    }
            }
        }
    }
    let region_label: BTreeMap<String, String> = names
        .iter()
        .map(|n| (n.clone(), regions.iter().find(|r| &r.name == n).map(label).unwrap_or_else(|| n.clone())))
        .collect();
    let mut domain_name: BTreeMap<String, String> = BTreeMap::new();
    for n in &names {
        let root = find(&mut parent, n);
        let l = region_label[n].clone();
        domain_name.entry(root).and_modify(|d| if l < *d { *d = l.clone() }).or_insert(l);
    }
    names
        .iter()
        .map(|n| {
            let root = find(&mut parent, n);
            (n.clone(), domain_name[&root].clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(name: &str, domain: Option<&str>) -> RegionConfig {
        RegionConfig { name: name.into(), failure_domain: domain.map(str::to_string), ..Default::default() }
    }

    #[test]
    fn configured_groups() {
        let d = assign(&[r("a", Some("p")), r("b", Some("p")), r("c", None)], &BTreeMap::new(), true);
        assert_eq!(d["a"], "p");
        assert_eq!(d["b"], "p");
        assert_eq!(d["c"], "c");
    }

    #[test]
    fn correlated_regions_merge_and_uncorrelated_do_not() {
        let wave: Vec<Option<f64>> = (0..60).map(|i| Some(if (i / 10) % 2 == 0 { 0.5 } else { 0.0 })).collect();
        let anti: Vec<Option<f64>> = wave.iter().map(|v| v.map(|x| 0.5 - x)).collect();
        let mut h = BTreeMap::new();
        h.insert("a".to_string(), wave.clone());
        h.insert("b".to_string(), wave);
        h.insert("c".to_string(), anti);
        let regions = [r("a", None), r("b", None), r("c", None)];
        let d = assign(&regions, &h, true);
        assert_eq!(d["a"], "a");
        assert_eq!(d["b"], "a");
        assert_eq!(d["c"], "c");
        let d = assign(&regions, &h, false);
        assert_eq!(d["b"], "b");
    }

    #[test]
    fn quiet_regions_never_correlate() {
        let flat: Vec<Option<f64>> = vec![Some(0.0); 60];
        assert_eq!(correlation(&flat, &flat), None);
        let short: Vec<Option<f64>> = vec![Some(0.5); 5];
        assert_eq!(correlation(&short, &short), None);
    }

    #[test]
    fn history_aligns_series() {
        let mut h = History::default();
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), Some(0.1));
        h.record(1, &m);
        m.insert("b".to_string(), Some(0.2));
        h.record(2, &m);
        h.record(2, &m);
        let s = h.series();
        assert_eq!(s["a"], vec![Some(0.1), Some(0.1)]);
        assert_eq!(s["b"], vec![None, Some(0.2)]);
    }
}
