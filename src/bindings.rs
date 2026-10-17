//! Session → region bindings.
//!
//! A binding pins a session (by the SHA-256 of its prompt cache key) to the region whose upstream
//! prompt cache holds it. Bindings change only when a session moves, are gossiped to every node so
//! any ingress honours them, and merge last-writer-wins. Soft state: a restart relearns them from
//! peers. Pure and synchronous; time is a parameter.
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingRecord {
    /// SHA-256 hex of the prompt cache key.
    pub key: String,
    pub region: String,
    /// When the binding last changed region (ms). Orders concurrent writers.
    pub updated_at: i64,
    /// Node that wrote it; breaks `updated_at` ties.
    pub node: String,
}

#[derive(Clone, Debug)]
struct Entry {
    record: BindingRecord,
    last_used: i64,
}

pub struct Bindings {
    map: LruCache<String, Entry>,
    ttl_ms: i64,
}

fn newer(a: &BindingRecord, b: &BindingRecord) -> bool {
    (a.updated_at, &a.node, &a.region) > (b.updated_at, &b.node, &b.region)
}

impl Bindings {
    pub fn new(cap: usize, ttl_ms: i64) -> Self {
        Self { map: LruCache::new(NonZeroUsize::new(cap.max(1)).unwrap()), ttl_ms }
    }

    /// The live binding for `key`, refreshing its idle timer. Expired bindings are dropped.
    pub fn get(&mut self, key: &str, now: i64) -> Option<BindingRecord> {
        let expired = match self.map.get_mut(key) {
            Some(entry) if now - entry.last_used <= self.ttl_ms => {
                entry.last_used = now;
                return Some(entry.record.clone());
            }
            Some(_) => true,
            None => false,
        };
        if expired {
            self.map.pop(key);
        }
        None
    }

    /// @cc [owner:ghuntley,label:routing] binding-write-is-latest
    /// `bind` MUST leave `key` bound to `region` with `updated_at = now` when the region changes,
    /// MUST NOT change `updated_at` when the key is already bound to `region`, and MUST return
    /// the previous region when the binding moved.
    pub fn bind(&mut self, key: &str, region: &str, node: &str, now: i64) -> BindOutcome {
        if let Some(entry) = self.map.get_mut(key)
            && now - entry.last_used <= self.ttl_ms && entry.record.region == region {
                entry.last_used = now;
                return BindOutcome::Unchanged;
            }
        let previous = self.get(key, now).map(|r| r.region);
        let record = BindingRecord { key: key.to_string(), region: region.to_string(), updated_at: now, node: node.to_string() };
        self.map.put(key.to_string(), Entry { record, last_used: now });
        match previous {
            Some(from) => BindOutcome::Moved { from },
            None => BindOutcome::Created,
        }
    }

    /// @cc [owner:ghuntley,label:routing] binding-merge-lww
    /// `merge` MUST be last-writer-wins on `(updated_at, node, region)`: applying the same set of
    /// records in any order and any number of times MUST leave every key with the greatest record.
    pub fn merge(&mut self, record: BindingRecord, now: i64) -> bool {
        if record.key.is_empty() || record.region.is_empty() || now - record.updated_at > self.ttl_ms {
            return false;
        }
        match self.map.get_mut(&record.key) {
            Some(entry) if !newer(&record, &entry.record) => false,
            Some(entry) => {
                entry.record = record;
                entry.last_used = entry.last_used.max(now);
                true
            }
            None => {
                self.map.put(record.key.clone(), Entry { record, last_used: now });
                true
            }
        }
    }

    /// Records written at or after `since`, for incremental gossip.
    pub fn changed_since(&self, since: i64) -> Vec<BindingRecord> {
        self.map.iter().filter(|(_, e)| e.record.updated_at >= since).map(|(_, e)| e.record.clone()).collect()
    }

    /// Every live record, for a peer's full sync on join.
    pub fn snapshot(&self, now: i64) -> Vec<BindingRecord> {
        self.map
            .iter()
            .filter(|(_, e)| now - e.last_used <= self.ttl_ms)
            .map(|(_, e)| e.record.clone())
            .collect()
    }

    pub fn prune(&mut self, now: i64) -> usize {
        let expired: Vec<String> = self
            .map
            .iter()
            .filter(|(_, e)| now - e.last_used > self.ttl_ms)
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired {
            self.map.pop(key);
        }
        expired.len()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Live bindings per region.
    pub fn counts_by_region(&self, now: i64) -> std::collections::BTreeMap<String, i64> {
        let mut out = std::collections::BTreeMap::new();
        for (_, e) in self.map.iter() {
            if now - e.last_used <= self.ttl_ms {
                *out.entry(e.record.region.clone()).or_insert(0) += 1;
            }
        }
        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindOutcome {
    Created,
    Unchanged,
    Moved { from: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_reports_creation_moves_and_no_ops() {
        let mut b = Bindings::new(10, 1000);
        assert_eq!(b.bind("k", "a", "n1", 0), BindOutcome::Created);
        assert_eq!(b.bind("k", "a", "n1", 10), BindOutcome::Unchanged);
        assert_eq!(b.get("k", 10).unwrap().updated_at, 0);
        assert_eq!(b.bind("k", "b", "n1", 20), BindOutcome::Moved { from: "a".into() });
        assert_eq!(b.get("k", 20).unwrap().region, "b");
    }

    #[test]
    fn idle_bindings_expire() {
        let mut b = Bindings::new(10, 1000);
        b.bind("k", "a", "n1", 0);
        assert!(b.get("k", 900).is_some());
        assert!(b.get("k", 1800).is_some(), "use refreshes the idle timer");
        assert!(b.get("k", 2801).is_none());
        assert_eq!(b.bind("k", "a", "n1", 3000), BindOutcome::Created);
    }

    #[test]
    fn merge_is_last_writer_wins() {
        let mut b = Bindings::new(10, 10_000);
        let rec = |region: &str, at: i64, node: &str| BindingRecord { key: "k".into(), region: region.into(), updated_at: at, node: node.into() };
        assert!(b.merge(rec("a", 5, "n1"), 10));
        assert!(!b.merge(rec("b", 4, "n2"), 10));
        assert!(b.merge(rec("c", 6, "n2"), 10));
        assert!(!b.merge(rec("c", 6, "n2"), 10));
        assert_eq!(b.get("k", 10).unwrap().region, "c");
        assert_eq!(b.changed_since(6).len(), 1);
        assert!(b.changed_since(7).is_empty());
    }
}
