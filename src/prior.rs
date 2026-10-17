//! Chase-the-sun: time-of-day weights that steer where *new* sessions land.
//!
//! The configured prior is operator data (`[[regions]]` `hourly`/`weekend` tables or the
//! `utc_offset_hours` + `peak_hours` shorthand). The learned prior is an hour-of-week moving
//! average of each region's observed failure rate. Neither ever moves a bound session.
//! Pure and synchronous; time is a parameter.
use crate::config::RegionConfig;
use std::collections::BTreeMap;

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;
const SLOTS: usize = 7 * 24;
const ALPHA: f64 = 0.1;
/// Floor on a learned factor so a region that failed every hour last week is still tried.
const MIN_FACTOR: f64 = 0.05;

/// UTC hour of day and whether the UTC day is Saturday or Sunday.
pub fn utc_hour_and_weekend(ts_ms: i64) -> (usize, bool) {
    let days = ts_ms.div_euclid(DAY_MS);
    let hour = (ts_ms.rem_euclid(DAY_MS) / HOUR_MS) as usize;
    // 1970-01-01 was a Thursday; 0 = Sunday.
    let weekday = (days + 4).rem_euclid(7);
    (hour, weekday == 0 || weekday == 6)
}

/// Hour-of-week slot, Sunday 00:00 UTC = 0.
pub fn slot(ts_ms: i64) -> usize {
    let days = ts_ms.div_euclid(DAY_MS);
    let weekday = (days + 4).rem_euclid(7) as usize;
    weekday * 24 + (ts_ms.rem_euclid(DAY_MS) / HOUR_MS) as usize
}

/// @cc [owner:ghuntley,label:routing] configured-prior
/// `configured_weight` MUST be the region's `weight` (default 1) times: the `weekend` table's
/// UTC-hour entry on Saturday/Sunday UTC when set, else the `hourly` table's UTC-hour entry when
/// set, else `peak_weight` (default 0.5) when the region-local hour from `utc_offset_hours` falls in
/// `peak_hours` (`[start, end)`, wrapping past midnight when start > end) on a local weekday, else 1.
/// It MUST NOT depend on the region's name.
pub fn configured_weight(region: &RegionConfig, ts_ms: i64) -> f64 {
    let (hour, weekend) = utc_hour_and_weekend(ts_ms);
    let base = region.weight.unwrap_or(1.0).max(0.0);
    let hourly = match (&region.weekend, &region.hourly) {
        (Some(table), _) if weekend => Some(table[hour]),
        (_, Some(table)) => Some(table[hour]),
        _ => None,
    };
    let factor = hourly.unwrap_or_else(|| match (region.utc_offset_hours, region.peak_hours) {
        (Some(offset), Some([start, end])) => {
            let local = ts_ms + (offset * HOUR_MS as f64) as i64;
            let (local_hour, local_weekend) = utc_hour_and_weekend(local);
            let (start, end) = (start as usize, end as usize);
            let in_peak = if start <= end { (start..end).contains(&local_hour) } else { local_hour >= start || local_hour < end };
            if in_peak && !local_weekend { region.peak_weight.unwrap_or(0.5).max(0.0) } else { 1.0 }
        }
        _ => 1.0,
    });
    base * factor
}

/// Hour-of-week moving average of each region's failure rate.
#[derive(Default)]
pub struct LearnedPrior {
    rates: BTreeMap<String, Vec<Option<f64>>>,
}

impl LearnedPrior {
    /// Folds one sample (a region's window failure rate) into the current hour-of-week slot.
    pub fn observe(&mut self, region: &str, rate: f64, ts_ms: i64) {
        let slots = self.rates.entry(region.to_string()).or_insert_with(|| vec![None; SLOTS]);
        let s = &mut slots[slot(ts_ms)];
        let rate = rate.clamp(0.0, 1.0);
        *s = Some(match *s {
            Some(previous) => previous + ALPHA * (rate - previous),
            None => rate,
        });
    }

    /// `1 - learned failure rate` for the slot, floored; 1 when nothing was learned.
    pub fn factor(&self, region: &str, ts_ms: i64) -> f64 {
        self.rates
            .get(region)
            .and_then(|slots| slots[slot(ts_ms)])
            .map(|rate| (1.0 - rate).max(MIN_FACTOR))
            .unwrap_or(1.0)
    }

    /// The learned failure-rate table for one region (168 hour-of-week slots).
    pub fn table(&self, region: &str) -> Option<&[Option<f64>]> {
        self.rates.get(region).map(Vec::as_slice)
    }
}

/// The weight a region gets for new sessions at `ts_ms`: the configured prior pulled toward the
/// learned factor by `learned_weight` (0 = configured only, 1 = fully learned).
pub fn weight(region: &RegionConfig, learned: &LearnedPrior, learned_weight: f64, ts_ms: i64) -> f64 {
    let lambda = learned_weight.clamp(0.0, 1.0);
    configured_weight(region, ts_ms) * ((1.0 - lambda) + lambda * learned.factor(&region.name, ts_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region() -> RegionConfig {
        RegionConfig { name: "x".into(), ..Default::default() }
    }

    #[test]
    fn calendar() {
        // 1970-01-03 was a Saturday.
        assert_eq!(utc_hour_and_weekend(2 * DAY_MS + 5 * HOUR_MS), (5, true));
        assert_eq!(utc_hour_and_weekend(0), (0, false));
        assert_eq!(slot(3 * DAY_MS), 0, "1970-01-04 was a Sunday");
    }

    #[test]
    fn hourly_and_weekend_tables() {
        let mut r = region();
        let mut hourly = vec![1.0; 24];
        hourly[14] = 0.25;
        r.hourly = Some(hourly);
        r.weight = Some(2.0);
        assert_eq!(configured_weight(&r, 14 * HOUR_MS), 0.5);
        assert_eq!(configured_weight(&r, 13 * HOUR_MS), 2.0);
        r.weekend = Some(vec![0.1; 24]);
        assert!((configured_weight(&r, 2 * DAY_MS + 14 * HOUR_MS) - 0.2).abs() < 1e-9);
        assert_eq!(configured_weight(&r, 14 * HOUR_MS), 0.5, "weekday unaffected");
    }

    #[test]
    fn peak_shorthand_uses_local_time_and_wraps() {
        let mut r = region();
        r.utc_offset_hours = Some(10.0);
        r.peak_hours = Some([9, 17]);
        // 00:00 UTC Thursday is 10:00 local.
        assert_eq!(configured_weight(&r, 0), 0.5);
        assert_eq!(configured_weight(&r, 8 * HOUR_MS), 1.0);
        r.peak_hours = Some([22, 2]);
        r.utc_offset_hours = Some(0.0);
        assert_eq!(configured_weight(&r, 23 * HOUR_MS), 0.5);
        assert_eq!(configured_weight(&r, DAY_MS + HOUR_MS), 0.5);
        assert_eq!(configured_weight(&r, 12 * HOUR_MS), 1.0);
    }

    #[test]
    fn learned_prior_moves_toward_observed_rates() {
        let mut l = LearnedPrior::default();
        assert_eq!(l.factor("x", 0), 1.0);
        l.observe("x", 1.0, 0);
        assert_eq!(l.factor("x", 0), MIN_FACTOR);
        l.observe("x", 0.0, 0);
        assert!((l.factor("x", 0) - 0.1).abs() < 1e-9);
        assert_eq!(l.factor("x", HOUR_MS), 1.0, "other slots untouched");
        let r = region();
        assert!((weight(&r, &l, 0.5, 0) - 0.55).abs() < 1e-9);
        assert_eq!(weight(&r, &l, 0.0, 0), 1.0);
    }
}
