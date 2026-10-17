# 0008 - Per-Node Statistics with Cluster Fan-Out

## Status

Accepted. Latency reporting is amended by [0011](0011-mergeable-latency-histograms.md).

## Context

The operator wants statistics as comprehensive as underclass's (underclass ADR 0021), across the whole mesh. GeoClass routing must stay stateless.

## Decision

- **Store.** Each node keeps an append-only SQLite file (`stats.db`) that routing never reads. Tables:
  - `attempts`, one row per upstream attempt, written by the ingress node:
    - request id, attempt number, timing, and model;
    - hashed session and account;
    - ingress and egress region and node, failure domain, Cloudflare colo, and placement reason;
    - status, gate result, stream outcome, and sanitized error code;
    - time to first output and gate hold time;
    - input, cached input, and output tokens;
    - overload-hit and rescued flags.
  - `relay_attempts`, egress done for other nodes.
  - `events`, covering region state changes, session moves, retries succeeding or exhausting, peers joining, going stale, or leaving, prior shifts, and domain changes.
  - `health_samples`, one per minute per region and model, so routing decisions can be replayed.
  - `stats_daily`, finished-day rollups as in underclass.
- **Reported.** Requests, attempts, sessions, tokens, and cache-hit ratio split by whether the session had just moved. Also concurrency, outcomes, gate results, placement reasons, events, mean time to first output and hold time, and the rescue rate (rescued ÷ overload-hit requests). All of these are bucketed by minute, hour, or day. They can be broken down by total, egress region, ingress region, model, region × model, account, or failure domain.
- **Cluster view.** `scope=cluster` sends the same pinned window to every live peer's `/mesh/stats` and merges the answers. Request and session counts are summed and flagged as upper bounds.
- **Retention.** `stats_retention_days` (default 730), with a 15-minute maintenance job as in underclass.

## Consequences

- Losing a node loses only that node's history.
- Surfaces: `geoclass stats`, `geoclass top` (`gtop`), the web UI, `/admin/api/{stats,health,routing,monitor}`, and Prometheus text at `/metrics`.
