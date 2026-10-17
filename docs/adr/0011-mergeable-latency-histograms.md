# 0011 - Mergeable Latency Histograms

## Status

Accepted. Amends the latency reporting in 0008.

## Context

ADR 0008 reported latency as means only: TTFT, gate hold time, and the relay's time to headers. Means hide the tail, which is what users notice. Percentiles have to survive two merges:

- the health window gossiped between nodes;
- the cluster stats fan-out.

Exact percentiles cannot be merged from per-node summaries.

The plan also asked for the relay's upstream connect and TLS times. reqwest does not expose per-request connection timings, and pooled connections often have none to report.

## Decision

- **Fixed-bucket histogram.** `hist::Hist` has 21 buckets with upper bounds from 5 ms to 60 s, plus an overflow bucket. Merging is element-wise addition. A percentile is the upper bound of the bucket that reaches the requested rank, so it is an upper estimate within one bucket's width.
- **Health windows** carry a TTFT histogram per (region, model). `health_samples` stores the p50 and p95 each minute, next to the mean.
- **Stats totals** carry histograms for TTFT, gate hold, and the relay hop. SQLite computes them with a `CASE` expression generated from the same bounds, so the SQL and Rust bucketing cannot drift. A test checks that they agree.
- **Relay hop instead of connect/TLS.** For attempts sent through a peer, the ingress records `hop_ms`: its own time to response headers minus the peer's `x-geoclass-headers-ms`. That is the latency the mesh adds, which is the number that matters for routing. Egress nodes also keep a histogram of upstream time to headers for the requests they relayed.
- **Bounds are sent with the stats.** `hist_bounds_ms` is included in stats responses so the web UI computes percentiles from the bounds the server actually uses.

## Consequences

- **The bucket bounds are part of the gossip and stats wire format.** Changing them needs every node upgraded together.
- Connect and TLS time are not reported separately; a slow handshake shows up in the hop and in time to headers.
- Older stats files gain the new columns (`attempts.hop_ms`, `health_samples.ttft_p50_ms`, `ttft_p95_ms`) in place when opened.
