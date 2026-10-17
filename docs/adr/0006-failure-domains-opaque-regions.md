# 0006 - Operator-Defined Regions and Failure Domains

## Status

Accepted. The learning rule is amended by [0010](0010-corroborated-failure-domain-learning.md).

## Context

Where our nodes are tells us little about where the upstream serves them. Research in October 2026 found:

- **US.** Large US capacity (Azure plus Stargate sites).
- **Europe.** An EU in-region processing pool exists.
- **Australia.** Residency there is storage-only, so Australian egress is probably served from APAC or US capacity.
- **Routing is opaque.** There is no public map from client egress to serving cluster, and chatgpt.com sits behind Cloudflare anycast.

Two egress regions can therefore share one serving pool, in which case retrying from one to the other is pointless. The operator also asked that no region be named in code.

## Decision

- **Regions are opaque strings** supplied by the operator. Each node's `region` and the `[[regions]]` table hold all per-region data. No code matches on a region name.
- **Failure domains.** An optional `failure_domain` groups regions the operator knows share a pool. Each minute, every node also records each region's failure rate. Regions whose rates correlate at 0.8 or more over at least 30 shared minutes, with at least 3 episode minutes each, are merged into one domain. Learning only merges; it never splits an operator group.
- **Retry order.** Retries prefer regions whose domain this request has not tried yet.
- **Evidence.** Each attempt records the Cloudflare colo from `cf-ray` and its failure domain, so the operator can see where egress actually lands.

## Consequences

- Moving GeoClass to new locations is a configuration change only.
- Domain learning needs real overload episodes before it merges anything; until then every region is treated as independent.
