# 0010 - Corroborated Failure-Domain Learning

## Status

Accepted. Amends the learning rule in 0006.

## Context

ADR 0006 merged two regions into one failure domain only when their per-minute failure rates correlated at 0.8 or more. Overload episodes are rare, so in practice that threshold is reached slowly, and retries keep wasting an attempt on a region that shares a pool with the one that just failed.

Two more signals are cheap to collect:

- **Time to first output.** When two regions share a serving pool, load shows up in both regions' TTFT, not only in outright failures.
- **The Cloudflare colo** (`cf-ray` suffix). This identifies the edge, not the origin pool, so a shared colo on its own proves little. Different regions whose egress lands at the same colo do share a network path, though, and that makes a weaker failure correlation more believable.

## Decision

- Each minute, every node records each region's failure rate and mean TTFT from the merged health window. It also notes the colo of each region's egress.
- Each egress node keeps its last 64 colos and reports the most common one as `colo` in its `NodeInfo` (gossip and `/mesh/info`). A region's colo is the most common value among its live nodes. Every node therefore sees the same colos, as it already sees the same health.
- **When regions merge.** `domains::same_pool` merges two regions when either holds:
  - their failure correlation is 0.8 or more (unchanged), or
  - their failure correlation is between 0.6 and 0.8, and is corroborated by a shared colo or by a TTFT correlation of 0.8 or more.
- **Never on corroboration alone.** A shared colo or correlated TTFT without correlated failures never merges anything, because TTFT also rises with global demand.
- **Unchanged from 0006:** learning only merges and never splits an operator group, and the minimum sample and episode rules still apply to the failure correlation.

## Consequences

- Domains that really share a pool are found sooner, so retries leave a failing pool earlier.
- A false merge costs only retry ordering: a merged region is still tried, just later. Sessions and placement are unaffected.
- The thresholds are constants in `src/domains.rs`, covered by unit tests and a Hegel property (learning never splits a configured group, and the result does not depend on region order).
