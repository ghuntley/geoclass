# 0005 - Stateless Gossip Mesh with Tailscale Discovery

## Status

Accepted

## Context

GeoClass runs on many servers that must agree, roughly, on which regions are healthy and where sessions are bound. Consensus systems (Raft, etcd) add operational weight and an availability dependency, and none of this routing state needs strong consistency. The servers already share a tailnet.

## Decision

- **Membership is the tailnet.** Each node asks its local `tailscaled` LocalAPI (`/localapi/v0/status`) every `discovery_interval_ms` for online peers carrying `tag:geoclass`. It then calls `GET /mesh/info` on each to learn its node id and region; a node's region is configured on that node. A static `peers` list replaces discovery for tests and for setups without Tailscale.
- **Authentication.** Every `/mesh/*` and `/relay/*` call must carry the shared mesh key. With Tailscale discovery, the receiver also checks through LocalAPI `whois` that the caller's node carries the tag. The NixOS module opens the mesh port only on `tailscale0`.
- **Gossip** is push-pull every `gossip_interval_ms` (2 s). Each digest carries the sender's own health window per egress region and model, its recently changed bindings, and its in-flight count. Health is the sum of the latest windows from all live nodes.
- **Liveness.** A peer that misses 3 rounds is stale and leaves placement. A peer that leaves the tailnet status is removed.
- **No database for routing.** All routing state is soft and rebuilt from gossip within seconds of a restart. The statistics store (ADR 0008) is never read by routing.

## Consequences

- Nodes can briefly disagree. The cost is at most one cache miss or one extra retry.
- Every node gossips directly with every other node, which is fine for tens of nodes.
- A node without a mesh key runs alone and serves only its own region.
