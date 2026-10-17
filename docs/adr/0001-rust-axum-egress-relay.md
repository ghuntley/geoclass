# 0001 - Rust/Axum Egress Relay in Front of underclass

## Status

Accepted

## Context

underclass pools ChatGPT/Codex subscriptions and handles account-scoped failures: quota cooldowns, credential refresh, and stickiness to an account. Capacity failures (`server_is_overloaded`, "Selected model is at capacity") are different. They hit every account at once because they come from the upstream serving region, not from the subscription (underclass ADR 0021). The operator has seen capacity vary with the egress location and runs servers in several regions, all on one tailnet.

Two topologies were considered:

- **GeoClass owns accounts.** Each region runs its own underclass and GeoClass routes between them. Every region holds credentials, and quota state is split across instances.
- **Egress relay.** One underclass keeps every account and injects tokens as it does today. It points `UNDERCLASS_CODEX_UPSTREAM` at a local GeoClass node, which decides which region's node sends the request to the upstream.

## Decision

GeoClass is an egress relay written in Rust on Axum/Tokio with reqwest, the same stack as underclass (underclass ADR 0001).

- underclass needs no code change.
- GeoClass forwards request headers (including the account token) unchanged, minus hop-by-hop and `x-geoclass-*` headers. It never stores or logs them.
- Responses come back unchanged. Account-scoped errors (429 quota, 401/403) pass straight through so underclass's health machine still sees them.
- Each node listens on two TCP ports and one Unix socket:
  - **ingress** on `127.0.0.1:9090`, for underclass;
  - **mesh** on `:9091`, for peers: gossip, relay, and metrics;
  - **monitor socket**, for `geoclass top`.

## Consequences

- Credentials exist in one place. Bearer tokens cross the tailnet only inside WireGuard, from GeoClass to GeoClass.
- An account's requests can leave from several countries within minutes. That may look like account sharing to the upstream; per-account statistics (`account` dimension, hashed) make it visible.
- Only the Codex path is relayed. Copilot keeps talking directly to its upstream.
