# 0003 - A Session Is Its Prompt Cache Key, Sticky Until Its Region Has Problems

## Status

Accepted

## Context

The upstream prompt cache is local to the serving cluster. Moving a conversation to another region throws away a warm cache: time to first token goes up, and on a subscription more uncached input tokens are billed against quota. underclass already keys stickiness on `prompt_cache_key` (underclass ADR 0003). Time-of-day steering ("chase the sun", ADR 0007) wants to move traffic around, which would fight the cache if it applied to running sessions.

## Decision

- **Session key.** A session is identified by the request body's `prompt_cache_key` (else `promptCacheKey`) and nothing else. Requests without one belong to no session and are spread by request id.
- **Bindings.** A binding maps `sha256(key)` to a region. A session stays in its bound region until that region turns red, has no live egress node, or fails this request with a capacity error.
- **Moves.** A moved session is bound to its new region and stays there until *that* region has problems. It never moves back just because the old region recovered, and it is never rebalanced when idle.
- **All red.** When every available region is red, a bound session stays where it is. Its cache is the only advantage left.
- **Placing new sessions.** New sessions are placed by weighted rendezvous hashing, so every node agrees without coordination.
- **Storage and sharing.** Bindings are kept in an LRU with a 24 h idle TTL. Each node gossips recently changed bindings. A node that discovers a peer pulls its full binding table once, and merges are last-writer-wins on `(updated_at, node, region)`. Any ingress node therefore honours a binding written elsewhere.

## Consequences

- A session loses its cache only when its region is actually in trouble, or (at most once) when a binding has not propagated yet.
- Chase-the-sun only influences where new sessions land.
- Raw prompt cache keys never leave the ingress node; only their hashes are stored, logged, or gossiped.
