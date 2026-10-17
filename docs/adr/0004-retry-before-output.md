# 0004 - Retry Before Output (the Gate)

## Status

Accepted

## Context

Capacity failures arrive inside an HTTP 200 stream. `response.created` comes first, then `response.failed` with `server_is_overloaded`, at a median of 4.8 s (underclass ADR 0021). underclass fails over only before the first byte, so these failures always reach the user. Nothing useful has been sent at that point, though: no model output has been produced.

## Decision

Every 2xx attempt passes through a pure gate (`gate.rs`) that holds bytes until the stream proves itself:

- **Model output** (any `*.delta`, `response.output_item.added`, or chat `choices`) commits. The held bytes are released, and every later byte passes straight through. A committed stream is never retried.
- **A capacity failure** (`overload`/`capacity`/`slow_down` codes) seen before any output means retry in another region. The held bytes are discarded and the client never sees them.
- **Any other terminal event** (completed, incomplete, a non-capacity failure such as a policy refusal or quota) commits. It belongs to the client.
- **Holding too long:** more than `gate_max_bytes` (256 KiB) held commits. A transport error while holding means retry. A clean end of stream while holding commits.

Before any body arrives, a 5xx or a capacity error body means retry, and anything else (including 429 quota) passes through. The order of events within a single chunk is respected. When every region fails, the last failure is returned exactly as received, so underclass still records the capacity event.

## Consequences

- The client sees exactly one attempt's bytes, and the released bytes are the upstream's bytes exactly (a Hegel property).
- Time to first byte rises to time to first output for every request. That costs nothing in practice, because clients render nothing before output anyway.
- Retries are bounded by `retry_budget` regions and `retry_deadline_ms`.
