# 0002 - Property-Based Testing with Hegel, No Mutation Testing

## Status

Accepted

## Context

GeoClass's routing core is the same kind of invariant-heavy logic as underclass's pool, where example-based tests under-specify behaviour. Examples: whether a bound session stays put, whether retries ever revisit a region, whether merged bindings converge, and whether the gate ever releases different bytes than the upstream sent. underclass ADR 0002 settled this question for that codebase.

## Decision

Follow underclass ADR 0002:

- Plain `#[test]` unit tests cover exact behaviour.
- Hegel (`hegeltest`, pinned) property tests in `tests/properties.rs` cover invariants. Mutation testing is rejected.
- `placement`, `health`, `bindings`, `domains`, `prior`, `gate`, `sse`, and `stats` are pure and synchronous, with time passed in, so properties can drive them.
- `tests/e2e.rs` meshes three in-process nodes against mock upstreams.

## Consequences

- New pure-logic modules ship with properties.
- A failing property is a contract question: fix the code or tighten the property, never loosen the assertion. Two such failures were found while building the first version:
  - output after a capacity failure in the same chunk;
  - bound sessions when every region is red.
