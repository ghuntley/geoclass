# 0007 - Chase-the-Sun Priors as Configuration

## Status

Accepted

## Context

Upstream demand follows the working day, and US working hours dominate. Capacity is more likely to be spare in a region whose clients are asleep. Bound sessions must not move for this reason (ADR 0003), but new sessions can be steered at no cache cost.

## Decision

- **Configured weight.** Each region's weight for new sessions is its `weight` (default 1) times an hour-of-day factor taken from:
  - its `weekend` table (24 UTC hourly weights) on Saturday or Sunday UTC, when set;
  - else its `hourly` table;
  - else the `utc_offset_hours` + `peak_hours` + `peak_weight` shorthand: `peak_weight` inside the local weekday peak window, 1 outside it.
- **Learned prior.** Each node keeps an hour-of-week moving average of every region's observed failure rate. `learned_prior_weight` (default 0.5) pulls the configured weight toward `1 - learned rate`.
- **Health.** The live health state always applies on top: amber halves a region's weight, and red removes the region unless every region is red.

## Consequences

- With no configuration all regions weigh the same, and only health steers traffic.
- Prior changes larger than 25% between minutes are recorded as `prior_shift` events.
- The example config in the README encodes the October 2026 research. It is a starting point, not knowledge built into the code.
