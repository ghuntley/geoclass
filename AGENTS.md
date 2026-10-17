# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## What this is

`geoclass` is a geo-aware egress router for [underclass](https://github.com/ghuntley/underclass). underclass keeps the ChatGPT/Codex accounts and points `UNDERCLASS_CODEX_UPSTREAM` at a local GeoClass node. GeoClass nodes in several regions mesh over Tailscale and decide which region each request leaves from.

- When a region returns capacity errors (`server_is_overloaded` inside a 200 stream), the request is retried from another region before any output reaches the client.
- Sessions, identified by `prompt_cache_key`, stick to a region until that region has trouble, so upstream prompt caches stay warm.
- New sessions follow operator-defined chase-the-sun weights.

Routing is stateless: all of it is rebuilt from gossip. Statistics live in a per-node SQLite file. Architecture decisions live in `docs/adr/`; read 0001, 0003, and 0004 first.

## Commands

```sh
cargo build                 # build
cargo test                  # unit, e2e (3 in-process nodes), and Hegel property tests (all must pass)
cargo clippy --all-targets  # must be warning-free
cargo run -- serve --region <name>
cargo run -- check-config --region <name>
cargo run -- stats [--since 24h] [--by region|ingress|model|region_model|account|domain] [--scope cluster|local] [--json]
cargo run -- top            # or gtop from the Nix package
nix build .#checks.x86_64-linux.geoclass-vm          # NixOS machine tests (Linux builder required)
nix build .#checks.x86_64-linux.geoclass-mesh-vm
nix build .#checks.x86_64-linux.geoclass-tailscale-vm
```

## Testing policy (enforced)

1. **Unit tests** (`#[test]`, in-module) cover exact behaviour: header forwarding, gate verdicts, hysteresis thresholds, config validation.
2. **Property tests** use [Hegel](https://hegel.dev) (`hegeltest`, pinned) in `tests/properties.rs`. They cover invariants over the pure core: placement order-independence and stickiness, retry sequences, gate byte-exactness, binding convergence, health hysteresis, and cluster stats merging. New pure-logic modules must ship properties.
3. **End-to-end tests** in `tests/e2e.rs` mesh three in-process nodes against mock upstreams.
4. **NixOS machine tests** in `nixos/tests/` cover the service, a three-region mesh, and real Tailscale discovery via headscale.
5. **No mutation testing** (ADR 0002).

## Architecture rules

- **Pure sync core, async at the edges.** `placement`, `health`, `bindings`, `domains`, `prior`, `gate`, `sse`, and `stats` take time as a parameter and do no I/O.
- **Regions are opaque.** Never match on, default, or special-case a region name. All region knowledge is operator config (ADR 0006).
- **Retry only before output.** Only capacity codes, 5xx, and transport errors are retried, and only before the gate commits (ADR 0004). Account-scoped errors always pass through.
- **A session is its prompt cache key**, and it is sticky until its region has trouble (ADR 0003).
- **Routing never reads the stats store** (ADR 0005, ADR 0008).

## Security invariants

- Never log, store, or gossip headers, bodies, raw prompt cache keys, raw account ids, the mesh key, or the UI token. `src/logging.rs` is the only place that formats routing identity, using hashes truncated to 12 characters.
- Strip every `x-geoclass-*` header before egress.
- Mesh routes require the mesh key and, with Tailscale discovery, a `whois` tag check. The admin API requires the UI token. The monitor socket serves only the snapshot.

## Code contracts

Use the `code-contracts` skill for every code change and code review. Follow its contract discovery, writing, and enforcement procedures before submitting commits or pull requests.

- Repo-wide rules live in [`CONTRACTS`](CONTRACTS) and [`nixos/CONTRACTS`](nixos/CONTRACTS). Declaration contracts use `/// @cc` doc comments on the narrowest relevant function.
- `owner` is the account's GitHub username (`ghuntley`); never add `notify` unless explicitly requested.
- Validate syntax and duplicate IDs with `cc-check format` (npm `@spolu/cc-check`). There is no automated semantic enforcement, so verify the prose against the implementation for every touched contract.

## ADR policy

- Every non-trivial architectural decision gets a MADR record in `docs/adr/NNNN-title.md` (Status / Context / Decision / Consequences).
- To supersede a decision, mark the old ADR `Superseded by NNNN` and write the new one. Never delete ADRs.
- Index:
  - 0001 Rust/Axum egress relay
  - 0002 Hegel PBT, no mutation testing
  - 0003 session stickiness
  - 0004 retry before output
  - 0005 stateless gossip and Tailscale discovery
  - 0006 opaque regions and failure domains
  - 0007 chase-the-sun priors as config
  - 0008 statistics store and cluster fan-out
  - 0009 Nix flake, NixOS module, and machine tests

## Files agents may edit

`AGENTS.md`, `README.md`, `docs/adr/*`, `CONTRACTS`, `Cargo.toml`, `flake.nix`, `nixos/*`, `examples/*`, `.github/*`, and everything under `src/` and `tests/`. Do not touch `devenv.nix`/`devenv.lock` unless the task explicitly requires it. Never commit `*.bak`, `stats.db`, or `.hegel/` artifacts.
