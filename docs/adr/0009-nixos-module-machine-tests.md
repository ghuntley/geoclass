# 0009 - Nix Flake, NixOS Module, and Machine Tests

## Status

Accepted

## Context

GeoClass is deployed on NixOS hosts in several regions. underclass ships a flake with a package, an app, an overlay, a NixOS module, and a machine test (underclass `flake.nix`).

## Decision

The flake mirrors underclass:

- **Outputs.** `packages.{geoclass,default}` (with a `gtop` wrapper), `apps.{default,gtop}`, `overlays.default`, `nixosModules.default`, and the devenv shell.
- **The `services.geoclass` module:**
  - renders operator region data into `config.toml` without interpreting it;
  - takes the mesh key and UI token only from `environmentFile`;
  - opens the mesh port only on `meshInterfaces` (default `tailscale0`) and never the ingress port;
  - orders after `tailscaled` for discovery, runs under `DynamicUser` with systemd hardening, and exposes `/run/geoclass/monitor.sock`.
- **Machine tests** (Linux `checks`):
  - `geoclass-vm` checks a single node: listener surfaces, auth, pass-through of capacity and quota failures, no secrets in the journal, and statistics surviving a restart.
  - `geoclass-mesh-vm` runs three regions plus an outsider with static discovery. It checks retry before output, sticky sessions across nodes, account errors passing through, cluster statistics, mesh-key enforcement, and peer loss.
  - `geoclass-tailscale-vm` runs headscale plus two tagged nodes and an untagged one. It checks LocalAPI discovery, relaying through a discovered peer, `whois` tag enforcement, and that the mesh port is closed off the tailnet.

## Consequences

- The VM tests need a Linux builder. On macOS, only module evaluation can be checked, against plain nixpkgs.
