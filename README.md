# geoclass

**A geo-aware egress router for [underclass](https://github.com/ghuntley/underclass). It routes OpenAI requests around regional capacity errors while keeping each session's prompt cache warm.**

```
                                   tailnet (WireGuard, tag:geoclass)
                         ┌───────────────────────────────────────────────────┐
 opencode ─► underclass ─┤► geoclass@us ──────────────────────────► chatgpt.com  (from the US)
             (accounts,  │      │  gossip: health · bindings · peers          │
              tokens)    │      ├──────► geoclass@de ───────────────► chatgpt.com  (from Germany)
                         │      └──────► geoclass@au ───────────────► chatgpt.com  (from Australia)
                         └───────────────────────────────────────────────────┘
          UNDERCLASS_CODEX_UPSTREAM=http://127.0.0.1:9090/backend-api/codex/responses
```

When OpenAI runs out of capacity in a region, Codex streams `response.created`, then (about 5 s later) `response.failed: server_is_overloaded`. The client shows "Selected model is at capacity", and switching subscriptions doesn't help. GeoClass holds each response until real model output appears. If a capacity failure comes first, it quietly sends the same request out from another region. The client never sees the failed attempt.

---

## Why

- **Capacity is regional; quota is per account.** underclass already handles quota. GeoClass handles capacity by choosing where requests leave from, and passes every account-scoped error straight back to underclass.
- **Prompt caches are local to a serving cluster.** Moving a conversation between regions makes the next turn slower and burns more uncached tokens. GeoClass keeps a session in its region until that region is actually in trouble.
- **No coordinator.** Every node meshes with the others over Tailscale and rebuilds its routing state from gossip. There is nothing to back up and no leader to lose.

## Quick start

On every server (each in its own region, all on the same tailnet):

```sh
export GEOCLASS_MESH_KEY="<same random value on every node>"   # openssl rand -hex 32
export GEOCLASS_UI_TOKEN="$(openssl rand -hex 32)"
nix run github:ghuntley/geoclass -- serve --region us           # de, au, … — any names you like
```

```
geoclass node us-1 (region us)
ingress: http://127.0.0.1:9090  (UNDERCLASS_CODEX_UPSTREAM=http://127.0.0.1:9090/backend-api/codex/responses)
mesh:    http://0.0.0.0:9091
web ui:  http://127.0.0.1:9090/
```

Then point underclass at its local node:

```sh
UNDERCLASS_CODEX_UPSTREAM=http://127.0.0.1:9090/backend-api/codex/responses underclass serve
```

Tag every GeoClass machine in your tailnet policy, and let only tagged nodes reach the mesh port:

```jsonc
{
  "tagOwners": { "tag:geoclass": ["autogroup:admin"] },
  "acls": [
    { "action": "accept", "src": ["tag:geoclass"], "dst": ["tag:geoclass:9091"] }
  ]
}
```

```sh
tailscale up --advertise-tags=tag:geoclass
```

That's it. Nodes find each other through the tailnet, and underclass needs no code change.

## How routing works

- **Sessions.** A session is its `prompt_cache_key` (opencode sends one when `setCacheKey: true`; `underclass connect` sets that up).
  - **New sessions** get a region by weighted rendezvous hashing, so every node agrees without coordination.
  - **Bound sessions** stay in their region until it turns red, loses every live node, or fails the request.
  - **Moved sessions** stick to their new region until *that* region has problems. They never bounce back just because the old region recovered.
  - **Requests without a key** are spread by request id and never bound.
- **Retry before output.** Every 2xx response is held in a gate until the first model output: `*.delta`, `response.output_item.added`, or chat `choices`.
  - **A capacity failure before output** means the attempt is retried from a region in a failure domain this request hasn't tried yet. The held bytes are thrown away.
  - **Output, a completed or incomplete response, or any other failure** (policy refusal, quota) commits. The held bytes go to the client, then everything streams straight through.
  - **Committed streams are never retried.** Holding more than 256 KiB also commits.
  - **Before any body:** a 5xx or a capacity error body is retried; everything else (429 quota, 401, 403) passes straight through.
  - **If every region fails,** the last failure is returned exactly as it was received, so underclass still records it.
- **Health.** Each node counts its own attempts per egress region and model over a 5-minute window (attempts, overloads, errors, time to first output) and gossips the sums. Every node adds up the latest window from all live nodes.
  - A region turns **red** at a 10% failure rate (with at least 5 attempts) and goes back to **green** only at 3% or below.
  - **Amber** halves a region's weight for new sessions.
  - A red region with no new failures goes green once its window empties, so traffic probes it again.
- **Failure domains.** Two egress regions may share one upstream serving pool, in which case retrying from one to the other is pointless.
  - **Configured:** you can group them with `failure_domain`.
  - **Learned:** regions whose per-minute failure rates correlate at 0.8 or more over at least 30 minutes are merged automatically.
- **Chase the sun.** New sessions favour regions whose working day is over.
  - **Configured weights:** each region gets 24 UTC hourly weights (and optionally weekend ones), or a simpler `utc_offset_hours` + `peak_hours` + `peak_weight`.
  - **Learned weights:** each node also learns an hour-of-week failure rate per region and blends it in (`learned_prior_weight`).
  - **No moves:** this never moves a bound session.
- **Regions are yours.** GeoClass never interprets a region name. `us`, `de`, and `au` below are just names from the example config.

Design decisions and their trade-offs live in [`docs/adr/`](docs/adr). Start with [0003](docs/adr/0003-session-stickiness.md) and [0004](docs/adr/0004-retry-before-output.md).

## Where OpenAI serves from (October 2026)

| region | what is public | implication |
|---|---|---|
| US | Most capacity: Azure plus Stargate. Abilene, TX already serves inference, and six more US sites are under construction ([Epoch AI](https://epoch.ai/blog/openai-stargate-where-the-us-sites-stand), [stargate.how](https://stargate.how/location)). | Busiest during US working hours (≈14:00–01:00 UTC). |
| Europe | In-region *processing* exists for Europe ([OpenAI](https://openai.com/index/introducing-data-residency-in-europe/)); Stargate Norway is planned. | Probably a separate pool from the US, but which pool a German IP lands on isn't documented. |
| Australia | Residency is storage-only; inference residency covers the US, Europe, and the UAE ([ausgpt](https://ausgpt.com.au/resources/ai-data-residency-australia), [Computerworld](https://www.computerworld.com/article/4096675/openai-expands-data-residency-for-enterprise-customers.html)). Status incidents mention an "APAC region" ([status](https://status.openai.com//incidents/01M1NKFZH5EEYEREC54HNAHY35)). | Australian egress is likely served from APAC or US capacity. Let GeoClass learn whether it moves with `us`. |

Capacity errors are regional by OpenAI's own account. A Codex maintainer reportedly described `server_is_overloaded` as low capacity "in your region" ([laozhang](https://blog.laozhang.ai/en/posts/codex-selected-model-at-capacity.md)), and OpenAI has moved traffic between regions by hand during incidents ([write-up](https://status.openai.com/incidents/01KXXDNEAKEPRGFM661SBJJAM6/write-up)). There is no public map from client egress to serving cluster; chatgpt.com sits behind Cloudflare anycast.

**None of this is authoritative.** GeoClass records the Cloudflare colo (`cf-ray`) and the failure domain of every attempt, so `geoclass stats --by region` shows what your egress actually gets.

## Configuration

Use `~/.config/geoclass/config.toml` (or `GEOCLASS_CONFIG`). [`examples/geoclass.toml`](examples/geoclass.toml) is a complete three-region example; validate one with `geoclass check-config`.

| key | default | meaning |
|---|---|---|
| `region` | — (required) | this node's region (`--region`, `GEOCLASS_REGION`) |
| `node_id` | hostname | unique node name (`GEOCLASS_NODE_ID`) |
| `ingress_bind` | `127.0.0.1:9090` | where underclass connects (`GEOCLASS_INGRESS`) |
| `mesh_bind` / `mesh_port` | `0.0.0.0:9091` / its port | peer listener (`GEOCLASS_MESH_BIND`, `GEOCLASS_MESH_PORT`) |
| `advertise_url` | tailnet IPv4 + `mesh_port` | how peers reach this node (`GEOCLASS_ADVERTISE_URL`) |
| `upstream` | `https://chatgpt.com` | egress origin; paths are kept (`GEOCLASS_UPSTREAM`) |
| `discovery` | `tailscale` | `tailscale` (LocalAPI, by tag) or `static` (`GEOCLASS_DISCOVERY`) |
| `tailscale_tag` / `tailscale_socket` | `tag:geoclass` / `/run/tailscale/tailscaled.sock` | discovery and `whois` checks |
| `peers` | `[]` | static peer mesh URLs (`GEOCLASS_PEERS`, comma-separated) |
| `gossip_interval_ms` / `discovery_interval_ms` | `2000` / `15000` | mesh cadence; a peer is stale after 3 missed rounds |
| `retry_budget` / `retry_deadline_ms` | `0` (every region) / `120000` | retry bounds per request |
| `gate_max_bytes` | `262144` | most bytes held before committing anyway |
| `binding_ttl_ms` / `binding_cap` | 24 h / `100000` | session binding idle TTL and LRU size |
| `learned_prior_weight` | `0.5` | how far learned failure history moves the configured prior |
| `domain_learning` | `true` | merge regions whose failures correlate |
| `stats_retention_days` | `730` | statistics retention (`GEOCLASS_STATS_RETENTION_DAYS`) |
| `[thresholds]` | `window_ms=300000, red_rate=0.10, green_rate=0.03, min_samples=5` | health window and hysteresis |
| `[[regions]]` | this node's region only | `name`, `failure_domain`, `weight`, `hourly[24]`, `weekend[24]`, `utc_offset_hours`, `peak_hours=[start,end]`, `peak_weight` |

Secrets come only from the environment: `GEOCLASS_MESH_KEY` (the same on every node; without it a node runs alone) and `GEOCLASS_UI_TOKEN` (minted and stored on first run when absent). Statistics live in `~/.local/share/geoclass/stats.db` (`GEOCLASS_DATA_DIR`).

## Statistics

Every attempt is recorded by the node that received the request:
- ingress and egress region and node;
- failure domain, Cloudflare colo, and placement reason (`new`, `bound`, `moved_red`, `moved_failure`, `moved_unavailable`, `retry`, `unkeyed`);
- gate result (`output`, `terminal`, `overflow`, `eof`, `retried`, `passthrough`, `exhausted`, `abandoned`), stream outcome, and sanitized error code;
- time to first output and gate hold time;
- input, cached input, and output tokens;
- the hashed session and account.

Separately, egress nodes record what they relayed for others; health is sampled every minute per region and model; and events are kept for:
- region state changes;
- session moves;
- rescues and exhausted retries;
- peers joining, going stale, or leaving;
- prior shifts and domain changes.

Below is real output from a two-node smoke test with local mock upstreams. Region `alpha`'s upstream was overloaded. The first request was rescued through `beta`, and the second request in the same session went straight to `beta`, its bound region:

```
$ geoclass stats --since 1h --by region
2026-10-17 06:00 → 2026-10-17 07:28 UTC · hour buckets · scope cluster · 2 node(s)
                     requests attempts retried rescued  rescue   cache    tokens in tokens out     ttft  peak   mean  events
2026-10-17 07:00            2        3       1       1  100.0%   80.0%          200         10      2ms     1   0.00
total                       2        3       1       1  100.0%   80.0%          200         10      2ms     1   0.00
cache hit: stayed 80.0% · moved –   gate hold mean 0ms   gates {"output": 2, "retried": 1}

by region
                     requests attempts retried rescued  rescue   cache    tokens in tokens out     ttft  peak   mean  events
alpha                       1        1       1       0    0.0%       –            0          0        –     1   0.00
beta                        2        2       0       1       –   80.0%          200         10      2ms     1   0.00

relayed for other regions
  from alpha          requests        2 errors      0 mean      1ms to-headers      1ms

requests and sessions are summed across nodes (upper bounds)
```

- **Rescue rate:** requests that hit a capacity failure and were then served from another region, divided by requests that hit one. This is the headline number.
- **Cache hit (stayed vs moved):** what a move costs.
- **`--scope cluster`** (the default) asks every live peer for the same window and merges the answers. Request and session counts are then summed and flagged as upper bounds. `--scope local` reports this node only.
- **`--by`** takes `total`, `region`, `ingress`, `model`, `region_model`, `account`, or `domain`; **`--bucket`** takes `minute`, `hour`, or `day`; **`--json`** prints the raw response.

`geoclass top` (`gtop` from the Nix package) is a live terminal view showing:
- region states, weights, and failure rates;
- live peers and how old their gossip is;
- attempts and retries per minute;
- recent attempts.

Locally it reads `/run/geoclass/monitor.sock` without a token. The web UI at `/` shows the same data plus the statistics panel. Prometheus text is served at `/metrics` on the mesh port.

## CLI

```
geoclass serve [--region NAME] [--log-format auto|json|pretty]
geoclass check-config [--region NAME]
geoclass stats [--since 24h|7d|2y] [--bucket minute|hour|day] [--by …] [--scope cluster|local] [--json] [--url ORIGIN]
geoclass top [--url ORIGIN]
```

With `--url`, `stats` and `top` take the admin token from `GEOCLASS_UI_TOKEN`; a token stored on this machine is never sent to a remote host.

## Endpoints

| listener | route | auth | purpose |
|---|---|---|---|
| ingress | `*` (any other path) | — (loopback only) | upstream requests from underclass |
| ingress, mesh | `GET /` | none | web UI |
| ingress, mesh | `GET /admin/api/monitor` | UI token | live snapshot |
| ingress, mesh | `GET /admin/api/stats` | UI token | statistics (`since`, `from_ms`, `to_ms`, `bucket`, `by`, `scope`) |
| ingress, mesh | `GET /admin/api/health` | UI token | per-minute region health samples (`since`, `model`) |
| ingress, mesh | `GET /admin/api/routing` | UI token | bindings per region, failure domains, configured and learned priors |
| ingress, mesh | `GET /admin/api/peers` | UI token | discovered peers |
| mesh | `GET /mesh/info`, `POST /mesh/gossip`, `GET /mesh/bindings`, `GET /mesh/peers`, `GET /mesh/stats` | mesh key + tag | peer protocol |
| mesh | `ANY /relay/{path}` | mesh key + tag | egress on behalf of a peer |
| mesh | `GET /metrics` | none (tailnet-only port) | Prometheus |
| monitor socket | `GET /monitor` | local only | snapshot for `gtop` |

Every response carries `x-request-id`; a valid UUIDv4 from underclass is preserved (underclass ADR 0012). Routed responses also carry `x-geoclass-region`, `x-geoclass-attempts`, and `x-geoclass-reason`.

## NixOS

The repository is a Nix flake. It provides the `geoclass` package (with `gtop`), apps, an overlay, a devenv shell, and a NixOS module:

```nix
{
  inputs.geoclass.url = "github:ghuntley/geoclass";

  outputs = { nixpkgs, geoclass, ... }: {
    nixosConfigurations.us-1 = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        geoclass.nixosModules.default
        {
          services.tailscale.enable = true;
          services.geoclass = {
            enable = true;
            region = "us";                                   # "de" on the German host, "au" on the Australian one
            environmentFile = "/run/secrets/geoclass.env";   # GEOCLASS_MESH_KEY=…, GEOCLASS_UI_TOKEN=…
            regions = [
              { name = "us"; failureDomain = "us"; utcOffsetHours = -6; peakHours = [ 8 18 ]; peakWeight = 0.5; }
              { name = "de"; failureDomain = "eu"; utcOffsetHours = 1; peakHours = [ 9 18 ]; peakWeight = 0.6; weight = 1.2; }
              { name = "au"; utcOffsetHours = 10; peakHours = [ 9 18 ]; peakWeight = 0.7; }
            ];
            settings.thresholds = { red_rate = 0.1; green_rate = 0.03; };
          };
        }
      ];
    };
  };
}
```

The module:
- opens the mesh port only on `meshInterfaces` (default `tailscale0`) and never opens the ingress port;
- orders the service after `tailscaled`;
- runs it under `DynamicUser` with systemd hardening;
- exposes `/run/geoclass/monitor.sock`;
- puts `geoclass` and `gtop` on PATH.

## Testing

```sh
cargo test                                          # unit, 3-node e2e, and Hegel property tests
nix build .#checks.x86_64-linux.geoclass-vm          # single node: surfaces, auth, no secrets in the journal, durability
nix build .#checks.x86_64-linux.geoclass-mesh-vm     # us/de/au + outsider: retry, stickiness across nodes, cluster stats, peer loss
nix build .#checks.x86_64-linux.geoclass-tailscale-vm # headscale: LocalAPI discovery, whois tag enforcement
```

The machine tests need a Linux builder.

## Security

- **Credentials.** Account tokens arrive from underclass, travel between nodes only over the tailnet, and are forwarded unchanged. They are never stored, logged, or gossiped.
- **Mesh.** Every `x-geoclass-*` header (including the mesh key) is stripped before egress. Mesh routes require the shared mesh key and, with Tailscale discovery, a LocalAPI `whois` check that the caller carries the tag.
- **Identities.** Prompt cache keys and account ids are only ever stored, gossiped, or logged as SHA-256 hashes; logs and snapshots show at most 12 characters.
- **Contracts.** Repository rules live in [`CONTRACTS`](CONTRACTS) and [`nixos/CONTRACTS`](nixos/CONTRACTS); see [`AGENTS.md`](AGENTS.md).

## License

MIT
