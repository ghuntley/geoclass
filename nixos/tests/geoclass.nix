{
  pkgs,
  module,
}:
let
  mock = import ./mock.nix { inherit pkgs; };
in
pkgs.testers.nixosTest {
  name = "geoclass-service";

  nodes.machine =
    { ... }:
    {
      imports = [
        module
        (mock "solo")
      ];
      services.geoclass = {
        enable = true;
        region = "solo";
        discovery = "static";
        environmentFile = "/etc/geoclass-test.env";
        settings.upstream = "http://127.0.0.1:8000";
      };
      environment.etc."geoclass-test.env".text = ''
        GEOCLASS_MESH_KEY=test-mesh-key-0123
        GEOCLASS_UI_TOKEN=test-ui-token-0123
      '';
    };

  testScript = ''
    import json

    TOKEN = "Authorization: Bearer test-ui-token-0123"
    REQUEST = (
        "curl -sf -X POST -H 'Authorization: Bearer canary-token-0123' -H 'Content-Type: application/json' "
        "-d '{\"model\":\"m\",\"stream\":true,\"prompt_cache_key\":\"session-canary\"}' "
        "-D /tmp/headers http://127.0.0.1:9090/backend-api/codex/responses"
    )

    machine.start()
    machine.wait_for_unit("mock-upstream.service")
    machine.wait_for_unit("geoclass.service")
    machine.wait_for_open_port(9090)
    machine.wait_for_open_port(9091)
    machine.wait_for_open_port(8000)

    with subtest("ingress listens on loopback only"):
        machine.succeed("ss -ltnH 'sport = :9090' | grep -q '127.0.0.1:9090'")
        machine.fail("ss -ltnH 'sport = :9090' | grep -v '127.0.0.1:9090'")

    with subtest("requests egress with the account token and carry the region header"):
        out = machine.succeed(REQUEST)
        assert "hello from solo" in out, out
        machine.succeed("grep -qi '^x-geoclass-region: solo' /tmp/headers")

    with subtest("a capacity failure with nowhere else to go reaches the client unchanged"):
        machine.succeed("echo overloaded > /var/lib/mock-upstream/mode")
        out = machine.succeed(REQUEST)
        assert "server_is_overloaded" in out, out
        machine.succeed("echo quota > /var/lib/mock-upstream/mode")
        machine.succeed(REQUEST.replace("curl -sf", "curl -s -o /tmp/quota -w '%{http_code}'") + " | grep -qx 429")
        machine.succeed("echo ok > /var/lib/mock-upstream/mode")

    with subtest("admin API needs the UI token; the monitor socket needs none and exposes nothing else"):
        machine.fail("curl -sf -o /dev/null http://127.0.0.1:9090/admin/api/monitor")
        stats = json.loads(machine.succeed(f"curl -sf -H '{TOKEN}' 'http://127.0.0.1:9090/admin/api/stats?scope=local&since=1h'"))
        assert stats["totals"]["attempts"] >= 3, stats
        machine.succeed("test -S /run/geoclass/monitor.sock")
        machine.succeed("test $(stat -c %a /run/geoclass/monitor.sock) = 666")
        machine.succeed("curl --unix-socket /run/geoclass/monitor.sock -sf http://localhost/monitor | grep -q minute_bins")
        machine.fail("curl --unix-socket /run/geoclass/monitor.sock -sf -o /dev/null http://localhost/admin/api/stats")

    with subtest("mesh endpoints need the mesh key"):
        machine.fail("curl -sf -o /dev/null http://127.0.0.1:9091/mesh/bindings")
        machine.fail("curl -sf -o /dev/null -X POST http://127.0.0.1:9091/relay/backend-api/codex/responses")
        machine.succeed("curl -sf -H 'x-geoclass-mesh-key: test-mesh-key-0123' http://127.0.0.1:9091/mesh/info | grep -q solo")
        machine.succeed("curl -sf http://127.0.0.1:9091/metrics | grep -q geoclass_region_state")

    with subtest("no secret, token, or prompt cache key reaches the journal"):
        for secret in ["canary-token-0123", "test-mesh-key-0123", "test-ui-token-0123", "session-canary"]:
            machine.fail(f"journalctl -u geoclass.service --no-pager | grep -F {secret}")

    with subtest("statistics persist across a restart"):
        machine.succeed("test -f /var/lib/geoclass/stats.db")
        machine.systemctl("restart geoclass")
        machine.wait_for_unit("geoclass.service")
        machine.wait_for_open_port(9090)
        stats = json.loads(machine.succeed(f"curl -sf -H '{TOKEN}' 'http://127.0.0.1:9090/admin/api/stats?scope=local&since=1h'"))
        assert stats["totals"]["attempts"] >= 3, stats

    with subtest("the CLI is on PATH"):
        machine.succeed("command -v geoclass")
        machine.succeed("command -v gtop")
  '';
}
