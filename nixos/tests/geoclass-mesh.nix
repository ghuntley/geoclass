# Three regions on three machines, meshed with static discovery over the test VLAN, plus an
# outsider without the mesh key.
{
  pkgs,
  module,
}:
let
  mock = import ./mock.nix { inherit pkgs; };
  regions = [
    "us"
    "de"
    "au"
  ];
  node =
    region:
    { ... }:
    {
      imports = [
        module
        (mock region)
      ];
      services.geoclass = {
        enable = true;
        inherit region;
        discovery = "static";
        staticPeers = map (r: "http://${r}:9091") (builtins.filter (r: r != region) regions);
        advertiseUrl = "http://${region}:9091";
        meshInterfaces = [ "eth1" ];
        environmentFile = "/etc/geoclass-test.env";
        # "us" is overwhelmingly preferred for new sessions so the test controls placement.
        regions = [
          {
            name = "us";
            weight = 10000;
          }
          { name = "de"; }
          { name = "au"; }
        ];
        settings = {
          upstream = "http://127.0.0.1:8000";
          gossip_interval_ms = 500;
          discovery_interval_ms = 1000;
          thresholds.min_samples = 3;
        };
      };
      environment.etc."geoclass-test.env".text = ''
        GEOCLASS_MESH_KEY=test-mesh-key-0123
        GEOCLASS_UI_TOKEN=test-ui-token-0123
      '';
    };
in
pkgs.testers.nixosTest {
  name = "geoclass-mesh";

  nodes = {
    us = node "us";
    de = node "de";
    au = node "au";
    outsider =
      { ... }:
      {
        environment.systemPackages = [ pkgs.curl ];
      };
  };

  testScript = ''
    import json

    TOKEN = "Authorization: Bearer test-ui-token-0123"
    nodes = {"us": us, "de": de, "au": au}

    def send(machine, key, out="/tmp/headers"):
        body = json.dumps({"model": "m", "stream": True, **({"prompt_cache_key": key} if key else {})})
        return machine.succeed(
            "curl -s -X POST -H 'Authorization: Bearer canary-token-0123' -H 'Content-Type: application/json' "
            f"-d '{body}' -D {out} http://127.0.0.1:9090/backend-api/codex/responses"
        )

    def header(machine, name, path="/tmp/headers"):
        return machine.succeed(f"grep -i '^{name}:' {path} | cut -d' ' -f2 | tr -d '\\r\\n'")

    def admin(machine, path):
        return json.loads(machine.succeed(f"curl -sf -H '{TOKEN}' 'http://127.0.0.1:9090{path}'"))

    def mode(machine, value):
        machine.succeed(f"echo {value} > /var/lib/mock-upstream/mode")

    start_all()
    for m in nodes.values():
        m.wait_for_unit("geoclass.service")
        m.wait_for_open_port(9090)
        m.wait_for_open_port(8000)

    with subtest("every node discovers the other two"):
        for m in nodes.values():
            m.wait_until_succeeds(
                f"curl -sf -H '{TOKEN}' http://127.0.0.1:9090/admin/api/peers | jq -e '[.[] | select(.live)] | length == 2'",
                timeout=60,
            )

    with subtest("a capacity failure before output is retried in another region, invisibly"):
        mode(us, "overloaded")
        out = send(us, "session-1")
        served = header(us, "x-geoclass-region")
        assert served in ("de", "au"), served
        assert out.count("event: response.created") == 1, out
        assert f"hello from {served}" in out and "server_is_overloaded" not in out, out
        assert header(us, "x-geoclass-attempts") == "2"

    with subtest("the moved session stays put after the old region recovers, via any node"):
        mode(us, "ok")
        for m in nodes.values():
            m.wait_until_succeeds(
                f"curl -sf -H '{TOKEN}' http://127.0.0.1:9090/admin/api/routing | jq -e '.bindings | add == 1'",
                timeout=30,
            )
        for name, m in nodes.items():
            send(m, "session-1")
            assert header(m, "x-geoclass-region") == served, name
            assert header(m, "x-geoclass-reason") == "bound", name

    with subtest("account errors pass through without a retry"):
        mode(us, "quota")
        before = int(de.succeed("curl -sf http://127.0.0.1:8000/count")) + int(au.succeed("curl -sf http://127.0.0.1:8000/count"))
        send(de, "session-quota")
        de.succeed("grep -q ' 429' /tmp/headers")
        after = int(de.succeed("curl -sf http://127.0.0.1:8000/count")) + int(au.succeed("curl -sf http://127.0.0.1:8000/count"))
        assert after == before, (before, after)
        mode(us, "ok")

    with subtest("cluster statistics merge every node"):
        stats = admin(au, "/admin/api/stats?scope=cluster&since=1h&bucket=minute&by=region")
        assert len(stats["nodes"]) == 3 and all(n["ok"] for n in stats["nodes"]), stats["nodes"]
        assert stats["totals"]["rescued"] >= 1 and stats["totals"]["overload_hit"] >= 1, stats["totals"]

    with subtest("the mesh refuses callers without the key"):
        outsider.fail("curl -sf -o /dev/null http://us:9091/mesh/bindings")
        outsider.fail("curl -sf -o /dev/null -X POST http://us:9091/relay/backend-api/codex/responses")
        outsider.succeed("curl -s -o /dev/null -w '%{http_code}' http://us:9091/mesh/info | grep -qx 401")

    with subtest("a stopped peer leaves placement"):
        de.systemctl("stop geoclass")
        us.wait_until_succeeds(
            f"curl -sf -H '{TOKEN}' http://127.0.0.1:9090/admin/api/monitor | jq -e '.regions[] | select(.name == \"de\") | .available == false'",
            timeout=30,
        )

    with subtest("no secret reaches any journal"):
        for m in nodes.values():
            for secret in ["canary-token-0123", "test-mesh-key-0123", "test-ui-token-0123", "session-1"]:
                m.fail(f"journalctl -u geoclass.service --no-pager | grep -F {secret}")
  '';
}
