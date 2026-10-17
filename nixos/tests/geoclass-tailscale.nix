# Tailscale discovery against a real control plane (headscale): tagged nodes find each other
# through the tailscaled LocalAPI, and an untagged tailnet node is refused by the whois check
# even when it presents the mesh key.
{
  pkgs,
  module,
}:
let
  mock = import ./mock.nix { inherit pkgs; };
  tls-cert = pkgs.runCommand "headscale-cert" { buildInputs = [ pkgs.openssl ]; } ''
    openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj '/CN=headscale' \
      -addext 'subjectAltName = DNS:headscale' -keyout key.pem -out cert.pem
    mkdir -p $out
    cp key.pem cert.pem $out
  '';
  policy = pkgs.writeText "policy.json" (
    builtins.toJSON {
      tagOwners."tag:geoclass" = [ "test@" ];
      acls = [
        {
          action = "accept";
          src = [ "*" ];
          dst = [ "*:*" ];
        }
      ];
    }
  );
  tailnetNode = {
    services.tailscale.enable = true;
    security.pki.certificateFiles = [ "${tls-cert}/cert.pem" ];
  };
  geoNode =
    region:
    { ... }:
    {
      imports = [
        module
        (mock region)
        tailnetNode
      ];
      services.geoclass = {
        enable = true;
        inherit region;
        discovery = "tailscale";
        environmentFile = "/etc/geoclass-test.env";
        settings = {
          upstream = "http://127.0.0.1:8000";
          gossip_interval_ms = 500;
          discovery_interval_ms = 1000;
        };
      };
      environment.etc."geoclass-test.env".text = ''
        GEOCLASS_MESH_KEY=test-mesh-key-0123
        GEOCLASS_UI_TOKEN=test-ui-token-0123
      '';
    };
in
pkgs.testers.nixosTest {
  name = "geoclass-tailscale";

  nodes = {
    headscale = {
      services.headscale = {
        enable = true;
        port = 8080;
        settings = {
          server_url = "https://headscale";
          prefixes.v4 = "100.64.0.0/10";
          derp.server = {
            enabled = true;
            region_id = 999;
            stun_listen_addr = "0.0.0.0:3478";
          };
          dns = {
            base_domain = "tailnet";
            override_local_dns = false;
          };
          policy = {
            mode = "file";
            path = "${policy}";
          };
        };
      };
      services.nginx = {
        enable = true;
        virtualHosts.headscale = {
          addSSL = true;
          sslCertificate = "${tls-cert}/cert.pem";
          sslCertificateKey = "${tls-cert}/key.pem";
          locations."/" = {
            proxyPass = "http://127.0.0.1:8080";
            proxyWebsockets = true;
          };
        };
      };
      networking.firewall = {
        allowedTCPPorts = [
          80
          443
        ];
        allowedUDPPorts = [ 3478 ];
      };
      environment.systemPackages = [ pkgs.headscale ];
    };
    alpha = geoNode "alpha";
    beta = geoNode "beta";
    outsider =
      { ... }:
      {
        imports = [ tailnetNode ];
        environment.systemPackages = [ pkgs.curl ];
      };
  };

  testScript = ''
    import json

    TOKEN = "Authorization: Bearer test-ui-token-0123"

    start_all()
    headscale.wait_for_unit("headscale")
    headscale.wait_for_open_port(443)
    headscale.succeed("headscale users create test")
    user_id = json.loads(headscale.succeed("headscale users list -o json"))[0]["id"]
    tagged = headscale.succeed(f"headscale preauthkeys create -u {user_id} --reusable --tags tag:geoclass").strip().splitlines()[-1]
    plain = headscale.succeed(f"headscale preauthkeys create -u {user_id} --reusable").strip().splitlines()[-1]

    for m in (alpha, beta):
        m.wait_for_unit("tailscaled.service")
        m.succeed(f"tailscale up --login-server https://headscale --auth-key {tagged} --advertise-tags tag:geoclass")
    outsider.wait_for_unit("tailscaled.service")
    outsider.succeed(f"tailscale up --login-server https://headscale --auth-key {plain}")
    alpha.wait_until_succeeds("tailscale ping beta", timeout=60)

    with subtest("tagged nodes find each other through the LocalAPI"):
        for m in (alpha, beta):
            m.wait_until_succeeds(
                f"curl -sf -H '{TOKEN}' http://127.0.0.1:9090/admin/api/peers | jq -e '[.[] | select(.live and .source == \"tailscale\")] | length == 1'",
                timeout=90,
            )
        peers = json.loads(alpha.succeed(f"curl -sf -H '{TOKEN}' http://127.0.0.1:9090/admin/api/peers"))
        assert peers[0]["region"] == "beta", peers

    with subtest("requests egress through a discovered peer when the local region is overloaded"):
        alpha.succeed("echo overloaded > /var/lib/mock-upstream/mode")
        out = alpha.succeed(
            "curl -s -X POST -H 'Content-Type: application/json' -d '{\"model\":\"m\",\"stream\":true}' "
            "-D /tmp/headers http://127.0.0.1:9090/backend-api/codex/responses"
        )
        assert "hello from beta" in out, out
        alpha.succeed("grep -qi '^x-geoclass-region: beta' /tmp/headers")

    with subtest("an untagged tailnet node is refused even with the mesh key"):
        alpha_ip = alpha.succeed("tailscale ip -4").strip()
        outsider.succeed(
            f"curl -s -o /dev/null -w '%{{http_code}}' -H 'x-geoclass-mesh-key: test-mesh-key-0123' "
            f"http://{alpha_ip}:9091/mesh/info | grep -qx 403"
        )

    with subtest("the mesh port is closed off the tailnet"):
        outsider.fail("curl -sf --max-time 5 -o /dev/null http://alpha:9091/metrics")
  '';
}
