# A mock Codex upstream on port 8000, switchable through /var/lib/mock-upstream/mode.
{ pkgs }:
region:
{ ... }:
{
  systemd.services.mock-upstream = {
    wantedBy = [ "multi-user.target" ];
    before = [ "geoclass.service" ];
    environment = {
      REGION = region;
      MODE_FILE = "/var/lib/mock-upstream/mode";
      PORT = "8000";
    };
    serviceConfig = {
      ExecStart = "${pkgs.python3}/bin/python3 ${./mock-upstream.py}";
      StateDirectory = "mock-upstream";
    };
  };
  environment.systemPackages = [
    pkgs.curl
    pkgs.jq
    pkgs.iproute2
  ];
}
