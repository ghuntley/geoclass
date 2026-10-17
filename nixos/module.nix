self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.geoclass;
  inherit (lib)
    mkEnableOption
    mkIf
    mkOption
    types
    literalExpression
    optionalAttrs
    ;

  toml = pkgs.formats.toml { };

  regionToToml =
    r:
    { inherit (r) name; }
    // optionalAttrs (r.failureDomain != null) { failure_domain = r.failureDomain; }
    // optionalAttrs (r.weight != null) { inherit (r) weight; }
    // optionalAttrs (r.hourly != null) { inherit (r) hourly; }
    // optionalAttrs (r.weekend != null) { inherit (r) weekend; }
    // optionalAttrs (r.utcOffsetHours != null) { utc_offset_hours = r.utcOffsetHours; }
    // optionalAttrs (r.peakHours != null) { peak_hours = r.peakHours; }
    // optionalAttrs (r.peakWeight != null) { peak_weight = r.peakWeight; };

  configFile = toml.generate "geoclass-config.toml" (
    {
      region = cfg.region;
      ingress_bind = cfg.ingressAddress;
      mesh_bind = cfg.meshAddress;
      mesh_port = cfg.meshPort;
      discovery = cfg.discovery;
      tailscale_tag = cfg.tailscaleTag;
      tailscale_socket = cfg.tailscaleSocket;
      peers = cfg.staticPeers;
      regions = map regionToToml cfg.regions;
    }
    // optionalAttrs (cfg.nodeId != null) { node_id = cfg.nodeId; }
    // optionalAttrs (cfg.advertiseUrl != null) { advertise_url = cfg.advertiseUrl; }
    // cfg.settings
  );

  regionModule = types.submodule {
    options = {
      name = mkOption {
        type = types.str;
        description = "Region name. Any string; GeoClass never interprets it.";
      };
      failureDomain = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "Regions sharing a failure domain are known to hit the same upstream serving pool.";
      };
      weight = mkOption {
        type = types.nullOr types.number;
        default = null;
        description = "Static multiplier on the region's chase-the-sun weight (default 1).";
      };
      hourly = mkOption {
        type = types.nullOr (types.listOf types.number);
        default = null;
        description = "24 weights, one per UTC hour, for placing new sessions.";
      };
      weekend = mkOption {
        type = types.nullOr (types.listOf types.number);
        default = null;
        description = "24 weights, one per UTC hour, used on Saturday and Sunday (UTC).";
      };
      utcOffsetHours = mkOption {
        type = types.nullOr types.number;
        default = null;
        description = "Shorthand when `hourly` is unset: the region's UTC offset for `peakHours`.";
      };
      peakHours = mkOption {
        type = types.nullOr (types.listOf types.int);
        default = null;
        example = [
          9
          18
        ];
        description = "Local `[start end)` peak window in hours (wraps past midnight when start > end).";
      };
      peakWeight = mkOption {
        type = types.nullOr types.number;
        default = null;
        description = "Weight inside the local peak window on weekdays (default 0.5).";
      };
    };
  };
in
{
  options.services.geoclass = {
    enable = mkEnableOption "GeoClass, the geo-aware egress router for underclass";

    package = mkOption {
      type = types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.geoclass;
      defaultText = literalExpression "geoclass flake package";
      description = "The geoclass package to run.";
    };

    region = mkOption {
      type = types.str;
      example = "eu-central";
      description = "This node's region. Any string; it only has to match the names peers use.";
    };

    nodeId = mkOption {
      type = types.nullOr types.str;
      default = config.networking.hostName;
      defaultText = literalExpression "config.networking.hostName";
      description = "Unique node identifier in the mesh.";
    };

    ingressAddress = mkOption {
      type = types.str;
      default = "127.0.0.1:9090";
      description = ''
        Loopback address underclass talks to. Point underclass at it with
        `UNDERCLASS_CODEX_UPSTREAM=http://<ingressAddress>/backend-api/codex/responses`.
      '';
    };

    meshAddress = mkOption {
      type = types.str;
      default = "0.0.0.0:9091";
      description = "Listen address for the mesh (gossip, relay, metrics). The firewall only opens it on `meshInterfaces`.";
    };

    meshPort = mkOption {
      type = types.port;
      default = 9091;
      description = "Port peers use to reach this node's mesh listener.";
    };

    meshInterfaces = mkOption {
      type = types.listOf types.str;
      default = [ "tailscale0" ];
      description = "Interfaces on which the firewall admits the mesh port. Never add public interfaces.";
    };

    advertiseUrl = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "http://100.64.0.7:9091";
      description = "This node's mesh URL as peers reach it (default: its tailnet IPv4 and `meshPort`).";
    };

    discovery = mkOption {
      type = types.enum [
        "tailscale"
        "static"
      ];
      default = "tailscale";
      description = "Find peers through the tailscaled LocalAPI (by tag) or from `staticPeers`.";
    };

    tailscaleTag = mkOption {
      type = types.str;
      default = "tag:geoclass";
      description = "ACL tag every GeoClass node carries; peers without it are ignored and refused.";
    };

    tailscaleSocket = mkOption {
      type = types.str;
      default = "/run/tailscale/tailscaled.sock";
      description = ''
        Path of the tailscaled LocalAPI socket. tailscaled creates it world-connectable and lets
        non-root callers make the read-only calls GeoClass needs (status and whois), so the
        DynamicUser service needs no extra group.
      '';
    };

    staticPeers = mkOption {
      type = types.listOf types.str;
      default = [ ];
      example = [ "http://10.0.0.2:9091" ];
      description = "Peer mesh URLs when `discovery = \"static\"`.";
    };

    regions = mkOption {
      type = types.listOf regionModule;
      default = [ ];
      description = "Operator data for every region in the mesh: failure domains and chase-the-sun weights.";
    };

    environmentFile = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "/run/secrets/geoclass.env";
      description = ''
        Environment file for secrets: GEOCLASS_MESH_KEY (shared by every node) and
        GEOCLASS_UI_TOKEN (minted on first run when absent). Keeps secrets out of the Nix store.
      '';
    };

    settings = mkOption {
      type = toml.type;
      default = { };
      example = literalExpression ''
        {
          stats_retention_days = 730;
          retry_deadline_ms = 120000;
          thresholds = { red_rate = 0.1; green_rate = 0.03; min_samples = 5; window_ms = 300000; };
        }
      '';
      description = "Other non-secret config.toml keys (merged last).";
    };

    stateDirectory = mkOption {
      type = types.strMatching "^[A-Za-z0-9_.-]+$";
      default = "geoclass";
      description = "systemd StateDirectory name; statistics live in /var/lib/<name>/stats.db.";
    };
  };

  config = mkIf cfg.enable {
    environment.systemPackages = [ cfg.package ];

    networking.firewall.interfaces = lib.genAttrs cfg.meshInterfaces (_: {
      allowedTCPPorts = [ cfg.meshPort ];
    });

    systemd.services.geoclass = {
      description = "GeoClass geo-aware egress router";
      after = [ "network-online.target" ] ++ lib.optional (cfg.discovery == "tailscale") "tailscaled.service";
      wants = [ "network-online.target" ] ++ lib.optional (cfg.discovery == "tailscale") "tailscaled.service";
      wantedBy = [ "multi-user.target" ];

      environment = {
        GEOCLASS_CONFIG = "${configFile}";
        GEOCLASS_DATA_DIR = "/var/lib/${cfg.stateDirectory}";
        GEOCLASS_MONITOR_SOCKET = "/run/geoclass/monitor.sock";
      };

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/geoclass serve";
        EnvironmentFile = mkIf (cfg.environmentFile != null) [ (toString cfg.environmentFile) ];

        DynamicUser = true;
        StateDirectory = cfg.stateDirectory;
        RuntimeDirectory = "geoclass";
        RuntimeDirectoryMode = "0755";
        Restart = "on-failure";
        RestartSec = 5;

        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectKernelTunables = true;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictAddressFamilies = [
          "AF_UNIX"
          "AF_INET"
          "AF_INET6"
        ];
        RestrictNamespaces = true;
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        SystemCallArchitectures = "native";
        UMask = "0077";
      };
    };
  };
}
