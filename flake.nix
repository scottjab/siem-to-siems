{
  description = "siem-to-siems — tsnet HTTP receiver that fans events out to NDJSON and HTTP sinks";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      # Systems we build for.
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system:
        f (import nixpkgs { inherit system; }));

      version =
        if self ? shortRev then self.shortRev
        else if self ? dirtyShortRev then self.dirtyShortRev
        else "dev";

      # NixOS module: renders config.json from `settings` and runs the service.
      nixosModule = { config, lib, pkgs, ... }:
        let
          cfg = config.services.siem-to-siems;
          settingsFormat = pkgs.formats.json { };
          configFile = settingsFormat.generate "siem-to-siems.json" cfg.settings;
          # Best-effort port extraction (":443" -> 443) for the optional firewall rule.
          addr = cfg.settings.server.addr or ":443";
          port = lib.toInt (lib.last (lib.splitString ":" addr));
        in
        {
          options.services.siem-to-siems = {
            enable = lib.mkEnableOption "siem-to-siems event fan-out receiver";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.siem-to-siems;
              defaultText = lib.literalExpression "siem-to-siems flake package for the host system";
              description = "The siem-to-siems package to run.";
            };

            settings = lib.mkOption {
              type = settingsFormat.type;
              default = { };
              example = lib.literalExpression ''
                {
                  tsnet.hostname = "siem-to-siems";
                  server = { addr = ":443"; tls_enabled = true; };
                  destinations = {
                    ndjson = { directory = "/var/lib/siem-to-siems/logs"; rotate = "1h"; };
                    parquet = { directory = "/var/lib/siem-to-siems/parquet"; rotate = "5m"; daily_merge = "24h"; };
                    http = [ { url = "https://splunk.example.com:8088/services/collector/raw"; } ];
                  };
                }
              '';
              description = ''
                Contents of the service's config.json (see the project README for the
                schema). At least one destination must be configured.

                Do not put secrets here — the rendered file is world-readable in the Nix
                store. Supply the Tailscale auth key via {option}`services.siem-to-siems.environmentFile`
                as `TS_AUTHKEY` instead (tsnet reads it when `tsnet.auth_key` is empty).
              '';
            };

            environmentFile = lib.mkOption {
              type = lib.types.nullOr lib.types.path;
              default = null;
              example = "/run/secrets/siem-to-siems.env";
              description = ''
                Path to a systemd EnvironmentFile holding secrets, e.g.
                `TS_AUTHKEY=tskey-auth-...`. Kept out of the Nix store.
              '';
            };

            stateDir = lib.mkOption {
              type = lib.types.str;
              default = "/var/lib/siem-to-siems";
              description = ''
                Working directory and HOME for the service. tsnet stores its node state
                here, and relative output paths in {option}`settings` resolve against it.
              '';
            };

            openFirewall = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Open the configured server TCP port in the firewall.";
            };
          };

          config = lib.mkIf cfg.enable {
            systemd.services.siem-to-siems = {
              description = "siem-to-siems event fan-out receiver";
              wantedBy = [ "multi-user.target" ];
              after = [ "network-online.target" ];
              wants = [ "network-online.target" ];

              serviceConfig = {
                ExecStart = lib.getExe cfg.package;
                Environment = [
                  "SIEM_TO_SIEMS_CONFIG=${configFile}"
                  "HOME=${cfg.stateDir}"
                ];
                EnvironmentFile = lib.optional (cfg.environmentFile != null) cfg.environmentFile;

                DynamicUser = true;
                StateDirectory = "siem-to-siems";
                WorkingDirectory = cfg.stateDir;

                Restart = "always";
                RestartSec = "10s";

                # tsnet's ListenTLS binds :443 by default.
                AmbientCapabilities = [ "CAP_NET_BIND_SERVICE" ];
                CapabilityBoundingSet = [ "CAP_NET_BIND_SERVICE" ];

                # Hardening.
                NoNewPrivileges = true;
                ProtectSystem = "strict";
                ProtectHome = true;
                PrivateTmp = true;
                PrivateDevices = true;
                ProtectKernelTunables = true;
                ProtectKernelModules = true;
                ProtectControlGroups = true;
                RestrictAddressFamilies = [ "AF_INET" "AF_INET6" "AF_UNIX" ];
                RestrictNamespaces = true;
                LockPersonality = true;
                MemoryDenyWriteExecute = true;
                SystemCallArchitectures = "native";
              };
            };

            networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ port ];
          };
        };
    in
    {
      nixosModules.siem-to-siems = nixosModule;
      nixosModules.default = nixosModule;

      packages = forAllSystems (pkgs: rec {
        siem-to-siems = pkgs.buildGoModule {
          pname = "siem-to-siems";
          inherit version;
          src = ./.;

          # Freshest Go available in nixpkgs (1.26.x); matches the go.mod directive.
          go = pkgs.go_1_26;

          # If go.mod/go.sum change, set this to lib.fakeHash, run `nix build`, then
          # paste the hash nix reports.
          vendorHash = "sha256-1NJIIXDvGMmuyOFAQkwPXu9R6xNPlkUkNCyhmMLnmR4=";

          # The binary lives in ./cmd/siem-to-siems.
          subPackages = [ "cmd/siem-to-siems" ];

          ldflags = [ "-s" "-w" ];

          meta = with pkgs.lib; {
            description = "tsnet HTTP receiver that fans events out to NDJSON and HTTP sinks";
            homepage = "https://github.com/scottjab/siem-to-siems";
            mainProgram = "siem-to-siems";
            platforms = systems;
          };
        };
        default = siem-to-siems;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.go_1_26
            pkgs.gopls
            pkgs.gotools
            pkgs.go-tools # staticcheck
          ];
        };
      });

      apps = forAllSystems (pkgs: rec {
        siem-to-siems = {
          type = "app";
          program = "${self.packages.${pkgs.system}.siem-to-siems}/bin/siem-to-siems";
        };
        default = siem-to-siems;
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
