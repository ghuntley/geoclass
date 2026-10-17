{
  inputs = {
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    systems.url = "github:nix-systems/default";
    devenv.url = "github:cachix/devenv";
    devenv.inputs.nixpkgs.follows = "nixpkgs";
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs =
    {
      self,
      nixpkgs,
      devenv,
      systems,
      ...
    }@inputs:
    let
      forEachSystem = nixpkgs.lib.genAttrs (import systems);
    in
    {
      devShells = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = devenv.lib.mkShell {
            inherit inputs pkgs;
            modules = [ ./devenv.nix ];
          };
        }
      );

      packages = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        rec {
          geoclass = pkgs.rustPlatform.buildRustPackage {
            pname = "geoclass";
            version = "0.1.0";

            src = self;
            cargoLock.lockFile = ./Cargo.lock;
            doCheck = false;
            nativeBuildInputs = [ pkgs.makeWrapper ];
            postInstall = ''
              makeWrapper "$out/bin/geoclass" "$out/bin/gtop" --add-flags top
            '';

            meta = with pkgs.lib; {
              description = "Geo-aware, prompt-cache-sticky egress router for OpenAI capacity errors, meshed over Tailscale";
              homepage = "https://github.com/ghuntley/geoclass";
              license = licenses.mit;
              mainProgram = "geoclass";
            };
          };
          default = geoclass;
        }
      );

      apps = forEachSystem (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.geoclass}/bin/geoclass";
        };
        gtop = {
          type = "app";
          program = "${self.packages.${system}.geoclass}/bin/gtop";
        };
      });

      overlays.default = final: _prev: {
        geoclass = self.packages.${final.stdenv.hostPlatform.system}.geoclass;
      };

      nixosModules.default = import ./nixos/module.nix self;

      checks = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          isLinux = nixpkgs.lib.elem system [
            "x86_64-linux"
            "aarch64-linux"
          ];
          module = self.nixosModules.default;
        in
        nixpkgs.lib.optionalAttrs isLinux {
          geoclass-module = pkgs.runCommand "geoclass-nixos-module-eval" {
            toplevel =
              (pkgs.nixos [
                module
                {
                  services.geoclass = {
                    enable = true;
                    region = "example";
                    regions = [
                      {
                        name = "example";
                        utcOffsetHours = 1;
                        peakHours = [
                          9
                          18
                        ];
                      }
                    ];
                  };
                  system.stateVersion = "25.11";
                  fileSystems."/".device = "none";
                  fileSystems."/".fsType = "tmpfs";
                  boot.loader.grub.enable = false;
                }
              ]).config.system.build.toplevel.drvPath;
          } "test -n \"$toplevel\" && touch $out";

          geoclass-vm = import ./nixos/tests/geoclass.nix { inherit pkgs module; };
          geoclass-mesh-vm = import ./nixos/tests/geoclass-mesh.nix { inherit pkgs module; };
          geoclass-tailscale-vm = import ./nixos/tests/geoclass-tailscale.nix { inherit pkgs module; };
        }
      );
    };
}
