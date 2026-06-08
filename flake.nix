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
    in
    {
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
