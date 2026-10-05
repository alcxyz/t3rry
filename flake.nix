{
  description = "t3rry — move T3 Code projects between server base directories";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        sourceVersion = builtins.replaceStrings ["\n"] [""] (builtins.readFile ./VERSION);
        revision = self.rev or self.dirtyRev or "unknown";
        developmentVersion = import ./build-version.nix {
          version = sourceVersion;
          inherit revision;
        };
        releaseVersion = import ./build-version.nix {
          version = sourceVersion;
          inherit revision;
          release = true;
        };
        mkT3rry = version: pkgs.buildGoModule {
          pname = "t3rry";
          inherit version;
          src = self;

          # Update when go.mod changes:
          #   nix build .# 2>&1 | grep "got:" | awk '{print $2}'
          vendorHash = "sha256-7IC/p5GlD2EZkDXQzkaZ7E19S/ABKEBsg68vt8pykis=";

          subPackages = [ "cmd/t3rry" ];
          env.CGO_ENABLED = "0";
          ldflags = [ "-s" "-w" "-X main.version=${version}" ];

          meta = with pkgs.lib; {
            description = "Move T3 Code projects between server base directories";
            homepage = "https://github.com/alcxyz/t3rry";
            license = licenses.mit;
            mainProgram = "t3rry";
          };
        };
      in {
        packages = (rec {
          t3rry = mkT3rry developmentVersion;
          default = t3rry;
        }) // pkgs.lib.optionalAttrs
          (builtins.match "(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)" sourceVersion != null
            && builtins.match "[0-9a-f]{7,64}" revision != null)
          { release = mkT3rry releaseVersion; };

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [ go gopls gotools goreleaser sqlite ];
        };
      }
    );
}
