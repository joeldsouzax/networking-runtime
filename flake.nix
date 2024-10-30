{
  description = "minimal rust nix project";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = { nixpkgs.follows = "nixpkgs"; };
    };

    crane = { url = "github:ipetkov/crane"; };
  };
  outputs = { nixpkgs, utils, rust-overlay, crane, ... }:
    utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        inherit (pkgs) lib;

        rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile
          ./rust-toolchain.toml;

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        src = craneLib.cleanCargoSource ./.;

        nativeBuildInputs = [ rustToolchain ]
          ++ lib.optionals pkgs.stdenv.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Foundation
            pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
            pkgs.darwin.apple_sdk.frameworks.CoreServices
            pkgs.darwin.apple_sdk.frameworks.CoreFoundation
          ];

        buildInputs = [ ]
          ++ lib.optionals pkgs.stdenv.isDarwin [ pkgs.libiconv ];

        commonArgs = {
          inherit src buildInputs nativeBuildInputs;
          strictDeps = true;
        };
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        bin = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
        }); # actual app building here :D

        # docker image
        image = pkgs.dockerTools.buildLayeredImage {
          name = "my-app";
          created = "now";
          contents = [ bin ];
          config = { Cmd = [ "${bin}/bin/my-app" ]; };
        };

      in with pkgs; {
        packages = {
          inherit bin image;
          default = bin;
        };

        devShells.default = craneLib.devShell {
          inputsFrom = [ bin ];
          packages = [ cargo-watch dive ];
        };
      });
}
