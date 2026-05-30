{
  description = "StarRocks development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      lib = nixpkgs.lib;
      revision = self.shortRev or self.dirtyShortRev or "unknown";
      version = "main-${revision}";
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
            }
          )
        );
      mkEnv =
        pkgs:
        import ./nix/dev-env.nix {
          inherit pkgs version;
          src = self.outPath;
          commitHash = revision;
        };
      forAllSystemsWithEnv =
        f:
        forAllSystems (
          pkgs:
          let
            env = mkEnv pkgs;
          in
          f pkgs env
        );
    in
    {
      packages = forAllSystemsWithEnv (
        pkgs: env: {
          default = env.starrocks;
          starrocks = env.starrocks;
          starrocks-build-env = env.buildWrapper;
          starrocks-maven-deps = env.mavenDeps;
          starrocks-thirdparty = env.starrocksThirdparty;
          starrocks-thirdparty-foundation = env.starrocksThirdpartyFoundation;
          starrocks-thirdparty-rpc = env.starrocksThirdpartyRpc;
          starrocks-thirdparty-formats = env.starrocksThirdpartyFormats;
          starrocks-thirdparty-leaf = env.starrocksThirdpartyLeaf;
        }
      );

      devShells = forAllSystemsWithEnv (
        pkgs: env: {
          default = env.devShell;
          be = env.beDevShell;
        }
      );

      checks = forAllSystemsWithEnv (
        pkgs: env: {
          starrocks-nix-env =
            pkgs.runCommand "starrocks-nix-env-check"
              {
                nativeBuildInputs = env.checkInputs;
                src = self;
              }
              ''
                cp -R "$src" source
                chmod -R +w source
                cd source

                bash -n build-support/build_helpers.sh
                . build-support/build_helpers.sh

                OSTYPE=darwin
                getopt_path="$(starrocks_resolve_getopt_bin)"
                test -x "$getopt_path"

                starrocks-build-env --print > "$out"
              '';
        }
      );

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
