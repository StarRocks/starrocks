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
                buildInputs = env.checkLinkInputs;
                strictDeps = true;
                src = self;
              }
              ''
                cp -R "$src" source
                chmod -R +w source
                cd source

                bash -n build-support/build_helpers.sh
                . build-support/build_helpers.sh

                cp nix/thirdparty-archives.nix thirdparty-archives.generated-check
                python3 nix/generate-thirdparty-archives.py
                diff -u thirdparty-archives.generated-check nix/thirdparty-archives.nix

                OSTYPE=darwin
                getopt_path="$(starrocks_resolve_getopt_bin)"
                test -x "$getopt_path"

                starrocks-build-env --print > "$out"

                ${lib.optionalString pkgs.stdenv.hostPlatform.isLinux ''
                  cxx="$(grep '^CXX=' "$out" | cut -d= -f2-)"
                  export LIBRARY_PATH="$(grep '^LIBRARY_PATH=' "$out" | cut -d= -f2-)"
                  export NIX_LDFLAGS="$(grep '^NIX_LDFLAGS=' "$out" | cut -d= -f2-)"
                  printf 'int main() { return 0; }\n' > libiberty-check.cc
                  "$cxx" libiberty-check.cc -liberty -o libiberty-check
                  ./libiberty-check
                ''}
              '';
        }
      );

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
