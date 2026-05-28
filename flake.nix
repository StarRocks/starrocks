{
  description = "StarRocks development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      lib = nixpkgs.lib;
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
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          env = import ./nix/dev-env.nix { inherit pkgs; };
        in
        {
          default = env.buildWrapper;
          starrocks-build-env = env.buildWrapper;
        }
      );

      devShells = forAllSystems (
        pkgs:
        let
          env = import ./nix/dev-env.nix { inherit pkgs; };
        in
        {
          default = env.devShell;
        }
      );

      checks = forAllSystems (
        pkgs:
        let
          env = import ./nix/dev-env.nix { inherit pkgs; };
        in
        {
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
