{ pkgs }:

let
  lib = pkgs.lib;
  isDarwin = pkgs.stdenv.hostPlatform.isDarwin;
  isLinux = pkgs.stdenv.hostPlatform.isLinux;

  jdk = pkgs.jdk21_headless;
  llvm = pkgs.llvmPackages.llvm;
  clang = pkgs.llvmPackages.clang;

  joinedRoot =
    name: paths:
    pkgs.symlinkJoin {
      inherit name paths;
    };

  opensslRoot = joinedRoot "starrocks-openssl-root" [
    pkgs.openssl.dev
    pkgs.openssl.out
  ];
  curlRoot = joinedRoot "starrocks-curl-root" [
    pkgs.curl.dev
    pkgs.curl.out
  ];
  icuRoot = joinedRoot "starrocks-icu-root" [
    pkgs.icu.dev
    pkgs.icu.out
  ];
  llvmToolchain = joinedRoot "starrocks-llvm-toolchain" [
    clang
    llvm
  ];

  commonInputs = with pkgs; [
    autoconf
    automake
    bash
    bashInteractive
    bison
    byacc
    bzip2
    ccache
    cmake
    coreutils
    curl
    file
    findutils
    flex
    gawk
    git
    gnumake
    gnugrep
    gnused
    gnutar
    gzip
    jdk
    libtool
    maven
    ninja
    perl
    pkg-config
    protobuf
    python3
    thrift
    unzip
    wget
    which
    xz
    zip
  ];

  linuxInputs = lib.optionals isLinux (
    with pkgs;
    [
      binutils
      gcc
      util-linux
    ]
  );

  darwinInputs = lib.optionals isDarwin ([
    llvmToolchain
    pkgs.darwin.cctools
    pkgs.libiconv
    pkgs.getopt
    opensslRoot
    curlRoot
    icuRoot
  ]);

  allInputs = commonInputs ++ linuxInputs ++ darwinInputs;
  toolPath = lib.makeBinPath allInputs;

  platformExports =
    if isDarwin then
      ''
        export STARROCKS_USE_NIX_DEPS=1
        export STARROCKS_LLVM_HOME="${llvmToolchain}"
        export LLVM_HOME="${llvmToolchain}"
        export CC="${llvmToolchain}/bin/clang"
        export CXX="${llvmToolchain}/bin/clang++"
        export AR="${llvmToolchain}/bin/llvm-ar"
        export RANLIB="${llvmToolchain}/bin/llvm-ranlib"
        export STRIP="${llvmToolchain}/bin/llvm-strip"
        export OPENSSL_ROOT_DIR="${opensslRoot}"
        export CURL_ROOT="${curlRoot}"
        export ICU_ROOT="${icuRoot}"
      ''
    else
      ''
        export STARROCKS_GCC_HOME="${pkgs.gcc}"
        export CC="${pkgs.gcc}/bin/gcc"
        export CXX="${pkgs.gcc}/bin/g++"
      '';

  commonExports = ''
    export PATH="${toolPath}:$PATH"
    export STARROCKS_HOME="''${STARROCKS_HOME:-$PWD}"
    export JAVA_HOME="${jdk.home}"
    export CUSTOM_MVN="${pkgs.maven}/bin/mvn"
    export CUSTOM_CMAKE="${pkgs.cmake}/bin/cmake"
    export PYTHON="${pkgs.python3}/bin/python3"
  '';

  buildWrapper = pkgs.writeShellScriptBin "starrocks-build-env" ''
    set -euo pipefail

    ${commonExports}
    ${platformExports}

    if [[ "''${1:-}" == "--print" ]]; then
      printf 'STARROCKS_HOME=%s\n' "$STARROCKS_HOME"
      printf 'JAVA_HOME=%s\n' "$JAVA_HOME"
      printf 'CUSTOM_MVN=%s\n' "$CUSTOM_MVN"
      printf 'CUSTOM_CMAKE=%s\n' "$CUSTOM_CMAKE"
      printf 'PYTHON=%s\n' "$PYTHON"
      printf 'CC=%s\n' "''${CC:-}"
      printf 'CXX=%s\n' "''${CXX:-}"
      printf 'AR=%s\n' "''${AR:-}"
      printf 'RANLIB=%s\n' "''${RANLIB:-}"
      printf 'STRIP=%s\n' "''${STRIP:-}"
      printf 'STARROCKS_USE_NIX_DEPS=%s\n' "''${STARROCKS_USE_NIX_DEPS:-}"
      printf 'STARROCKS_LLVM_HOME=%s\n' "''${STARROCKS_LLVM_HOME:-}"
      printf 'LLVM_HOME=%s\n' "''${LLVM_HOME:-}"
      printf 'OPENSSL_ROOT_DIR=%s\n' "''${OPENSSL_ROOT_DIR:-}"
      printf 'CURL_ROOT=%s\n' "''${CURL_ROOT:-}"
      printf 'ICU_ROOT=%s\n' "''${ICU_ROOT:-}"
      exit 0
    fi

    if [[ ! -x ./build.sh ]]; then
      echo "starrocks-build-env must be run from the StarRocks repository root" >&2
      exit 1
    fi

    exec ./build.sh "$@"
  '';
in
{
  inherit buildWrapper;

  checkInputs = [
    pkgs.bash
    pkgs.coreutils
    pkgs.gnugrep
    buildWrapper
  ]
  ++ lib.optionals isDarwin [ pkgs.getopt ]
  ++ lib.optionals isLinux [ pkgs.util-linux ];

  devShell = pkgs.mkShell {
    packages = allInputs ++ [ buildWrapper ];

    shellHook = ''
      ${commonExports}
      ${platformExports}

      echo "StarRocks Nix shell ready. Run: starrocks-build-env --print"
    '';
  };
}
