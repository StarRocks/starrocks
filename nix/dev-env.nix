{
  pkgs,
  src ? ../.,
  version ? "main",
  commitHash ? "unknown",
}:

let
  lib = pkgs.lib;
  system = pkgs.stdenv.hostPlatform.system;
  isDarwin = pkgs.stdenv.hostPlatform.isDarwin;
  isLinux = pkgs.stdenv.hostPlatform.isLinux;
  supportedPlatforms = [
    "x86_64-linux"
    "aarch64-linux"
    "aarch64-darwin"
  ];
  supportedSystem = lib.elem system supportedPlatforms;
  unsupportedSystemMessage = "Unsupported StarRocks Nix platform '${system}'. Supported platforms: ${lib.concatStringsSep ", " supportedPlatforms}.";
  linuxGcc = pkgs.gcc14;
  thirdpartyArchives = import ./thirdparty-archives.nix {
    inherit pkgs lib;
  };
  thirdpartyArchiveInputs = thirdpartyArchives.fetchedFor system;
  awsCrtArchiveInputs = thirdpartyArchives.fetchedAwsCrt;
  thirdpartyArchiveNames = thirdpartyArchives.archiveNamesFor system;
  thirdpartyPackageNames = thirdpartyArchives.packageNamesFor system;
  thirdpartyArchiveNamesArg = lib.concatStringsSep " " thirdpartyArchiveNames;
  thirdpartyFoundationPackages = [
    "libevent"
    "zlib"
    "lz4"
    "lzo2"
    "bzip"
    "openssl"
    "boost"
    "protobuf"
    "gflags"
    "gtest"
    "glog"
    "rapidjson"
    "snappy"
    "gperftools"
    "curl"
    "re2"
    "thrift"
    "leveldb"
  ];
  thirdpartyRpcPackages = [
    "brpc"
    "rocksdb"
    "kerberos"
    "sasl"
    "absl"
    "grpc"
    "flatbuffers"
    "jemalloc"
    "brotli"
  ];
  thirdpartyFormatsPackages = [
    "xsimd"
    "arrow"
    "librdkafka"
    "pulsar"
    "s2"
    "bitshuffle"
    "croaringbitmap"
    "cctz"
    "fmt"
    "fmt_shared"
    "ryu"
    "hadoop_src"
    "ragel"
    "hyperscan"
    "mariadb"
    "aws_cpp_sdk"
    "vpack"
    "opentelemetry"
    "benchmark"
    "fast_float"
    "jansson"
    "avro_c"
    "avro_cpp"
    "serdes"
    "datasketches"
    "fiu"
    "llvm"
    "clucene"
    "simdutf"
    "poco"
    "icu"
    "libxml2"
    "azure"
    "libdivide"
    "flamegraph"
    "xxhash"
    "blake3"
  ];
  assignedThirdpartyLayerPackages =
    thirdpartyFoundationPackages ++ thirdpartyRpcPackages ++ thirdpartyFormatsPackages;
  thirdpartyLeafPackages = lib.filter (
    package: !(lib.elem package assignedThirdpartyLayerPackages)
  ) thirdpartyPackageNames;
  availableThirdpartyPackages =
    packages: lib.filter (package: lib.elem package thirdpartyPackageNames) packages;
  thirdpartyVarsOverride =
    {
      "x86_64-linux" = "thirdparty/vars-x86_64.sh";
      "aarch64-linux" = "thirdparty/vars-aarch64.sh";
      "aarch64-darwin" = "thirdparty/vars-darwin-aarch64.sh";
    }
    .${system};
  preseedThirdpartyArchives = lib.concatMapStringsSep "\n" (
    archive: ''ln -sf "${archive.path}" "thirdparty/src/${archive.name}"''
  ) thirdpartyArchiveInputs;
  preseedAwsCrtArchives = lib.concatMapStringsSep "\n" (
    archive: ''ln -sf "${archive.path}" "thirdparty/src/aws-crt-archives/${archive.name}.zip"''
  ) awsCrtArchiveInputs;
  mavenDepsHashes = {
    "x86_64-linux" = "sha256-9kFycfLpxOwtSDqbjkIfWxKGVCZ8ig7h1stG5HK4Awo=";
    "aarch64-linux" = "sha256-Y3JNy/QVDFtprV5+7htzS3pHIi4vzqNW7T2EfF3zXaY=";
    "aarch64-darwin" = "sha256-9kFycfLpxOwtSDqbjkIfWxKGVCZ8ig7h1stG5HK4Awo=";
  };
  mavenDepsHash = mavenDepsHashes.${system} or (throw unsupportedSystemMessage);
  packageBuildParallelismLimit =
    {
      "x86_64-linux" = 2;
      "aarch64-linux" = 1;
      "aarch64-darwin" = 4;
    }
    .${system} or (throw unsupportedSystemMessage);

  jdk = pkgs.jdk21_headless;
  jdkHome = if isDarwin then "${jdk.bundle}/Contents/Home" else jdk.home;
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
  snappyRoot = joinedRoot "starrocks-snappy-root" [
    pkgs.snappy.dev
    pkgs.snappy.out
  ];
  brotliRoot = joinedRoot "starrocks-brotli-root" [
    pkgs.brotli.dev
    pkgs.brotli.lib
    pkgs.brotli.out
  ];
  zstdRoot = joinedRoot "starrocks-zstd-root" [
    pkgs.zstd.bin
    pkgs.zstd.dev
    pkgs.zstd.out
  ];
  pcre2Root = joinedRoot "starrocks-pcre2-root" [
    pkgs.pcre2.dev
    pkgs.pcre2.out
  ];
  icuRoot = joinedRoot "starrocks-icu-root" [
    pkgs.icu.dev
    pkgs.icu.out
  ];
  llvmToolchain = joinedRoot "starrocks-llvm-toolchain" [
    clang
    llvm
  ];
  streamvbyte = pkgs.stdenv.mkDerivation {
    pname = "streamvbyte";
    version = "0.5.1";

    src = pkgs.fetchurl {
      url = "https://github.com/lemire/streamvbyte/archive/refs/tags/v0.5.1.tar.gz";
      sha256 = "0287mwpqc83rgs7rmc3iilsl6bnnldf8791ppgs7j2zrh8sl1wmh";
    };

    nativeBuildInputs = [ pkgs.cmake ];

    cmakeFlags = [
      "-DCMAKE_BUILD_TYPE=Release"
      "-DCMAKE_POLICY_VERSION_MINIMUM=3.5"
      "-DSTREAMVBYTE_ENABLE_TESTS=OFF"
    ];
  };

  darwinBrewCompat =
    let
      formulae = {
        "autoconf" = pkgs.autoconf;
        "automake" = pkgs.automake;
        "aws-crt-cpp" = pkgs.aws-crt-cpp;
        "aws-sdk-cpp" = pkgs.aws-sdk-cpp;
        "bison" = pkgs.bison;
        "blake3" = pkgs.libblake3;
        "boost" = pkgs.boost;
        "brotli" = brotliRoot;
        "bzip2" = pkgs.bzip2;
        "cmake" = pkgs.cmake;
        "cyrus-sasl" = pkgs.cyrus_sasl;
        "fast_float" = pkgs.fast-float;
        "flatbuffers" = pkgs.flatbuffers;
        "fmt" = pkgs.fmt;
        "gperftools" = pkgs.gperftools;
        "googletest" = pkgs.gtest;
        "gnu-getopt" = pkgs.getopt;
        "jansson" = pkgs.jansson;
        "krb5" = pkgs.krb5;
        "libdeflate" = pkgs.libdeflate;
        "libevent" = pkgs.libevent;
        "librdkafka" = pkgs.rdkafka;
        "libtool" = pkgs.libtool;
        "libxml2" = pkgs.libxml2;
        "llvm" = llvmToolchain;
        "lz4" = pkgs.lz4;
        "lzo" = pkgs.lzo;
        "ninja" = pkgs.ninja;
        "openjdk@17" = jdk;
        "openssl@3" = opensslRoot;
        "opentelemetry-cpp" = pkgs.opentelemetry-cpp;
        "pcre2" = pcre2Root;
        "pkg-config" = pkgs.pkg-config;
        "ragel" = pkgs.ragel;
        "rapidjson" = pkgs.rapidjson;
        "simdjson" = pkgs.simdjson;
        "snappy" = snappyRoot;
        "streamvbyte" = streamvbyte;
        "wget" = pkgs.wget;
        "xsimd" = pkgs.xsimd;
        "zlib" = pkgs.zlib;
        "zstd" = zstdRoot;
      };
      linkFormula = name: package: ''
        ln -s "${package}" "$out/opt/${name}"
      '';
    in
    pkgs.runCommand "starrocks-brew-compat" { } ''
      mkdir -p "$out/bin" "$out/opt"
      ${lib.concatStringsSep "\n" (lib.mapAttrsToList linkFormula formulae)}

      mkdir -p "$out/opt/coreutils/libexec/gnubin" "$out/opt/gnu-tar/libexec/gnubin"
      for tool in "${pkgs.coreutils}"/bin/*; do
        ln -s "$tool" "$out/opt/coreutils/libexec/gnubin/$(basename "$tool")"
      done
      ln -s "${pkgs.gnutar}/bin/tar" "$out/opt/gnu-tar/libexec/gnubin/tar"

      mkdir -p "$out/include/aws" "$out/include/smithy"
      if [ -d "${pkgs.aws-sdk-cpp}/include/aws" ]; then
        for child in "${pkgs.aws-sdk-cpp}"/include/aws/*; do
          ln -s "$child" "$out/include/aws/$(basename "$child")"
        done
      fi
      if [ -d "${pkgs.aws-crt-cpp}/include/aws" ]; then
        for child in "${pkgs.aws-crt-cpp}"/include/aws/*; do
          target="$out/include/aws/$(basename "$child")"
          [ -e "$target" ] || ln -s "$child" "$target"
        done
      fi
      if [ -d "${pkgs.aws-sdk-cpp}/include/smithy" ]; then
        for child in "${pkgs.aws-sdk-cpp}"/include/smithy/*; do
          ln -s "$child" "$out/include/smithy/$(basename "$child")"
        done
      fi

      cat > "$out/bin/brew" <<EOF
      #!${pkgs.bash}/bin/bash
      set -euo pipefail

      case "\''${1:-}" in
        --prefix)
          if [[ \$# -eq 1 ]]; then
            printf '%s\n' "$out"
          elif [[ -e "$out/opt/\$2" ]]; then
            printf '%s\n' "$out/opt/\$2"
          else
            echo "No Nix formula mapping for \$2" >&2
            exit 1
          fi
          ;;
        list)
          if [[ "\''${2:-}" == "--formula" && -e "$out/opt/\''${3:-}" ]]; then
            exit 0
          fi
          exit 1
          ;;
        install)
          if [[ -e "$out/opt/\''${2:-}" ]]; then
            exit 0
          fi
          echo "No Nix formula mapping for \''${2:-}" >&2
          exit 1
          ;;
        *)
          echo "Unsupported brew command: \$*" >&2
          exit 1
          ;;
      esac
      EOF
      chmod +x "$out/bin/brew"
    '';

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
    m4
    maven
    ninja
    patch
    perl
    pkg-config
    python3
    unzip
    wget
    which
    xz
    zip
  ];

  devOnlyInputs = with pkgs; [
    protobuf
    thrift
  ];

  linuxInputs = lib.optionals isLinux (
    with pkgs;
    [
      binutils
      linuxGcc
      util-linux
    ]
  );
  linuxLinkInputs = lib.optionals isLinux [ pkgs.libiberty.out ];

  darwinInputs = lib.optionals isDarwin ([
    llvmToolchain
    pkgs.darwin.cctools
    pkgs.libiconv
    pkgs.getopt
  ]);

  allInputs = commonInputs ++ linuxInputs ++ darwinInputs;
  packageBuildInputs =
    allInputs
    ++ linuxLinkInputs
    ++ lib.optionals isDarwin [
      pkgs.sysctl
    ];
  thirdpartyBuildInputs =
    packageBuildInputs
    ++ lib.optionals isDarwin [
      darwinBrewCompat
    ];
  toolPath = lib.makeBinPath allInputs;
  packageToolPath = lib.makeBinPath packageBuildInputs;
  thirdpartyToolPath = lib.makeBinPath thirdpartyBuildInputs;
  sourceRoot = src;
  source = lib.cleanSourceWith {
    src = sourceRoot;
    filter =
      path: type:
      let
        relPath = lib.removePrefix "${toString sourceRoot}/" (toString path);
        baseName = baseNameOf path;
      in
      !(
        baseName == ".git"
        || baseName == ".direnv"
        || baseName == "output"
        || baseName == ".m2"
        || lib.hasPrefix "be/build_" relPath
        || lib.hasPrefix "be/output" relPath
        || lib.hasPrefix "thirdparty/src" relPath
        || lib.hasPrefix "thirdparty/installed" relPath
      );
  };
  thirdpartySource = lib.cleanSourceWith {
    src = sourceRoot;
    filter =
      path: type:
      let
        relPath = lib.removePrefix "${toString sourceRoot}/" (toString path);
        baseName = baseNameOf path;
      in
      (
        relPath == ""
        || relPath == "env.sh"
        || relPath == "thirdparty"
        || lib.hasPrefix "thirdparty/" relPath
      )
      && !(
        baseName == ".git"
        || baseName == ".direnv"
        || lib.hasPrefix "thirdparty/src" relPath
        || lib.hasPrefix "thirdparty/installed" relPath
      );
  };

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
        export PCRE2_ROOT_DIR="${pcre2Root}"
      ''
    else
      ''
        export STARROCKS_GCC_HOME="${linuxGcc}"
        export CC="${linuxGcc}/bin/gcc"
        export CXX="${linuxGcc}/bin/g++"
        export LIBRARY_PATH="${pkgs.libiberty.out}/lib''${LIBRARY_PATH:+:$LIBRARY_PATH}"
        export NIX_LDFLAGS="-L${pkgs.libiberty.out}/lib ''${NIX_LDFLAGS:-}"
      '';

  commonExports = ''
    export PATH="${toolPath}:$PATH"
    export STARROCKS_HOME="''${STARROCKS_HOME:-$PWD}"
    export JAVA_HOME="${jdkHome}"
    export CUSTOM_MVN="${pkgs.maven}/bin/mvn"
    export CUSTOM_CMAKE="${pkgs.cmake}/bin/cmake"
    export CMAKE_CMD="${pkgs.cmake}/bin/cmake"
    export CMAKE_POLICY_VERSION_MINIMUM="''${CMAKE_POLICY_VERSION_MINIMUM:-3.5}"
    export PYTHON="${pkgs.python3}/bin/python3"
  '';
  offlineMaven = pkgs.writeShellScript "starrocks-offline-mvn" ''
    exec "${pkgs.maven}/bin/mvn" --offline -nsu -Dmaven.repo.local="$TMPDIR/m2" "$@"
  '';
  parallelBuildExports = ''
    export BUILD_TYPE="''${BUILD_TYPE:-Release}"
    export PARALLEL="''${NIX_BUILD_CORES:-1}"
    export CCACHE_DISABLE=1
  '';
  packageParallelBuildExports = ''
    ${parallelBuildExports}
    starrocks_nix_parallel_limit=${toString packageBuildParallelismLimit}
    starrocks_nix_parallel_default="$starrocks_nix_parallel_limit"
    case "''${NIX_BUILD_CORES:-}" in
      "" | *[!0-9]* | 0) ;;
      *)
        if (( NIX_BUILD_CORES < starrocks_nix_parallel_default )); then
          starrocks_nix_parallel_default="$NIX_BUILD_CORES"
        fi
        ;;
    esac
    export PARALLEL="''${STARROCKS_NIX_PACKAGE_PARALLEL:-$starrocks_nix_parallel_default}"
  '';
  thirdpartyPkgConfigLibdir =
    thirdpartyRoot:
    "${thirdpartyRoot}/installed/lib/pkgconfig:${thirdpartyRoot}/installed/lib64/pkgconfig";

  buildWrapper = pkgs.writeShellScriptBin "starrocks-build-env" ''
    set -euo pipefail

    ${commonExports}
    ${platformExports}

    if [[ "''${1:-}" == "--print" ]]; then
      printf 'STARROCKS_HOME=%s\n' "$STARROCKS_HOME"
      printf 'JAVA_HOME=%s\n' "$JAVA_HOME"
      printf 'CUSTOM_MVN=%s\n' "$CUSTOM_MVN"
      printf 'CUSTOM_CMAKE=%s\n' "$CUSTOM_CMAKE"
      printf 'CMAKE_CMD=%s\n' "$CMAKE_CMD"
      printf 'CMAKE_POLICY_VERSION_MINIMUM=%s\n' "$CMAKE_POLICY_VERSION_MINIMUM"
      printf 'PYTHON=%s\n' "$PYTHON"
      printf 'CC=%s\n' "''${CC:-}"
      printf 'CXX=%s\n' "''${CXX:-}"
      printf 'AR=%s\n' "''${AR:-}"
      printf 'RANLIB=%s\n' "''${RANLIB:-}"
      printf 'STRIP=%s\n' "''${STRIP:-}"
      printf 'LIBRARY_PATH=%s\n' "''${LIBRARY_PATH:-}"
      printf 'NIX_LDFLAGS=%s\n' "''${NIX_LDFLAGS:-}"
      printf 'STARROCKS_USE_NIX_DEPS=%s\n' "''${STARROCKS_USE_NIX_DEPS:-}"
      printf 'STARROCKS_THIRDPARTY=%s\n' "''${STARROCKS_THIRDPARTY:-}"
      printf 'STARROCKS_LLVM_HOME=%s\n' "''${STARROCKS_LLVM_HOME:-}"
      printf 'LLVM_HOME=%s\n' "''${LLVM_HOME:-}"
      printf 'OPENSSL_ROOT_DIR=%s\n' "''${OPENSSL_ROOT_DIR:-}"
      printf 'CURL_ROOT=%s\n' "''${CURL_ROOT:-}"
      printf 'ICU_ROOT=%s\n' "''${ICU_ROOT:-}"
      printf 'PCRE2_ROOT_DIR=%s\n' "''${PCRE2_ROOT_DIR:-}"
      exit 0
    fi

    if [[ ! -x ./build.sh ]]; then
      echo "starrocks-build-env must be run from the StarRocks repository root" >&2
      exit 1
    fi

    exec ./build.sh "$@"
  '';

  mavenDeps = pkgs.stdenvNoCC.mkDerivation {
    pname = "starrocks-maven-deps";
    inherit version;

    src = source;

    nativeBuildInputs = [
      jdk
      pkgs.maven
      pkgs.python3
      pkgs.thrift
    ];

    impureEnvVars = lib.fetchers.proxyImpureEnvVars;

    dontConfigure = true;
    dontInstall = true;
    dontFixup = true;
    outputHashAlgo = "sha256";
    outputHashMode = "recursive";
    outputHash = mavenDepsHash;

    buildPhase = ''
      runHook preBuild

      export HOME="$TMPDIR/home"
      export JAVA_HOME="${jdkHome}"
      export MAVEN_OPTS="''${MAVEN_OPTS:-} -Dmaven.repo.local=$out/.m2"
      export PYTHON="${pkgs.python3}/bin/python3"
      export THRIFT="${pkgs.thrift}/bin/thrift"
      export STARROCKS_HOME="$PWD"

      mkdir -p "$HOME" "$out/.m2"

      mvn_go_offline() {
        ${pkgs.maven}/bin/mvn \
          --batch-mode \
          -DskipTests \
          -Dcheckstyle.skip=true \
          -Dmaven.clean.skip=true \
          -Dmaven.repo.local="$out/.m2" \
          de.qaware.maven:go-offline-maven-plugin:1.2.8:resolve-dependencies \
          "$@"
      }

      cd fe
      mvn_go_offline -pl plugin/hive-udf,fe-testing,plugin/spark-dpp,fe-server -am

      cd ../java-extensions
      mvn_go_offline

      find "$out/.m2" -type f \( \
        -name "*.lastUpdated" \
        -o -name "resolver-status.properties" \
        -o -name "_remote.repositories" \
      \) -delete

      runHook postBuild
    '';
  };

  mkThirdpartyLayer =
    {
      name,
      packageNames,
      deps ? [ ],
    }:
    let
      layerPackageNames = availableThirdpartyPackages packageNames;
      layerPackageArgs = lib.escapeShellArgs layerPackageNames;
      copyLayerDeps = lib.concatMapStringsSep "\n" (
        dep: ''cp -R "${dep}/installed"/. thirdparty/installed/''
      ) deps;
      makeLayerDepsWritable = lib.optionalString (deps != [ ]) ''
        chmod -R u+w thirdparty/installed
      '';
    in
    pkgs.stdenv.mkDerivation {
      pname = "starrocks-thirdparty-${name}";
      inherit version;

      src = thirdpartySource;

      nativeBuildInputs = thirdpartyBuildInputs;

      dontConfigure = true;
      dontFixup = true;
      enableParallelBuilding = true;
      strictDeps = true;

      buildPhase = ''
        runHook preBuild

        patchShebangs .

        export HOME="$TMPDIR/home"
        ${parallelBuildExports}

        mkdir -p \
          "$HOME" \
          thirdparty/src/aws-crt-archives \
          thirdparty/installed/lib/pkgconfig \
          thirdparty/installed/lib64/pkgconfig

        ${copyLayerDeps}
        ${makeLayerDepsWritable}

        ${commonExports}
        export PATH="${thirdpartyToolPath}:$PATH"
        ${platformExports}
        unset OPENSSL_ROOT_DIR CURL_ROOT ICU_ROOT OPENSSL_DIR OPENSSL_INCLUDE_DIR OPENSSL_LIB_DIR
        export PKG_CONFIG_PATH=
        export PKG_CONFIG_LIBDIR="${thirdpartyPkgConfigLibdir "$PWD/thirdparty"}"
        export STARROCKS_SKIP_THIRDPARTY_DOWNLOAD=1
        export STARROCKS_THIRDPARTY_ARCHIVES="${thirdpartyArchiveNamesArg}"
        export STARROCKS_TP_VARS_OVERRIDE="${thirdpartyVarsOverride}"
        export STARROCKS_AWS_CRT_ARCHIVES_DIR="$PWD/thirdparty/src/aws-crt-archives"

        ${preseedThirdpartyArchives}
        ${preseedAwsCrtArchives}

        ./thirdparty/download-thirdparty.sh
        patchShebangs thirdparty/src/*/

        ./thirdparty/build-thirdparty.sh -j "$PARALLEL" ${layerPackageArgs}

        runHook postBuild
      '';

      installPhase = ''
        runHook preInstall

        mkdir -p "$out"
        cp -R thirdparty/installed "$out/installed"

        runHook postInstall
      '';

      passthru = {
        inherit layerPackageNames;
      };

      meta = {
        description = "Native StarRocks third-party dependency layer: ${name}";
        homepage = "https://starrocks.io/";
        license = lib.licenses.asl20;
        platforms = supportedPlatforms;
      };
    };

  starrocksThirdpartyFoundation = mkThirdpartyLayer {
    name = "foundation";
    packageNames = thirdpartyFoundationPackages;
  };
  starrocksThirdpartyRpc = mkThirdpartyLayer {
    name = "rpc";
    packageNames = thirdpartyRpcPackages;
    deps = [ starrocksThirdpartyFoundation ];
  };
  starrocksThirdpartyFormats = mkThirdpartyLayer {
    name = "formats";
    packageNames = thirdpartyFormatsPackages;
    deps = [ starrocksThirdpartyRpc ];
  };
  starrocksThirdpartyLeaf = mkThirdpartyLayer {
    name = "leaf";
    packageNames = thirdpartyLeafPackages;
    deps = [ starrocksThirdpartyFormats ];
  };
  starrocksThirdparty = pkgs.runCommand "starrocks-thirdparty-${version}" { } ''
    mkdir -p "$out"
    cp -R "${starrocksThirdpartyLeaf}/installed" "$out/installed"
    chmod -R u+w "$out/installed"

    ${lib.optionalString isDarwin ''
      for dylib in \
        "${pkgs.krb5.lib}"/lib/libkrb5support*.dylib \
        "${pkgs.krb5.lib}"/lib/libkrb5*.dylib \
        "${pkgs.krb5.lib}"/lib/libcom_err*.dylib \
        "${pkgs.krb5.lib}"/lib/libk5crypto*.dylib \
        "${pkgs.krb5.lib}"/lib/libgssapi_krb5*.dylib \
        "${pkgs.cyrus_sasl.out}"/lib/libsasl2*.dylib
      do
        if [[ -e "$dylib" || -L "$dylib" ]]; then
          ln -sf "$dylib" "$out/installed/lib/$(basename "$dylib")"
        fi
      done
    ''}

    test -x "$out/installed/bin/protoc"
    test -x "$out/installed/bin/thrift"
  '';

  starrocks = pkgs.stdenv.mkDerivation {
    pname = "starrocks";
    inherit version;

    src = source;

    nativeBuildInputs = packageBuildInputs ++ [ buildWrapper ];
    buildInputs = linuxLinkInputs;

    dontConfigure = true;
    dontFixup = true;
    enableParallelBuilding = true;
    strictDeps = true;

    buildPhase = ''
      runHook preBuild

      patchShebangs .

      export HOME="$TMPDIR/home"
      export MAVEN_OPTS="''${MAVEN_OPTS:-} -Dmaven.repo.local=$TMPDIR/m2"
      export STARROCKS_VERSION="${version}"
      export STARROCKS_COMMIT_HASH="${commitHash}"
      export STARROCKS_OUTPUT="$TMPDIR/starrocks-output"
      ${packageParallelBuildExports}

      mkdir -p \
        "$HOME" \
        "$TMPDIR/m2" \
        "$STARROCKS_OUTPUT"

      ${commonExports}
      export PATH="${packageToolPath}:$PATH"
      ${platformExports}
      unset OPENSSL_DIR OPENSSL_INCLUDE_DIR OPENSSL_LIB_DIR
      export OPENSSL_ROOT_DIR="${starrocksThirdparty}/installed"
      export CURL_ROOT="${starrocksThirdparty}/installed"
      export ICU_ROOT="${starrocksThirdparty}/installed"
      export PCRE2_ROOT_DIR="${pcre2Root}"
      export PKG_CONFIG_PATH=
      export PKG_CONFIG_LIBDIR="${thirdpartyPkgConfigLibdir starrocksThirdparty}"
      export STARROCKS_THIRDPARTY="${starrocksThirdparty}"

      cp -R "${mavenDeps}/.m2"/. "$TMPDIR/m2"/
      chmod -R +w "$TMPDIR/m2"
      export CUSTOM_MVN="${offlineMaven}"

      ./build.sh \
        --fe \
        --be \
        --clean \
        -j "$PARALLEL" \
        --without-starcache \
        --without-tenann \
        --with-maven-batch-mode ON \
        --disable-java-check-style \
        --output "$STARROCKS_OUTPUT"

      runHook postBuild
    '';

    installPhase = ''
      runHook preInstall

      mkdir -p "$out"
      cp -R "$STARROCKS_OUTPUT"/. "$out"/

      test -f "$out/LICENSE.txt"
      test -f "$out/NOTICE.txt"
      test -d "$out/fe"
      test -d "$out/be"

      runHook postInstall
    '';

    meta = {
      description = "Native StarRocks build from source";
      homepage = "https://starrocks.io/";
      license = lib.licenses.asl20;
      platforms = supportedPlatforms;
    };
  };
  mkDevShell =
    {
      thirdpartyRoot ? null,
      message,
    }:
    pkgs.mkShell {
      buildInputs = linuxLinkInputs;

      packages =
        (if thirdpartyRoot == null then allInputs else packageBuildInputs)
        ++ devOnlyInputs
        ++ [ buildWrapper ];

      shellHook = ''
        ${commonExports}
        ${platformExports}
        ${lib.optionalString (thirdpartyRoot != null) ''
          export STARROCKS_THIRDPARTY="${thirdpartyRoot}"
        ''}

        echo "${message}"
      '';
    };
in
assert lib.assertMsg supportedSystem unsupportedSystemMessage;
{
  inherit
    buildWrapper
    mavenDeps
    starrocks
    starrocksThirdparty
    starrocksThirdpartyFoundation
    starrocksThirdpartyRpc
    starrocksThirdpartyFormats
    starrocksThirdpartyLeaf
    ;

  checkInputs = [
    pkgs.bash
    pkgs.coreutils
    pkgs.diffutils
    pkgs.gnugrep
    pkgs.python3
    buildWrapper
  ]
  ++ linuxLinkInputs
  ++ lib.optionals isDarwin [ pkgs.getopt ]
  ++ lib.optionals isLinux [ pkgs.util-linux ];
  checkLinkInputs = linuxLinkInputs;

  devShell = mkDevShell {
    message = "StarRocks Nix shell ready. Run: starrocks-build-env --print";
  };

  beDevShell = mkDevShell {
    thirdpartyRoot = starrocksThirdparty;
    message = "StarRocks BE Nix shell ready. Run: starrocks-build-env --be";
  };
}
