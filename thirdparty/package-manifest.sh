#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

starrocks_filter_default_packages() {
    local unsupported_packages="$1"
    local package
    local unsupported_package
    local filtered_packages=()
    local skip_package

    [[ -n "${unsupported_packages}" ]] || return 0

    for package in "${STARROCKS_THIRDPARTY_ALL_PACKAGES[@]}"; do
        skip_package=0
        for unsupported_package in ${unsupported_packages}; do
            if [[ "${package}" == "${unsupported_package}" ]]; then
                skip_package=1
                break
            fi
        done
        if [[ "${skip_package}" -eq 0 ]]; then
            filtered_packages+=("${package}")
        fi
    done

    STARROCKS_THIRDPARTY_ALL_PACKAGES=("${filtered_packages[@]}")
}

starrocks_set_default_packages() {
    local machine_type="$1"

    STARROCKS_THIRDPARTY_ALL_PACKAGES=(
        libevent
        zlib
        lz4
        lzo2
        bzip
        openssl
        boost
        protobuf
        gflags
        gtest
        glog
        rapidjson
        simdjson
        snappy
        gperftools
        curl
        re2
        thrift
        leveldb
        brpc
        rocksdb
        kerberos
        sasl
        absl
        grpc
        flatbuffers
        jemalloc
        brotli
        xsimd
        arrow
        adbc
        librdkafka
        pulsar
        s2
        bitshuffle
        croaringbitmap
        cctz
        fmt
        fmt_shared
        ryu
        hadoop_src
        jdk
        ragel
        hyperscan
        mariadb
        aliyun_jindosdk
        gcs_connector
        aws_cpp_sdk
        vpack
        opentelemetry
        benchmark
        fast_float
        starcache
        streamvbyte
        jansson
        avro_c
        avro_cpp
        serdes
        datasketches
        fiu
        llvm
        clucene
        simdutf
        poco
        icu
        libxml2
        azure
        libdivide
        flamegraph
        tenann
        xxhash
        blake3
        pprof
        benchgen
        paimon_cpp
    )

    if [[ "${machine_type}" != "aarch64" ]]; then
        STARROCKS_THIRDPARTY_ALL_PACKAGES+=(breakpad libdeflate)
    fi

    if [[ "$(uname -s)" == "Darwin" ]]; then
        starrocks_filter_default_packages "${DARWIN_UNSUPPORTED_PACKAGES:-}"
    fi
}

# Print the packages of the default order starting from the given one, i.e. the
# packages a `--continue <package>` run is going to build.
starrocks_packages_from() {
    local start="$1"
    local package
    local found=0

    for package in "${STARROCKS_THIRDPARTY_ALL_PACKAGES[@]}"; do
        if [[ "${package}" == "${start}" ]]; then
            found=1
        fi
        if [[ "${found}" -eq 1 ]]; then
            echo "${package}"
        fi
    done
}

# Print the archive keys the body of build_<name> refers to, following the calls
# it makes to other build_* helpers. An archive is referenced either through its
# extracted directory (X_SOURCE) or through the archive file itself (X_NAME, e.g.
# the ARROW_*_URL pointers build_arrow hands to arrow's bundled builds).
# Recursion is guarded by _starrocks_scanned_functions.
_starrocks_scan_build_function() {
    local name="$1"
    local body
    local callee

    case " ${_starrocks_scanned_functions} " in
    *" ${name} "*)
        return 0
        ;;
    esac
    _starrocks_scanned_functions="${_starrocks_scanned_functions} ${name}"

    body="$(awk -v fn="build_${name}()" '
            index($0, fn) == 1 { inside = 1; next }
            inside && $0 == "}" { inside = 0 }
            inside { print }
        ' "${_starrocks_build_scripts[@]}")"
    if [[ -z "${body}" ]]; then
        return 0
    fi

    printf '%s\n' "${body}" \
        | grep -oE '[A-Z][A-Z0-9_]*_(SOURCE|NAME)([^A-Z0-9_]|$)' \
        | sed -E 's/_(SOURCE|NAME).*$//'

    for callee in $(printf '%s\n' "${body}" \
        | grep -oE '(^|[^A-Za-z0-9_])build_[a-z0-9_]+' \
        | grep -oE 'build_[a-z0-9_]+' | sed 's/^build_//' | sort -u); do
        _starrocks_scan_build_function "${callee}"
    done
}

# Map package names to the archive keys of vars.sh, so that building a subset of
# the packages does not have to download, unpack and patch everything else.
#
# The mapping is derived from the build scripts instead of being hardcoded here:
# whatever archives the body of build_<package> refers to, directly or through the
# helpers it calls, are the archives that package is built from. Keys that vars.sh
# does not declare in TP_ARCHIVES are dropped, and a package that resolves to
# nothing fails the whole call, so callers can fall back to downloading everything.
starrocks_package_archives() {
    local script
    local package
    local key
    local resolved
    local keys=""

    _starrocks_build_scripts=()
    for script in "${TP_DIR}/build-thirdparty.sh" "${TP_DIR}/build-thirdparty-darwin.sh"; do
        if [[ -f "${script}" ]]; then
            _starrocks_build_scripts+=("${script}")
        fi
    done
    if [[ "${#_starrocks_build_scripts[@]}" -eq 0 ]]; then
        return 1
    fi

    for package in "$@"; do
        resolved=""
        _starrocks_scanned_functions=""
        for key in $(_starrocks_scan_build_function "${package}" | sort -u); do
            if [[ " ${TP_ARCHIVES} " == *" ${key} "* ]]; then
                resolved="${resolved} ${key}"
            fi
        done
        if [[ -z "${resolved}" ]]; then
            return 1
        fi
        keys="${keys}${resolved}"
    done

    echo ${keys} | tr ' ' '\n' | sort -u | tr '\n' ' '
}

# Narrow the download/unpack/patch pass down to the archives the given packages
# need. A STARROCKS_THIRDPARTY_ARCHIVES coming from the environment always wins.
starrocks_restrict_archives() {
    local archives

    if [[ -n "${STARROCKS_THIRDPARTY_ARCHIVES:-}" ]] || [[ "$#" -eq 0 ]]; then
        return 0
    fi

    if archives="$(starrocks_package_archives "$@")" && [[ -n "${archives}" ]]; then
        export STARROCKS_THIRDPARTY_ARCHIVES="${archives}"
        echo "Thirdparty archives needed by [$*]: ${archives}"
    else
        echo "Warning: cannot resolve the archives of [$*], downloading all archives"
    fi

    return 0
}

# The --continue <package> variant of the above: restrict to the archives of that
# package and of the ones built after it. An unknown package name resolves to no
# packages at all, which is reported instead of silently leaving the download
# unfiltered.
starrocks_restrict_archives_from() {
    local start="$1"
    local from

    from="$(starrocks_packages_from "${start}")"
    if [[ -z "${from}" ]]; then
        echo "Warning: unknown package [${start}] for --continue, downloading all archives"
        return 0
    fi

    starrocks_restrict_archives ${from}
}
