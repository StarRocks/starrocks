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

if [[ -n "${STARROCKS_BUILD_HELPERS_LOADED:-}" ]]; then
    return 0
fi
export STARROCKS_BUILD_HELPERS_LOADED=1

starrocks_is_darwin() {
    [[ "${OSTYPE:-}" == darwin* ]]
}

starrocks_detect_parallelism() {
    local cpu_count=""

    if starrocks_is_darwin; then
        cpu_count="$(sysctl -n hw.ncpu 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    else
        cpu_count="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    fi

    if [[ -z "${cpu_count}" || ! "${cpu_count}" =~ ^[0-9]+$ || "${cpu_count}" -lt 1 ]]; then
        cpu_count=1
    fi
    echo "${cpu_count}"
}

starrocks_default_ut_thin_archive() {
    if starrocks_is_darwin; then
        echo "OFF"
    else
        echo "ON"
    fi
}

starrocks_collect_test_binaries() {
    local test_dir="$1"
    local module_pattern="$2"

    find "${test_dir}" -type f -perm -111 -name "*test" \
        | grep -v starrocks_test \
        | grep -v starrocks_dw_test \
        | grep -v bench_test \
        | grep -v builtin_functions_fuzzy_test \
        | grep -i -e "${module_pattern}" || true
}

starrocks_resolve_java_home() {
    local java_home_candidate="$1"

    if [[ -z "${java_home_candidate}" ]]; then
        return 1
    fi
    if [[ -f "${java_home_candidate}/Contents/Home/include/jni.h" ]]; then
        echo "${java_home_candidate}/Contents/Home"
        return 0
    fi
    if [[ -f "${java_home_candidate}/libexec/openjdk.jdk/Contents/Home/include/jni.h" ]]; then
        echo "${java_home_candidate}/libexec/openjdk.jdk/Contents/Home"
        return 0
    fi
    if [[ -f "${java_home_candidate}/include/jni.h" ]]; then
        echo "${java_home_candidate}"
        return 0
    fi
    echo "${java_home_candidate}"
}

starrocks_setup_darwin_build_env() {
    local bundled_java_home=""

    export STARROCKS_ENV_QUIET=1
    . "${STARROCKS_HOME}/build-support/darwin_build_env.sh"

    unset STARROCKS_GCC_HOME

    MVN_CMD=${CUSTOM_MVN:-mvn}
    export MVN_CMD
    CMAKE_CMD=${CUSTOM_CMAKE:-cmake}
    export CMAKE_CMD
    BUILD_SYSTEM=ninja
    export BUILD_SYSTEM

    bundled_java_home="${STARROCKS_THIRDPARTY}/installed/open_jdk"
    if [[ -n "${JAVA_HOME:-}" ]]; then
        export JAVA_HOME="$(starrocks_resolve_java_home "${JAVA_HOME}")"
    elif [[ -d "${bundled_java_home}" ]]; then
        export JAVA_HOME="$(starrocks_resolve_java_home "${bundled_java_home}")"
    fi
}

starrocks_reset_stale_darwin_cmake_cache() {
    local build_dir="$1"
    local cache_path=""
    local cached_tool_paths=""
    local tool_path=""

    if ! starrocks_is_darwin; then
        return 0
    fi

    cache_path="${build_dir}/CMakeCache.txt"
    if [[ ! -f "${cache_path}" ]]; then
        return 0
    fi

    cached_tool_paths="$(
        sed -n -E \
            's/^(CMAKE_AR|CMAKE_RANLIB|CMAKE_C_COMPILER_AR|CMAKE_C_COMPILER_RANLIB|CMAKE_CXX_COMPILER_AR|CMAKE_CXX_COMPILER_RANLIB):FILEPATH=(.*)$/\2/p' \
            "${cache_path}"
    )"

    while IFS= read -r tool_path; do
        if [[ -n "${tool_path}" && ! -e "${tool_path}" ]]; then
            echo "[INFO] Reset stale CMake cache in ${build_dir} because ${tool_path} is missing"
            rm -rf "${build_dir}"
            mkdir -p "${build_dir}"
            return 0
        fi
    done <<< "${cached_tool_paths}"
}

starrocks_validate_darwin_thirdparty() {
    local tp_root="${STARROCKS_THIRDPARTY}"
    local tp_installed="${tp_root}/installed"

    if [[ ! -d "${tp_installed}" ]]; then
        if [[ -d "${tp_root}/bin" || -d "${tp_root}/lib" || -d "${tp_root}/lib64" ]]; then
            echo "Error: STARROCKS_THIRDPARTY must point to the thirdparty root, not the installed directory: ${tp_root}"
        else
            echo "Error: macOS BE build requires STARROCKS_THIRDPARTY to point to a prepared Darwin thirdparty root."
            echo "Expected directory: ${tp_installed}"
        fi
        exit 1
    fi

    local required_paths=(
        "${tp_installed}/bin/protoc"
        "${tp_installed}/bin/thrift"
    )
    local required_path=""
    for required_path in "${required_paths[@]}"; do
        if [[ ! -e "${required_path}" ]]; then
            echo "Error: missing macOS thirdparty artifact: ${required_path}"
            exit 1
        fi
    done

    # libgrpc.a / libgrpc++.a: be/cmake_modules/ThirdParty.cmake does
    # find_package(gRPC CONFIG REQUIRED) against ${tp_installed}/lib/cmake/grpc.
    # Failing here gives a clear rebuild-thirdparty error up front.
    local required_libs=(
        "libprotobuf.a"
        "librocksdb.a"
        "libglog.a"
        "libbrpc.a"
        "libgrpc.a"
        "libgrpc++.a"
    )
    local required_lib=""
    for required_lib in "${required_libs[@]}"; do
        if [[ ! -e "${tp_installed}/lib/${required_lib}" && ! -e "${tp_installed}/lib64/${required_lib}" ]]; then
            echo "Error: missing macOS thirdparty artifact: ${required_lib} under ${tp_installed}/lib or ${tp_installed}/lib64"
            exit 1
        fi
    done

    # gRPC is built from source as a static archive (libgrpc.a / libgrpc++.a)
    # and consumed via ${tp_installed}/lib/cmake/grpc/gRPCConfig.cmake, matching
    # the Linux flow. Only list dylibs that Homebrew still provides.
    local required_darwin_dylibs=(
        "libkrb5support.dylib"
        "libkrb5.dylib"
        "libcom_err.dylib"
        "libk5crypto.dylib"
        "libgssapi_krb5.dylib"
        "libsasl2.dylib"
        "libxml2.dylib"
    )
    local required_darwin_dylib=""
    for required_darwin_dylib in "${required_darwin_dylibs[@]}"; do
        if [[ ! -e "${tp_installed}/lib/${required_darwin_dylib}" &&
              ! -e "${tp_installed}/lib64/${required_darwin_dylib}" ]]; then
            echo "Error: missing macOS thirdparty dylib: ${required_darwin_dylib} under ${tp_installed}/lib or ${tp_installed}/lib64"
            exit 1
        fi
    done

    local jemalloc_shared_lib=""
    jemalloc_shared_lib=$(find "${tp_installed}/jemalloc/lib-shared" -maxdepth 1 -name 'libjemalloc*.dylib' -print -quit 2>/dev/null)
    if [[ -z "${jemalloc_shared_lib}" ]]; then
        echo "Error: missing macOS thirdparty jemalloc dylib under ${tp_installed}/jemalloc/lib-shared"
        exit 1
    fi

    local bundled_java_home="$(starrocks_resolve_java_home "${tp_installed}/open_jdk")"
    local resolved_java_home=""
    if [[ -n "${JAVA_HOME:-}" ]]; then
        resolved_java_home="$(starrocks_resolve_java_home "${JAVA_HOME}")"
    fi
    local bundled_jni_header="${bundled_java_home}/include/jni.h"
    local java_home_jni_header=""
    if [[ -n "${resolved_java_home}" ]]; then
        java_home_jni_header="${resolved_java_home}/include/jni.h"
    fi
    if [[ ! -f "${bundled_jni_header}" && ! -f "${java_home_jni_header}" ]]; then
        echo "Error: missing JNI headers. Expected ${bundled_jni_header} or ${resolved_java_home}/include/jni.h"
        exit 1
    fi

    local bundled_libjvm=""
    local java_home_libjvm=""
    if [[ -d "${bundled_java_home}/lib" ]]; then
        bundled_libjvm=$(find "${bundled_java_home}/lib" -name 'libjvm.dylib' -print -quit 2>/dev/null)
    fi
    if [[ -n "${resolved_java_home}" && -d "${resolved_java_home}/lib" ]]; then
        java_home_libjvm=$(find "${resolved_java_home}/lib" -name 'libjvm.dylib' -print -quit 2>/dev/null)
    fi
    if [[ -z "${bundled_libjvm}" && -z "${java_home_libjvm}" ]]; then
        echo "Error: missing libjvm.dylib under ${bundled_java_home} and ${resolved_java_home:-${JAVA_HOME:-JAVA_HOME}}"
        exit 1
    fi
}

starrocks_resolve_getopt_bin() {
    local gnu_getopt_prefix=""

    if ! starrocks_is_darwin; then
        echo "getopt"
        return 0
    fi

    if command -v brew >/dev/null 2>&1; then
        gnu_getopt_prefix="$(brew --prefix gnu-getopt 2>/dev/null)"
        if [[ -n "${gnu_getopt_prefix}" ]] && [[ -x "${gnu_getopt_prefix}/bin/getopt" ]]; then
            echo "${gnu_getopt_prefix}/bin/getopt"
            return 0
        fi
        echo "gnu-getopt is required on macOS. Please install it with 'brew install gnu-getopt'." >&2
        return 1
    fi

    echo "Homebrew is required on macOS to install gnu-getopt. Please install Homebrew first." >&2
    return 1
}

starrocks_collect_host_dylib_dependencies() {
    local scan_path="$1"

    if [[ ! -e "${scan_path}" ]]; then
        return 0
    fi

    otool -L "${scan_path}" 2>/dev/null | awk 'NR > 1 {print $1}' | while IFS= read -r dep_path; do
        case "${dep_path}" in
            ""|/usr/lib/*|/System/Library/*|@rpath/*|@loader_path/*|@executable_path/*)
                continue
                ;;
            /*)
                printf '%s\n' "${dep_path}"
                ;;
        esac
    done
}

starrocks_append_unbundled_host_dylib_dependencies() {
    local scan_path="$1"
    local packaged_basename_file="$2"
    local output_file="$3"
    local dep_path=""
    local dep_basename=""

    while IFS= read -r dep_path; do
        dep_basename="$(basename "${dep_path}")"
        if [[ -n "${dep_basename}" ]] && grep -Fxq "${dep_basename}" "${packaged_basename_file}"; then
            continue
        fi
        printf '%s\n' "${dep_path}" >> "${output_file}"
    done < <(starrocks_collect_host_dylib_dependencies "${scan_path}")
}

starrocks_write_output_be_host_dylib_manifest() {
    local output_be_dir="$1"
    local manifest_path="${output_be_dir}/HOST_DYLIB_DEPENDENCIES.txt"
    local tmp_manifest=""
    local packaged_basename_file=""
    local packaged_dylib=""

    tmp_manifest="$(mktemp "${TMPDIR:-/tmp}/starrocks-host-dylibs.XXXXXX")" || {
        echo "Error: failed to create temporary file for macOS host dylib dependency manifest"
        exit 1
    }
    packaged_basename_file="$(mktemp "${TMPDIR:-/tmp}/starrocks-packaged-dylibs.XXXXXX")" || {
        rm -f "${tmp_manifest}"
        echo "Error: failed to create temporary file for packaged macOS dylib names"
        exit 1
    }

    find "${output_be_dir}/lib" -type f \( -name '*.dylib' -o -name '*.dylib.*' \) -exec basename {} \; | sort -u > "${packaged_basename_file}"

    starrocks_append_unbundled_host_dylib_dependencies "${output_be_dir}/lib/starrocks_be" "${packaged_basename_file}" "${tmp_manifest}"

    while IFS= read -r -d '' packaged_dylib; do
        starrocks_append_unbundled_host_dylib_dependencies "${packaged_dylib}" "${packaged_basename_file}" "${tmp_manifest}"
    done < <(find "${output_be_dir}/lib" -type f \( -name '*.dylib' -o -name '*.dylib.*' \) -print0)

    {
        echo "# macOS development note for output/be"
        echo "# This package is not self-contained."
        echo "# It depends on host dynamic libraries not bundled in output/be."
        echo "# Rebuild on the target machine or install compatible packages if any path is missing."
        echo
        if [[ -s "${tmp_manifest}" ]]; then
            sort -u "${tmp_manifest}"
        else
            echo "# No non-system absolute host dylib dependencies were detected."
        fi
    } > "${manifest_path}"

    rm -f "${tmp_manifest}"
    rm -f "${packaged_basename_file}"
}
