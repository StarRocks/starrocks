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

starrocks_resolve_tool_command() {
    local tool_name="$1"

    if [[ -z "${tool_name}" ]]; then
        return 1
    fi

    if [[ "${tool_name}" == */* ]]; then
        [[ -x "${tool_name}" ]] || return 1
        echo "${tool_name}"
        return 0
    fi

    command -v "${tool_name}" 2>/dev/null
}

starrocks_same_tool_binary() {
    local left_path="$1"
    local right_path="$2"
    local left_id=""
    local right_id=""

    if [[ -z "${left_path}" || -z "${right_path}" || ! -e "${left_path}" || ! -e "${right_path}" ]]; then
        return 1
    fi

    if starrocks_is_darwin; then
        left_id="$(stat -f '%d:%i' "${left_path}" 2>/dev/null || true)"
        right_id="$(stat -f '%d:%i' "${right_path}" 2>/dev/null || true)"
    else
        left_id="$(stat -Lc '%d:%i' "${left_path}" 2>/dev/null || true)"
        right_id="$(stat -Lc '%d:%i' "${right_path}" 2>/dev/null || true)"
    fi

    [[ -n "${left_id}" && "${left_id}" == "${right_id}" ]]
}

starrocks_resolve_c_compiler() {
    if [[ -n "${CC:-}" ]]; then
        starrocks_resolve_tool_command "${CC}"
        return $?
    fi
    if [[ -n "${STARROCKS_GCC_HOME:-}" && -x "${STARROCKS_GCC_HOME}/bin/gcc" ]]; then
        echo "${STARROCKS_GCC_HOME}/bin/gcc"
        return 0
    fi
    if [[ -n "${STARROCKS_LLVM_HOME:-}" && -x "${STARROCKS_LLVM_HOME}/bin/clang" ]]; then
        echo "${STARROCKS_LLVM_HOME}/bin/clang"
        return 0
    fi
    starrocks_resolve_tool_command cc
}

starrocks_resolve_cxx_compiler() {
    if [[ -n "${CXX:-}" ]]; then
        starrocks_resolve_tool_command "${CXX}"
        return $?
    fi
    if [[ -n "${STARROCKS_GCC_HOME:-}" && -x "${STARROCKS_GCC_HOME}/bin/g++" ]]; then
        echo "${STARROCKS_GCC_HOME}/bin/g++"
        return 0
    fi
    if [[ -n "${STARROCKS_LLVM_HOME:-}" && -x "${STARROCKS_LLVM_HOME}/bin/clang++" ]]; then
        echo "${STARROCKS_LLVM_HOME}/bin/clang++"
        return 0
    fi
    starrocks_resolve_tool_command c++
}

starrocks_extract_tool_version() {
    local tool_path="$1"
    local version_line=""

    if [[ -z "${tool_path}" || ! -x "${tool_path}" ]]; then
        return 1
    fi

    version_line="$("${tool_path}" --version 2>/dev/null | sed -n '1p')"
    if [[ -z "${version_line}" ]]; then
        return 1
    fi

    # CMake caches AppleClang as semantic-version plus the first three clang build components,
    # e.g. "17.0.0.17000604" for "Apple clang version 17.0.0 (clang-1700.6.4.2)".
    if [[ "${version_line}" =~ ^Apple[[:space:]](LLVM|clang)[[:space:]]version[[:space:]]([0-9]+(\.[0-9]+)+)[[:space:]]+\(clang-([0-9]+)\.([0-9]+)\.([0-9]+)(\.[0-9]+)*\)$ ]]; then
        printf '%s.%d%02d%02d\n' \
            "${BASH_REMATCH[2]}" \
            "$((10#${BASH_REMATCH[4]}))" \
            "$((10#${BASH_REMATCH[5]}))" \
            "$((10#${BASH_REMATCH[6]}))"
        return 0
    fi

    echo "${version_line}" | awk '
        {
            for (i = 1; i <= NF; ++i) {
                if ($i ~ /^[0-9]+(\.[0-9]+)+$/) {
                    print $i
                    exit
                }
            }
        }
    '
}

starrocks_extract_cmake_set_value() {
    local cmake_file="$1"
    local variable_name="$2"

    if [[ ! -f "${cmake_file}" ]]; then
        return 1
    fi

    awk -v key="${variable_name}" '
        index($0, "set(" key " ") == 1 {
            value = substr($0, length("set(" key " ") + 1)
            sub(/\)$/, "", value)
            sub(/^"/, "", value)
            sub(/"$/, "", value)
            print value
            exit
        }
    ' "${cmake_file}"
}

starrocks_find_cmake_compiler_dir() {
    local build_dir="$1"

    if [[ ! -d "${build_dir}/CMakeFiles" ]]; then
        return 1
    fi

    find "${build_dir}/CMakeFiles" -mindepth 1 -maxdepth 1 -type d -name '[0-9]*' -print 2>/dev/null | LC_ALL=C sort | tail -n 1
}

starrocks_reconcile_be_build_dir() {
    local build_dir="$1"
    local compiler_dir=""
    local c_compiler_file=""
    local cxx_compiler_file=""
    local cached_c_compiler=""
    local cached_cxx_compiler=""
    local cached_c_version=""
    local cached_cxx_version=""
    local current_c_compiler=""
    local current_cxx_compiler=""
    local current_c_version=""
    local current_cxx_version=""
    local mismatch_reasons=()

    if [[ ! -d "${build_dir}" ]]; then
        return 0
    fi

    compiler_dir="$(starrocks_find_cmake_compiler_dir "${build_dir}")"
    if [[ -z "${compiler_dir}" ]]; then
        return 0
    fi

    c_compiler_file="${compiler_dir}/CMakeCCompiler.cmake"
    cxx_compiler_file="${compiler_dir}/CMakeCXXCompiler.cmake"

    cached_c_compiler="$(starrocks_extract_cmake_set_value "${c_compiler_file}" "CMAKE_C_COMPILER" || true)"
    cached_cxx_compiler="$(starrocks_extract_cmake_set_value "${cxx_compiler_file}" "CMAKE_CXX_COMPILER" || true)"
    cached_c_version="$(starrocks_extract_cmake_set_value "${c_compiler_file}" "CMAKE_C_COMPILER_VERSION" || true)"
    cached_cxx_version="$(starrocks_extract_cmake_set_value "${cxx_compiler_file}" "CMAKE_CXX_COMPILER_VERSION" || true)"

    current_c_compiler="$(starrocks_resolve_c_compiler || true)"
    current_cxx_compiler="$(starrocks_resolve_cxx_compiler || true)"
    current_c_version="$(starrocks_extract_tool_version "${current_c_compiler}" || true)"
    current_cxx_version="$(starrocks_extract_tool_version "${current_cxx_compiler}" || true)"

    if [[ -n "${cached_c_compiler}" && -n "${current_c_compiler}" &&
          "${cached_c_compiler}" != "${current_c_compiler}" ]] &&
       ! starrocks_same_tool_binary "${cached_c_compiler}" "${current_c_compiler}"; then
        mismatch_reasons+=("C compiler path changed: cached ${cached_c_compiler}, current ${current_c_compiler}")
    fi
    if [[ -n "${cached_cxx_compiler}" && -n "${current_cxx_compiler}" &&
          "${cached_cxx_compiler}" != "${current_cxx_compiler}" ]] &&
       ! starrocks_same_tool_binary "${cached_cxx_compiler}" "${current_cxx_compiler}"; then
        mismatch_reasons+=("C++ compiler path changed: cached ${cached_cxx_compiler}, current ${current_cxx_compiler}")
    fi
    if [[ -n "${cached_c_version}" && -n "${current_c_version}" && "${cached_c_version}" != "${current_c_version}" ]]; then
        mismatch_reasons+=("C compiler version changed: cached ${cached_c_version}, current ${current_c_version}")
    fi
    if [[ -n "${cached_cxx_version}" && -n "${current_cxx_version}" && "${cached_cxx_version}" != "${current_cxx_version}" ]]; then
        mismatch_reasons+=("C++ compiler version changed: cached ${cached_cxx_version}, current ${current_cxx_version}")
    fi

    if (( ${#mismatch_reasons[@]} == 0 )); then
        return 0
    fi

    echo "Detected a compiler toolchain change for ${build_dir}."
    echo "Removing the stale BE build directory so CMake can regenerate PCH artifacts."
    printf '  - %s\n' "${mismatch_reasons[@]}"
    rm -rf "${build_dir}"
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
    . "${STARROCKS_HOME}/build-mac/env_macos.sh"

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
