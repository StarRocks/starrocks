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
