#!/bin/bash
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

JDK_DOWNLOAD="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_aarch64_mac_hotspot_17.0.13_11.tar.gz"
JDK_NAME="OpenJDK17U-jdk_aarch64_mac_hotspot_17.0.13_11.tar.gz"
JDK_SOURCE="jdk-17.0.13+11"
JDK_MD5SUM="03cca86e04125f01b1f3ac3bdd132305"

# Keep Darwin source-package versions aligned with the root macOS build flow.
BOOST_DOWNLOAD="https://archives.boost.io/release/1.86.0/source/boost_1_86_0.tar.gz"
BOOST_NAME="boost_1_86_0.tar.gz"
BOOST_SOURCE="boost_1_86_0"
BOOST_MD5SUM="ac857d73bb754b718a039830b07b9624"

CURL_DOWNLOAD="https://curl.se/download/curl-8.16.0.tar.gz"
CURL_NAME="curl-8.16.0.tar.gz"
CURL_SOURCE="curl-8.16.0"
CURL_MD5SUM="3db9de72cc8f04166fa02d3173ac78bb"

HYPERSCAN_DOWNLOAD="https://github.com/VectorCamp/vectorscan/archive/refs/tags/vectorscan/5.4.12.tar.gz"
HYPERSCAN_NAME="vectorscan-5.4.12.tar.gz"
HYPERSCAN_SOURCE="vectorscan-vectorscan-5.4.12"
HYPERSCAN_MD5SUM="384eab5b23831993df96e5fa55f9951e"

DARWIN_UNSUPPORTED_PACKAGES="starcache tenann pprof"

darwin_is_unsupported_package() {
    local package_name
    local unsupported_package

    package_name="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"

    for unsupported_package in ${DARWIN_UNSUPPORTED_PACKAGES}; do
        if [[ "${package_name}" == "${unsupported_package}" ]]; then
            return 0
        fi
    done
    return 1
}

filtered_tp_archives=""
for archive in ${TP_ARCHIVES}; do
    if darwin_is_unsupported_package "${archive}"; then
        continue
    fi
    filtered_tp_archives="${filtered_tp_archives} ${archive}"
done
TP_ARCHIVES="${filtered_tp_archives# }"
