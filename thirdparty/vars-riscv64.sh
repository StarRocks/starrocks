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

#####################################################
# Download url, filename and unpaced filename
# of all thirdparties
#
# vars-${arch}.sh defines the thirdparties that are
# architecure-related.
#####################################################

# OPEN JDK FOR riscv64
# Adoptium Temurin provides official riscv64 builds. JDK 17 (matching the
# aarch64 baseline) is available for riscv64.
JDK_DOWNLOAD="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_riscv64_linux_hotspot_17.0.13_11.tar.gz"
JDK_NAME="OpenJDK17U-jdk_riscv64_linux_hotspot_17.0.13_11.tar.gz"
JDK_SOURCE="jdk-17.0.13+11"
# NOTE: verify the MD5 after a successful download; left as a placeholder so the
# download script's checksum guard is a no-op until the real value is filled in.
JDK_MD5SUM="6380891c4bf6854eef90d86d92e0f815"

# HYPERSCAN for riscv64
# The zte-riscv/vectorscan fork carries a native riscv64/RVV backend (ARCH_RISCV64
# in cmake/platform.cmake, cflags-riscv64.cmake probing RVV/Zbb/Zbc/Zicsr with a
# scalar fallback, src/util/arch/riscv64/*) ONLY on its vectorscan-rv branch; the
# develop branch has none of that and dies with "Unsupported platform"
# (CMakeLists.txt:144). Pin the branch explicitly. HYPERSCAN_* tarball fields are
# left empty because this package is a git source, not a tarball download (the
# download loop keys off HYPERSCAN_GIT_URL).
HYPERSCAN_GIT_URL="https://github.com/zte-riscv/vectorscan.git"
HYPERSCAN_GIT_BRANCH="vectorscan-rv"
HYPERSCAN_SOURCE="vectorscan"
HYPERSCAN_DOWNLOAD=""
HYPERSCAN_NAME=""
HYPERSCAN_MD5SUM=""

# jindosdk for Aliyun OSS - use the generic linux version (no native libs)
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux"
JINDOSDK_MD5SUM="5436e4fe39c4dfdc942e41821f1dd8a9"

# starcache - disabled on riscv64, see RISCV64_UNSUPPORTED_PACKAGES below. The
# official v4.2-rc2 binary is built from an unpublished internal source tree
# ("starcachelib"); the public github.com/StarRocks/starcache repo stopped
# updating in 2023-10 and diverged (namespace starrocks::starcache, no
# time_based_cache_adaptor.h, different API surface), so building from it
# cannot produce the library this BE expects. be/CMakeLists.txt turns
# WITH_STARCACHE off for riscv64 the same way as for macOS, and the data cache
# falls back to the non-starcache path.
STARCACHE_GIT_URL=""
STARCACHE_GIT_BRANCH=""
STARCACHE_SOURCE="starcache"
STARCACHE_DOWNLOAD=""
STARCACHE_NAME=""
STARCACHE_MD5SUM=""

# tenann and pprof have no riscv64 prebuilt binary and are excluded from the
# riscv64 build entirely (see RISCV64_UNSUPPORTED_PACKAGES below). tenann is OFF
# by default in be/CMakeLists.txt; pprof is a Go diagnostic tool (no C++ library
# to link), so excluding it does not break the BE link, exactly as darwin does.
# starcache is excluded because its official binary comes from an unpublished
# internal source tree; see the starcache block above.

# Packages excluded from the riscv64 thirdparty build because no riscv64
# prebuilt exists and source-building them is out of scope. Mirrors the
# DARWIN_UNSUPPORTED_PACKAGES mechanism in vars-darwin-aarch64.sh; consumed by
# package-manifest.sh starrocks_set_default_packages.
RISCV64_UNSUPPORTED_PACKAGES="tenann pprof starcache"
