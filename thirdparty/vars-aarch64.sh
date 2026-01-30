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

# OPEN JDK FOR aarch64
JDK_DOWNLOAD="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_aarch64_linux_hotspot_17.0.13_11.tar.gz"
JDK_NAME="OpenJDK17U-jdk_aarch64_linux_hotspot_17.0.13_11.tar.gz"
JDK_SOURCE="jdk-17.0.13+11"
JDK_MD5SUM="2e942562e2ffa7378c4948041911c3bb"

# HYPERSCAN for aarch64, provided by huawei kunpeng.
HYPERSCAN_DOWNLOAD="https://github.com/kunpengcompute/hyperscan/archive/refs/tags/v5.3.0.aarch64.tar.gz"
HYPERSCAN_NAME="hyperscan-5.3.0.aarch64.tar.gz"
HYPERSCAN_SOURCE="hyperscan-5.3.0.aarch64"
HYPERSCAN_MD5SUM="ef337257bde6583242a739fab6fb161f"

# jindosdk for Aliyun OSS
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux-el7-aarch64"
JINDOSDK_MD5SUM="27a4e2cd9a403c6e21079a866287d88b"

# tenann
TENANN_DOWNLOAD="https://github.com/StarRocks/tenann/releases/download/v0.5.0-RELEASE/tenann-v0.5.0-RELEASE-arm64-nosve.tar.gz"
TENANN_NAME="tenann-v0.5.0-RELEASE-arm64-nosve.tar.gz"
TENANN_SOURCE="tenann-v0.5.0-RELEASE"
TENANN_MD5SUM="18c61b80e3e4039bd3c6ca95231cb645"
# uncomment this for SVE version for better performance on ARM servers with SVE support
#TENANN_DOWNLOAD="https://github.com/StarRocks/tenann/releases/download/v0.5.0-RELEASE/tenann-v0.5.0-RELEASE-arm64.tar.gz"
#TENANN_NAME="tenann-v0.5.0-RELEASE-arm64.tar.gz"
#TENANN_SOURCE="tenann-v0.5.0-RELEASE"
#TENANN_MD5SUM="b8683b3df546944b60bc8fe3db8db3af"

# starcache
STARCACHE_DOWNLOAD="https://cdn-thirdparty.starrocks.com/starcache/v4.1-rc1/starcache-centos7_arm64.tar.gz"
STARCACHE_NAME="starcache.tar.gz"
STARCACHE_SOURCE="starcache"
STARCACHE_MD5SUM="365926dab3ef68d1785f434fab2d7728"

# pprof
PPROF_DOWNLOAD="https://github.com/murphyatwork/pprof/releases/download/20251124/pprof-linux-arm64.zip"
PPROF_NAME="pprof.zip"
PPROF_SOURCE="pprof"
PPROF_MD5SUM="1be75288c41a703f213b7dc98a59e4aa"