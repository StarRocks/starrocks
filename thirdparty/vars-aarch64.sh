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

# HYPERSCAN for aarch64, provided by vectorscan.
HYPERSCAN_DOWNLOAD="https://github.com/VectorCamp/vectorscan/archive/refs/tags/vectorscan/5.4.12.tar.gz"
HYPERSCAN_NAME="vectorscan-5.4.12.tar.gz"
HYPERSCAN_SOURCE="vectorscan-vectorscan-5.4.12"
HYPERSCAN_MD5SUM="384eab5b23831993df96e5fa55f9951e"

# jindosdk for Aliyun OSS
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux-el7-aarch64"
JINDOSDK_MD5SUM="27a4e2cd9a403c6e21079a866287d88b"

# tenann
TENANN_DOWNLOAD="https://github.com/StarRocks/tenann/releases/download/v0.5.1-rc2/tenann-v0.5.1-rc2-nosve-arm64.tar.gz"
TENANN_NAME="tenann-v0.5.1-rc2-nosve-arm64.tar.gz"
TENANN_SOURCE="tenann-v0.5.1-rc2-nosve"
TENANN_MD5SUM="6f3b7f3c8144f855edfd8a9abf04f82e"
# uncomment this for SVE version for better performance on ARM servers with SVE support
#TENANN_DOWNLOAD="https://github.com/StarRocks/tenann/releases/download/v0.5.1-rc2/tenann-v0.5.1-rc2-arm64.tar.gz"
#TENANN_NAME="tenann-v0.5.1-rc2-arm64.tar.gz"
#TENANN_SOURCE="tenann-v0.5.1-rc2"
#TENANN_MD5SUM="8b056305af24dab45ad21f6a037163b8"

# starcache
STARCACHE_DOWNLOAD="https://cdn-thirdparty.starrocks.com/starcache/v4.2-rc2/starcache-centos7_arm64.tar.gz"
STARCACHE_NAME="starcache.tar.gz"
STARCACHE_SOURCE="starcache"
STARCACHE_MD5SUM="3cfef8be7a06a71108174599eac6a9c9"

# pprof
PPROF_DOWNLOAD="https://github.com/StarRocks/pprof/releases/download/release%2F20260814/pprof-linux-arm64"
PPROF_NAME="pprof"
PPROF_SOURCE="pprof"
PPROF_MD5SUM="06ec565a9f6b417adfdf70f885214b9e"
