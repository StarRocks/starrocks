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

# HADOOP
HADOOP_DOWNLOAD="https://archive.apache.org/dist/hadoop/common/hadoop-3.4.0/hadoop-3.4.0-aarch64.tar.gz"
HADOOP_NAME="hadoop-3.4.0-aarch64.tar.gz"
HADOOP_SOURCE="hadoop-3.4.0"
HADOOP_MD5SUM="4cf40e127f27044310aae36dce23bdb1"

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

# async-profiler
ASYNC_PROFILER_DOWNLOAD="https://github.com/async-profiler/async-profiler/releases/download/v4.0/async-profiler-4.0-linux-arm64.tar.gz"
ASYNC_PROFILER_NAME="async-profiler-4.0-linux-arm64.tar.gz"
ASYNC_PROFILER_SOURCE="async-profiler-4.0-linux-arm64"
ASYNC_PROFILER_MD5SUM="588a42962cf925932615e4ce5a9daf9e"

# jindosdk for Aliyun OSS
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux-el7-aarch64"
JINDOSDK_MD5SUM="27a4e2cd9a403c6e21079a866287d88b"

# starcache
STARCACHE_DOWNLOAD="https://cdn-thirdparty.starrocks.com/starcache/v3.5-rc4/starcache-centos7_arm64.tar.gz"
STARCACHE_NAME="starcache.tar.gz"
STARCACHE_SOURCE="starcache"
STARCACHE_MD5SUM="652306980eb0a99d23823e75de807749"
