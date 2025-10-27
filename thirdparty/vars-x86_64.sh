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
HADOOP_DOWNLOAD="https://archive.apache.org/dist/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz"
HADOOP_NAME="hadoop-3.4.0.tar.gz"
HADOOP_SOURCE="hadoop-3.4.0"
HADOOP_MD5SUM="2f9244ab73169cf7bc0811d932067f6b"

# OPEN JDK
JDK_DOWNLOAD="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz"
JDK_NAME="OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz"
JDK_SOURCE="jdk-17.0.13+11"
JDK_MD5SUM="6876bb460fbcf6234778a62e1f3c4ae9"

# HYPERSCAN
HYPERSCAN_DOWNLOAD="https://github.com/intel/hyperscan/archive/v5.4.0.tar.gz"
HYPERSCAN_NAME="hyperscan-5.4.0.tar.gz"
HYPERSCAN_SOURCE="hyperscan-5.4.0"
HYPERSCAN_MD5SUM="65e08385038c24470a248f6ff2fa379b"

# async-profiler
ASYNC_PROFILER_DOWNLOAD="https://github.com/async-profiler/async-profiler/releases/download/v4.0/async-profiler-4.0-linux-x64.tar.gz"
ASYNC_PROFILER_NAME="async-profiler-4.0-linux-x64.tar.gz"
ASYNC_PROFILER_SOURCE="async-profiler-4.0-linux-x64"
ASYNC_PROFILER_MD5SUM="40257e6f4eb046427d8c59e67754fa62"

# jindosdk for Aliyun OSS
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux"
JINDOSDK_MD5SUM="5436e4fe39c4dfdc942e41821f1dd8a9"

# tenann
TENANN_DOWNLOAD="https://github.com/StarRocks/tenann/releases/download/v0.5.0-RELEASE/tenann-v0.5.0-RELEASE-x86_64.tar.gz"
TENANN_NAME="tenann-v0.5.0-RELEASE-x86_64.tar.gz"
TENANN_SOURCE="tenann-v0.5.0-RELEASE"
TENANN_MD5SUM="192ef22c00cf39315a91f54dab120855"

# starcache
STARCACHE_DOWNLOAD="https://cdn-thirdparty.starrocks.com/starcache/v3.5-rc4/starcache-centos7_amd64.tar.gz"
STARCACHE_NAME="starcache.tar.gz"
STARCACHE_SOURCE="starcache"
STARCACHE_MD5SUM="996aef75dbbf1b1f231bb95f4402fc59"
