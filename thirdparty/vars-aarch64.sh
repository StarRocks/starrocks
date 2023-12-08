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
HADOOP_DOWNLOAD="https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6-aarch64.tar.gz"
HADOOP_NAME="hadoop-3.3.6-aarch64.tar.gz"
HADOOP_SOURCE="hadoop-3.3.6"
HADOOP_MD5SUM="369f899194a920e0d1c3c3bc1718b3b5"

# OPEN JDK FOR aarch64, provided by huawei kunpeng (https://www.hikunpeng.com/zh/developer/devkit/compiler)
JDK_DOWNLOAD="https://mirror.iscas.ac.cn/kunpeng/archive/compiler/bisheng_jdk/bisheng-jdk-8u262-linux-aarch64.tar.gz"
JDK_NAME="bisheng-jdk-8u262-linux-aarch64.tar.gz"
JDK_SOURCE="bisheng-jdk1.8.0_262"
JDK_MD5SUM="a1254dea3728e0a86e53a55d8debfbeb"

# HYPERSCAN for aarch64, provided by huawei kunpeng.
HYPERSCAN_DOWNLOAD="https://github.com/kunpengcompute/hyperscan/archive/refs/tags/v5.3.0.aarch64.tar.gz"
HYPERSCAN_NAME="hyperscan-5.3.0.aarch64.tar.gz"
HYPERSCAN_SOURCE="hyperscan-5.3.0.aarch64"
HYPERSCAN_MD5SUM="ef337257bde6583242a739fab6fb161f"

# async-profiler
ASYNC_PROFILER_DOWNLOAD="https://github.com/async-profiler/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-arm64.tar.gz"
ASYNC_PROFILER_NAME="async-profiler-2.9-linux-arm64.tar.gz"
ASYNC_PROFILER_SOURCE="async-profiler-2.9-linux-arm64"
ASYNC_PROFILER_MD5SUM="d31a70d2c176146a46dffc15948040ed"

# jindosdk for Aliyun OSS
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux-el7-aarch64.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux-el7-aarch64"
JINDOSDK_MD5SUM="27a4e2cd9a403c6e21079a866287d88b"

# starcache
STARCACHE_DOWNLOAD="https://cdn-thirdparty.starrocks.com/starcache/v3.1.0/starcache-centos7_arm64.tar.gz"
STARCACHE_NAME="starcache.tar.gz"
STARCACHE_SOURCE="starcache"
STARCACHE_MD5SUM="35c9f9222ab4d995f1db40ff8555d041"
