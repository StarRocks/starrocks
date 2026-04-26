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

# jindosdk for Aliyun OSS
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux"
JINDOSDK_MD5SUM="5436e4fe39c4dfdc942e41821f1dd8a9"

# tenann
TENANN_DOWNLOAD="https://github.com/StarRocks/tenann/releases/download/v0.5.1-BETA/tenann-v0.5.1-BETA-x86_64.tar.gz"
TENANN_NAME="tenann-v0.5.1-BETA-x86_64.tar.gz"
TENANN_SOURCE="tenann-v0.5.1-BETA"
TENANN_MD5SUM="5b4ccc87389948c11ebb666a8700f7c4"

# starcache
STARCACHE_DOWNLOAD="https://cdn-thirdparty.starrocks.com/starcache/v4.1-rc3/starcache-centos7_amd64.tar.gz"
STARCACHE_NAME="starcache.tar.gz"
STARCACHE_SOURCE="starcache"
STARCACHE_MD5SUM="e9801128380baaf5956a75ac60009fe8"

# pprof
PPROF_DOWNLOAD="https://github.com/StarRocks/pprof/releases/download/release%2F20260421/pprof-linux-amd64"
PPROF_NAME="pprof"
PPROF_SOURCE="pprof"
PPROF_MD5SUM="9237ed8b48144c0170f5e4a5821f89f3"
