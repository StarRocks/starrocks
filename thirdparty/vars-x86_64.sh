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
JDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/java-se-8u41-ri.tar.gz"
JDK_NAME="java-se-8u41-ri.tar.gz"
JDK_SOURCE="java-se-8u41-ri"
JDK_MD5SUM="7295b5a3fb90e7aaf80df23d5eac222d"

# HYPERSCAN
HYPERSCAN_DOWNLOAD="https://github.com/intel/hyperscan/archive/v5.4.0.tar.gz"
HYPERSCAN_NAME="hyperscan-5.4.0.tar.gz"
HYPERSCAN_SOURCE="hyperscan-5.4.0"
HYPERSCAN_MD5SUM="65e08385038c24470a248f6ff2fa379b"

# async-profiler
ASYNC_PROFILER_DOWNLOAD="https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz"
ASYNC_PROFILER_NAME="async-profiler-3.0-linux-x64.tar.gz"
ASYNC_PROFILER_SOURCE="async-profiler-3.0-linux-x64"
ASYNC_PROFILER_MD5SUM="618ef8c256103d3170cf2cddc4fe3fe2"

# jindosdk for Aliyun OSS
JINDOSDK_DOWNLOAD="https://cdn-thirdparty.starrocks.com/jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_NAME="jindosdk-4.6.8-linux.tar.gz"
JINDOSDK_SOURCE="jindosdk-4.6.8-linux"
JINDOSDK_MD5SUM="5436e4fe39c4dfdc942e41821f1dd8a9"

# starcache
STARCACHE_DOWNLOAD="https://cdn-thirdparty.starrocks.com/starcache/v3.4.0-rc02/starcache-centos7_amd64.tar.gz"
STARCACHE_NAME="starcache.tar.gz"
STARCACHE_SOURCE="starcache"
STARCACHE_MD5SUM="babf9fe6091663480db16129a146e2d8"
