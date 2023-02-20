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
HADOOP_DOWNLOAD="https://cdn-thirdparty.starrocks.com/hadoop-3.3.0-aarch64.tar.gz"
HADOOP_NAME="hadoop-3.3.0-aarch64.tar.gz"
HADOOP_SOURCE="hadoop-3.3.0-aarch64"
HADOOP_MD5SUM="c84cfa985175341d3df0b2b57521eec9"

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
