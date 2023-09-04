#!/bin/bash
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#####################################################
# Download url, filename and unpaced filename
# of all thirdparties
# 
# vars-${arch}.sh defines the thirdparties that are
# architecure-related.
#####################################################

# HADOOP
HADOOP_DOWNLOAD="https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
HADOOP_NAME="hadoop-3.3.6.tar.gz"
HADOOP_SOURCE="hadoop-3.3.6"
HADOOP_MD5SUM="1cbe1214299cd3bd282d33d3934b5cbd"

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
ASYNC_PROFILER_DOWNLOAD="https://github.com/async-profiler/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-x64.tar.gz"
ASYNC_PROFILER_NAME="async-profiler-2.9-linux-x64.tar.gz"
ASYNC_PROFILER_SOURCE="async-profiler-2.9-linux-x64"
ASYNC_PROFILER_MD5SUM="29127cee36b7acf069d31603b4558361"
