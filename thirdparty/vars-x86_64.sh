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
HADOOP_DOWNLOAD="https://cdn-thirdparty.starrocks.com/hadoop-3.3.0.tar.gz"
HADOOP_NAME="hadoop-3.3.0.tar.gz"
HADOOP_SOURCE="hadoop-3.3.0"
HADOOP_MD5SUM="46c5004739df0c452610d5946eb8ce28"

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
