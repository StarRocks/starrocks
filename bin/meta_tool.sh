#!/usr/bin/env bash
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`
export STARROCKS_HOME=`cd "$curdir/.."; pwd`
source $STARROCKS_HOME/bin/common.sh
jvmarch=`jvm_arch`
export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/jvm/$jvmarch/server:$STARROCKS_HOME/lib/jvm/$jvmarch:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/hadoop/native:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$STARROCKS_HOME/lib/cachelib/lib64

${STARROCKS_HOME}/lib/starrocks_be meta_tool "$@"
