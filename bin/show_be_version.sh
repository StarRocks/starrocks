#!/usr/bin/env bash
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`
export STARROCKS_HOME=`cd "$curdir/.."; pwd`

source $STARROCKS_HOME/bin/common.sh
jvmarch=`jvm_arch`

if [ "$JAVA_HOME" = "" ]; then
    export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/jvm/$jvmarch/server:$STARROCKS_HOME/lib/jvm/$jvmarch
else
    java_version=$(jdk_version)
    if [[ $java_version -gt 8 ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$JAVA_HOME/lib
    else
        export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$jvmarch/server:$JAVA_HOME/jre/lib/$jvmarch
    fi
fi

export_cachelib_lib_path

${STARROCKS_HOME}/lib/starrocks_be --version
