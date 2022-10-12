#!/usr/bin/env bash
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`
export STARROCKS_HOME=`cd "$curdir/.."; pwd`

source $STARROCKS_HOME/bin/common.sh

if [ "$JAVA_HOME" = "" ]; then
    export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/jvm/amd64/server:$STARROCKS_HOME/lib/jvm/amd64
else
    java_version=$(jdk_version)
    if [[ $java_version -gt 8 ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$JAVA_HOME/lib
    else
        export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64
    fi
fi

${STARROCKS_HOME}/lib/starrocks_be --version
