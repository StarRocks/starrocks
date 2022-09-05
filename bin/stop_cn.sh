#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#############################################################################
# This script is used to stop CN process
# Usage:
#     sh stop_cn.sh [option]
#
# Options:
#     -h, --help              display this usage only
#     -g, --graceful          send SIGTERM to CN process instead of SIGKILL
#
#############################################################################

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

export STARROCKS_HOME=`cd "$curdir/.."; pwd`
# compatible with DORIS_HOME: DORIS_HOME still be using in config on the user side, so set DORIS_HOME to the meaningful value in case of wrong envs.
export DORIS_HOME="$STARROCKS_HOME"
export PID_DIR=`cd "$curdir"; pwd`

source $STARROCKS_HOME/bin/common.sh

export_env_from_conf $STARROCKS_HOME/conf/cn.conf

pidfile=$PID_DIR/cn.pid

sig=9

usage() {
    echo "
This script is used to stop CN process
Usage:
    sh stop_cn.sh [option]

Options:
    -h, --help              display this usage only
    -g, --graceful          send SIGTERM to CN process instead of SIGKILL
"
    exit 0
}

for arg in "$@"
do
    case $arg in
        --help|-h)
            usage
        ;;
        --graceful|-g)
            sig=15
        ;;
    esac
done

if [ -f $pidfile ]; then
    pid=`cat $pidfile`
    pidcomm=`ps -p $pid -o comm=`
    if [ "starrocks_be"x != "$pidcomm"x ]; then
        echo "ERROR: pid process may not be cn. "
        exit 1
    fi

    if kill -0 $pid; then
        if kill -${sig} $pid > /dev/null 2>&1; then
            echo "stop $pidcomm using signal $sig, and remove pid file. "
            rm $pidfile
            exit 0
        else
            exit 1
        fi
    else
        echo "Backend already exit, remove pid file. "
        rm $pidfile
    fi
else
    echo "$pidfile does not exist"
    exit 1
fi
