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

curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

export STARROCKS_HOME=`cd "$curdir/.."; pwd`
# compatible with DORIS_HOME: DORIS_HOME still be using in config on the user side, so set DORIS_HOME to the meaningful value in case of wrong envs.
export DORIS_HOME="$STARROCKS_HOME"
export PID_DIR=`cd "$curdir"; pwd`

source $STARROCKS_HOME/bin/common.sh

export_env_from_conf $STARROCKS_HOME/conf/be.conf

pidfile=$PID_DIR/be.pid

sig=9

if [ -f $pidfile ]; then
    pid=`cat $pidfile`
    pidcomm=`ps -p $pid -o comm=`
    if [ "starrocks_be"x != "$pidcomm"x ]; then
        echo "ERROR: pid process may not be be. "
        exit 1
    fi

    if kill -0 $pid >/dev/null 2>&1; then
        kill -${sig} $pid > /dev/null 2>&1
        if [ $? -ne 0 ]; then
            exit 1
        fi
        while kill -0 $pid >/dev/null 2>&1; do
            sleep 1
        done
    fi
    rm $pidfile
fi
