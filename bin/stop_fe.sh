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

export_env_from_conf $STARROCKS_HOME/conf/fe.conf

pidfile=$PID_DIR/fe.pid

if [ -f $pidfile ]; then
   pid=`cat $pidfile`
   pidcomm=`ps -p $pid -o comm=`
   
   if [ "java" != "$pidcomm" ]; then
       echo "ERROR: pid process may not be fe. "
   fi

   if kill $pid > /dev/null 2>&1; then
        while true
        do
            # check if fe proc is still alive
            if ps -p $pid > /dev/null; then
                echo "waiting fe to stop, pid: $pid"
                sleep 2
            else
                echo "stop $pidcomm, and remove pid file. "
                # This file will also be deleted within the process
                if [ -f $pidfile ]; then
                    rm $pidfile
                fi
                break
            fi
        done
   fi
fi

