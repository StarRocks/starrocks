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

SIG=15
TIME_OUT=-1

OPTS=$(getopt \
  -n $0 \
  -o gh \
  -l 'graceful' \
  -l 'timeout:' \
  -l 'help' \
  -- "$@")

eval set -- "$OPTS"

usage() {
    echo "
This script is used to stop FE process
Usage:
    ./stop_fe.sh [option]

Options:
    -h, --help              display this usage only
    -g, --graceful          send USER1 to FE process instead of SIGTERM
    --timeout               specify the timeout for graceful exit
"
    exit 0
}

while true; do
    case "$1" in
        --timeout) TIME_OUT=$2 ; shift 2 ;;
        --help|-h) usage ; shift ;;
        --graceful|-g) SIG=10 ; shift ;;
        --) shift ;  break ;;
    esac
done

export STARROCKS_HOME=`cd "$curdir/.."; pwd`
export PID_DIR=`cd "$curdir"; pwd`

source $STARROCKS_HOME/bin/common.sh

export_env_from_conf $STARROCKS_HOME/conf/fe.conf

pidfile=$PID_DIR/fe.pid

if [ -f $pidfile ]; then
    pid=`cat $pidfile`

    # ============================================================
    # Validation phase: Verify process before any kill operation
    # All checks must pass before proceeding to kill
    # ============================================================

    # Step 1: Check if process exists (kill -0 only checks, doesn't kill)
    if ! kill -0 $pid > /dev/null 2>&1; then
        echo "Process $pid does not exist, removing stale pid file"
        rm $pidfile
        exit 0
    fi

    # Step 2: Get process information
    pidcomm=`ps -p $pid -o comm= 2>/dev/null`
    pidcmd=`ps -p $pid -o command= 2>/dev/null | head -1`

    # Step 3: Verify we can get process information
    if [ -z "$pidcomm" ] || [ -z "$pidcmd" ]; then
        echo "ERROR: Cannot get process information for pid $pid"
        exit 1
    fi

    # Step 4: Verify it's a Java process
    # Extract basename from command path (works for both "java" and full paths like "/path/to/java")
    comm_basename=$(basename "$pidcomm")
    if [ "$comm_basename" != "java" ]; then
        echo "ERROR: Process $pid is not a Java process (comm: $pidcomm)"
        exit 1
    fi

    # Step 5: Verify it's StarRocksFE process
    if ! echo "$pidcmd" | grep -q "StarRocksFE"; then
        echo "ERROR: Process $pid may not be FE (command does not contain StarRocksFE)"
        exit 1
    fi

    # ============================================================
    # All validations passed, now safe to kill the process
    # ============================================================

    kill -${SIG} $pid > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        rm $pidfile
        exit 1
    fi
    
    # Waiting for a process to exit
    start_ts=$(date +%s)
    while kill -0 $pid > /dev/null 2>&1; do
        if [ $TIME_OUT -gt 0 ] && [ $(($(date +%s) - $start_ts)) -gt $TIME_OUT ]; then
            kill -9 $pid
            echo "graceful exit timeout, forced termination of the process"
            break
        else
            sleep 1
        fi
    done

    if [ -f $pidfile ]; then
        rm $pidfile
    fi
fi

