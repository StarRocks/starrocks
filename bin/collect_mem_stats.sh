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

source $STARROCKS_HOME/bin/common.sh

export LOG_DIR="$STARROCKS_HOME/log"
export PID_DIR=`cd "$curdir"; pwd`
pid=`cat $PID_DIR/fe.pid`

export_env_from_conf $STARROCKS_HOME/conf/fe.conf

ts=$(date +%Y%m%d-%H%M%S)
$JAVA_HOME/bin/jmap -histo $pid > $LOG_DIR/mem-stats-jmap-${ts}.txt

ts=$(date +%Y%m%d-%H%M%S)
${curdir}/profiler.sh -e alloc -d 300 -f $LOG_DIR/mem-stats-alloc-profile-${ts}.html $pid

