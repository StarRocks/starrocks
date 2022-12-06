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

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'daemon' \
  -l 'helper:' \
  -l 'host_type:' \
  -l 'debug' \
  -- "$@")

eval set -- "$OPTS"

RUN_DAEMON=0
HELPER=
HOST_TYPE=
ENABLE_DEBUGGER=0
while true; do
    case "$1" in
        --daemon) RUN_DAEMON=1 ; shift ;;
        --helper) HELPER=$2 ; shift 2 ;;
        --host_type) HOST_TYPE=$2 ; shift 2 ;;
        --debug) ENABLE_DEBUGGER=1 ; shift ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

export STARROCKS_HOME=`cd "$curdir/.."; pwd`

# compatible with DORIS_HOME: DORIS_HOME still be using in config on the user side, so set DORIS_HOME to the meaningful value in case of wrong envs.
export DORIS_HOME="$STARROCKS_HOME"

source $STARROCKS_HOME/bin/common.sh

# export env variables from fe.conf
#
# JAVA_OPTS
# LOG_DIR
# PID_DIR
export JAVA_OPTS="-Xmx1024m"
export LOG_DIR="$STARROCKS_HOME/log"
export PID_DIR=`cd "$curdir"; pwd`

export_env_from_conf $STARROCKS_HOME/conf/fe.conf

if [ -e $STARROCKS_HOME/conf/hadoop_env.sh ]; then
    source $STARROCKS_HOME/conf/hadoop_env.sh
fi

# java
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

# cannot be jre
if [ ! -f "$JAVA_HOME/bin/javac" ]; then
  echo "Error: JAVA_HOME can not be jre"
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
 
# check java version and choose correct JAVA_OPTS
JAVA_VERSION=$(jdk_version)
final_java_opt=$JAVA_OPTS
if [[ "$JAVA_VERSION" -gt 8 ]]; then
    if [ -z "$JAVA_OPTS_FOR_JDK_9" ]; then
        echo "JAVA_OPTS_FOR_JDK_9 is not set in fe.conf" >> $LOG_DIR/fe.out
        exit -1
    fi 
    final_java_opt=$JAVA_OPTS_FOR_JDK_9
fi

if [ ${ENABLE_DEBUGGER} -eq 1 ]; then
    # Allow attaching debuggers to the FE process:
    # https://www.jetbrains.com/help/idea/attaching-to-local-process.html
    if [[ "$JAVA_VERSION" -gt 8 ]]; then
        final_java_opt="${final_java_opt} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
    else
        final_java_opt="${final_java_opt} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
    fi
    echo "Start debugger with: $final_java_opt"
fi

if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
fi

echo "using java version $JAVA_VERSION" >> $LOG_DIR/fe.out
echo $final_java_opt >> $LOG_DIR/fe.out

# add libs to CLASSPATH
for f in $STARROCKS_HOME/lib/*.jar; do
  CLASSPATH=$f:${CLASSPATH};
done
export CLASSPATH=${CLASSPATH}:${STARROCKS_HOME}/lib:${STARROCKS_HOME}/conf

pidfile=$PID_DIR/fe.pid

if [ -f $pidfile ]; then
  if kill -0 `cat $pidfile` > /dev/null 2>&1; then
    echo Frontend running as process `cat $pidfile`.  Stop it first.
    exit 1
  fi
fi

if [ ! -f /bin/limit ]; then
    LIMIT=
else
    LIMIT=/bin/limit
fi

echo `date` >> $LOG_DIR/fe.out

if [ x"$HELPER" != x"" ]; then
    # change it to '-helper' to be compatible with code in Frontend
    HELPER="-helper $HELPER"
fi

if [ x"$HOST_TYPE" != x"" ]; then
    # change it to '-host_type' to be compatible with code in Frontend
    HOST_TYPE="-host_type $HOST_TYPE"
fi

if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup $LIMIT $JAVA $final_java_opt com.starrocks.StarRocksFE ${HELPER} ${HOST_TYPE} "$@" >> $LOG_DIR/fe.out 2>&1 </dev/null &
else
    $LIMIT $JAVA $final_java_opt com.starrocks.StarRocksFE ${HELPER} ${HOST_TYPE} "$@" >> $LOG_DIR/fe.out 2>&1 </dev/null
fi

echo $! > $pidfile
