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
  -- "$@")

eval set -- "$OPTS"

RUN_DAEMON=0
HELPER=
HOST_TYPE=
while true; do
    case "$1" in
        --daemon) RUN_DAEMON=1 ; shift ;;
        --helper) HELPER=$2 ; shift 2 ;;
        --host_type) HOST_TYPE=$2 ; shift 2 ;;
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
export JAVA_OPTS="-Xmx8g"
export LOG_DIR="$STARROCKS_HOME/log"
export PID_DIR=`cd "$curdir"; pwd`

export_env_from_conf $STARROCKS_HOME/conf/fe.conf

if [ -e $STARROCKS_HOME/conf/hadoop_env.sh ]; then
    source $STARROCKS_HOME/conf/hadoop_env.sh
fi

# java
if [[ -z ${JAVA_HOME} ]]; then
    if command -v javac &> /dev/null; then
        export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which javac))))"
        echo "Infered JAVA_HOME=$JAVA_HOME"
    else
      cat << EOF
Error: The environment variable JAVA_HOME is not set. The FE program requires JDK version 8 or higher in order to run.
Please take the following steps to resolve this issue:
1. Install OpenJDK 8 or higher using your Linux distribution's package manager.
For example:
sudo apt install openjdk-8-jdk  (on Ubuntu/Debian)
sudo yum install java-1.8.0-openjdk (on CentOS/RHEL)
2. Set the JAVA_HOME environment variable to point to your installed OpenJDK directory.
For example:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
3. Try running this script again.
EOF
      exit 1
    fi
fi

# cannot be jre
if [ ! -f "$JAVA_HOME/bin/javac" ]; then
  cat << EOF
Error: It appears that your JAVA_HOME environment variable is pointing to a non-JDK path: $JAVA_HOME
The FE program requires the full JDK to be installed and configured properly. Please check that JAVA_HOME
is set to the installation directory of JDK 8 or higher, rather than the JRE installation directory.
EOF
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
