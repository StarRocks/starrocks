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
  -l 'cluster_snapshot' \
  -l 'debug' \
  -l 'logconsole' \
  -l 'failpoint' \
  -- "$@")

eval set -- "$OPTS"

RUN_DAEMON=0
HELPER=
HOST_TYPE=
CLUSTER_SNAPSHOT=
ENABLE_DEBUGGER=0
FAILPOINT=
RUN_LOG_CONSOLE=${SYS_LOG_TO_CONSOLE:-0}
# min jdk version required
MIN_JDK_VERSION=17
while true; do
    case "$1" in
        --daemon) RUN_DAEMON=1 ; shift ;;
        --helper) HELPER=$2 ; shift 2 ;;
        --host_type) HOST_TYPE=$2 ; shift 2 ;;
        --cluster_snapshot) CLUSTER_SNAPSHOT="--cluster_snapshot" ; shift ;;
        --debug) ENABLE_DEBUGGER=1 ; shift ;;
        --logconsole) RUN_LOG_CONSOLE=1 ; shift ;;
        --failpoint) FAILPOINT="--failpoint" ; shift ;;
        --) shift ;  break ;;
        *) echo "Internal error" ; exit 1 ;;
    esac
done

export STARROCKS_HOME=`cd "$curdir/.."; pwd`

source $STARROCKS_HOME/bin/common.sh

check_and_update_max_processes

# export env variables from fe.conf
#
# JAVA_OPTS
# LOG_DIR
# PID_DIR
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
    elif command -v java &> /dev/null; then
        export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"
    else
      cat << EOF
Error: The environment variable JAVA_HOME is not set, and neither JDK or JRE is found.
The FE program requires JDK/JRE version $MIN_JDK_VERSION  or higher in order to run.
Please take the following steps to resolve this issue:
1. Install OpenJDK $MIN_JDK_VERSION or higher using your Linux distribution's package manager,
   or following the openjdk installation instructions at https://openjdk.org/install/
2. Set the JAVA_HOME environment variable to point to your installed OpenJDK directory.
   For example:
   export JAVA_HOME=/usr/lib/jvm/java-$MIN_JDK_VERSION
3. Try running this script again.
Note: If you are using a JRE environment, you should set your JAVA_HOME to your JRE directory.
For full development tools, JDK is recommended.
EOF
      exit 1
    fi
fi

JAVA=$JAVA_HOME/bin/java

# check java version and choose correct JAVA_OPTS
JAVA_VERSION=$(jdk_version)
if [[ "$JAVA_VERSION" -lt $MIN_JDK_VERSION ]]; then
    echo "Error: JDK $JAVA_VERSION is not supported, please use JDK version $MIN_JDK_VERSION or higher"
    exit -1
fi

final_java_opt=${JAVA_OPTS}
# Compatible with scenarios upgraded from jdk11
if [ ! -z "${JAVA_OPTS_FOR_JDK_11}" ] ; then
    echo "Warning: Configuration parameter JAVA_OPTS_FOR_JDK_11 is not supported, JAVA_OPTS is the only place to set jvm parameters"
    final_java_opt=${JAVA_OPTS_FOR_JDK_11}
fi

if [ -z "$final_java_opt" ] ; then
    # lookup fails, provide a fixed opts with best guess that may or may not work
    if [ -z "$DATE" ] ; then
        DATE=`date +%Y%m%d-%H%M%S`
    fi
    final_java_opt="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy"
    echo "JAVA_OPTS is not set in fe.conf, use default java options to start fe process: $final_java_opt"
fi

# Auto detect jvm -Xmx parameter in case $FE_ENABLE_AUTO_JVM_XMX_DETECT = true
#  default to 70% of total available mem and can be tuned by env var: FE_JVM_XMX_PERCENTAGE
# NOTE: the feature is only supported in container env
xmx=$(detect_jvm_xmx)
final_java_opt="${final_java_opt} ${xmx}"

if [ ${ENABLE_DEBUGGER} -eq 1 ]; then
    # Allow attaching debuggers to the FE process:
    # https://www.jetbrains.com/help/idea/attaching-to-local-process.html
    final_java_opt="${final_java_opt} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
    echo "Start debugger with: $final_java_opt"
fi

# add datadog profile settings when enabled
if [ "${ENABLE_DATADOG_PROFILE}" == "true" ] && [ -f "${STARROCKS_HOME}/datadog/dd-java-agent.jar" ]; then
    final_java_opt="-javaagent:${STARROCKS_HOME}/datadog/dd-java-agent.jar ${final_java_opt}"
fi

# add failpoint config file when enabled
if [ x"$FAILPOINT" != x"" ]; then
    failpoint_lib="${STARROCKS_HOME}/lib/byteman-4.0.24.jar"
    failpoint_conf="${STARROCKS_HOME}/conf/failpoint.btm"
    if [ ! -f ${failpoint_lib} ]; then
        echo "failpoint lib does not exist: ${failpoint_lib}"
        exit 1
    fi
    if [ ! -f ${failpoint_conf} ]; then
        echo "failpoint config file does not exist: ${failpoint_conf}"
        exit 1
    fi

    final_java_opt="${final_java_opt} -javaagent:${failpoint_lib}=script:${failpoint_conf}"
fi

if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
fi

read_var_from_conf meta_dir $STARROCKS_HOME/conf/fe.conf
mkdir -p ${meta_dir:-"$STARROCKS_HOME/meta"}

# add libs to CLASSPATH
for f in $STARROCKS_HOME/lib/*.jar; do
  CLASSPATH=$f:${CLASSPATH};
done
# Explicitly put fe-core-main.jar at the front of classpath to ensure StarRocks' 
# customized classes (e.g., HiveMetaStoreClient) are loaded before the original 
# ones from third-party JARs (e.g., hive-apache-3.1.2-22.jar).
# This prevents ClassLoader conflicts where the original HiveMetaStoreClient 
# (which uses old thrift paths like org.apache.thrift.transport.TFramedTransport)
# might be loaded instead of StarRocks' version (which uses the correct new paths
# like org.apache.thrift.transport.layered.TFramedTransport).
export CLASSPATH=${STARROCKS_HOME}/lib/fe-core-main.jar:${STARROCKS_HOME}/lib/starrocks-hadoop-ext.jar:${CLASSPATH}:${STARROCKS_HOME}/lib:${STARROCKS_HOME}/conf

pidfile=$PID_DIR/fe.pid

if [ -f $pidfile ]; then
  oldpid=$(cat $pidfile)
  # get the full command
  pscmd=$(ps -q $oldpid -o cmd=)
  if echo "$pscmd" | grep -q -w StarRocksFE &>/dev/null ; then
    echo Frontend running as process $oldpid. Stop it first.
    exit 1
  fi
fi

if [ ! -f /bin/limit ]; then
    LIMIT=
else
    LIMIT=/bin/limit
fi

if [ x"$HELPER" != x"" ]; then
    HELPER="--helper $HELPER"
fi

if [ x"$HOST_TYPE" != x"" ]; then
    HOST_TYPE="--host_type $HOST_TYPE"
fi


LOG_FILE=$LOG_DIR/fe.out

if [ ${RUN_LOG_CONSOLE} -eq 1 ] ; then
    if [ ! -w $STARROCKS_HOME/conf/fe.conf ] ; then
        # workaround configmap readonly, can't change its content
        mv $STARROCKS_HOME/conf/fe.conf $STARROCKS_HOME/conf/fe.conf.readonly
        cp $STARROCKS_HOME/conf/fe.conf.readonly $STARROCKS_HOME/conf/fe.conf
    fi
else
    # redirect all subsequent commands' stdout/stderr into $LOG_FILE
    exec >> $LOG_FILE 2>&1
fi
export SYS_LOG_TO_CONSOLE=${RUN_LOG_CONSOLE}

echo "using java version $JAVA_VERSION"
echo $final_java_opt
echo "start time: $(date), server uptime: $(uptime)"

# StarRocksFE java process will write its process id into $pidfile
if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup $LIMIT $JAVA $final_java_opt com.starrocks.StarRocksFE ${HELPER} ${HOST_TYPE} ${CLUSTER_SNAPSHOT} ${FAILPOINT} "$@" </dev/null &
else
    exec $LIMIT $JAVA $final_java_opt com.starrocks.StarRocksFE ${HELPER} ${HOST_TYPE} ${CLUSTER_SNAPSHOT} ${FAILPOINT} "$@" </dev/null
fi
