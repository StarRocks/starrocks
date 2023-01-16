#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
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
<<<<<<< HEAD
# compatible with DORIS_HOME: DORIS_HOME still be using in config on the user side, so set DORIS_HOME to the meaningful value in case of wrong envs.
export DORIS_HOME="$STARROCKS_HOME"
source $STARROCKS_HOME/bin/common.sh

# export env variables from cn.conf
#
# UDF_RUNTIME_DIR
# LOG_DIR
# PID_DIR
export UDF_RUNTIME_DIR=${STARROCKS_HOME}/lib/udf-runtime
export LOG_DIR=${STARROCKS_HOME}/log
export PID_DIR=`cd "$curdir"; pwd`

export_env_from_conf $STARROCKS_HOME/conf/cn.conf
export_mem_limit_from_conf $STARROCKS_HOME/conf/cn.conf

if [ $? -ne 0 ]; then
    exit 1
fi

if [ -e $STARROCKS_HOME/conf/hadoop_env.sh ]; then
    source $STARROCKS_HOME/conf/hadoop_env.sh
fi

# NOTE: JAVA_HOME must be configed if using hdfs scan, like hive external table
# this is only for starting cn
jvm_arch="amd64"
if [[ "${MACHINE_TYPE}" == "aarch64" ]]; then
    jvm_arch="aarch64"
fi

if [ "$JAVA_HOME" = "" ]; then
    export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/jvm/$jvm_arch/server:$STARROCKS_HOME/lib/jvm/$jvm_arch:$LD_LIBRARY_PATH
else
    java_version=$(jdk_version)
    if [[ $java_version -gt 8 ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/server:$JAVA_HOME/lib:$LD_LIBRARY_PATH
    # JAVA_HOME is jdk
    elif [[ -d "$JAVA_HOME/jre"  ]]; then
        export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/$jvm_arch/server:$JAVA_HOME/jre/lib/$jvm_arch:$LD_LIBRARY_PATH
    # JAVA_HOME is jre
    else
        export LD_LIBRARY_PATH=$JAVA_HOME/lib/$jvm_arch/server:$JAVA_HOME/lib/$jvm_arch:$LD_LIBRARY_PATH
    fi
fi

export LD_LIBRARY_PATH=$STARROCKS_HOME/lib/hadoop/native:$LD_LIBRARY_PATH

# HADOOP_CLASSPATH defined in $STARROCKS_HOME/conf/hadoop_env.sh
# put $STARROCKS_HOME/conf ahead of $HADOOP_CLASSPATH so that custom config can replace the config in $HADOOP_CLASSPATH
export CLASSPATH=$STARROCKS_HOME/conf:$HADOOP_CLASSPATH:$CLASSPATH
# https://github.com/aws/aws-cli/issues/5623
# https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
export AWS_EC2_METADATA_DISABLED=true

if [ ! -d $LOG_DIR ]; then
    mkdir -p $LOG_DIR
fi

if [ ! -d $UDF_RUNTIME_DIR ]; then
    mkdir -p ${UDF_RUNTIME_DIR}
fi

rm -f ${UDF_RUNTIME_DIR}/*

pidfile=$PID_DIR/cn.pid

if [ -f $pidfile ]; then
    if kill -0 $(cat $pidfile) > /dev/null 2>&1; then
        echo "Backend running as process `cat $pidfile`. Stop it first."
        exit 1
    else
        rm $pidfile
    fi
fi

chmod 755 ${STARROCKS_HOME}/lib/starrocks_be
echo "start time: "$(date) >> $LOG_DIR/cn.out

if [[ $(ulimit -n) -lt 60000 ]]; then
  ulimit -n 65535
fi

if [ ${RUN_DAEMON} -eq 1 ]; then
    nohup ${STARROCKS_HOME}/lib/starrocks_be --cn "$@" >> $LOG_DIR/cn.out 2>&1 </dev/null &
else
    exec ${STARROCKS_HOME}/lib/starrocks_be --cn "$@" >> $LOG_DIR/cn.out 2>&1 </dev/null
fi
=======
bash ${STARROCKS_HOME}/bin/start_backend.sh --cn $@
>>>>>>> dac4935e5 ([BugFix] Fix backend start script (#16559))
