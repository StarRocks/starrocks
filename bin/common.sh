#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# get jdk version, return version as an Integer.
# 1.8 => 8, 13.0 => 13
jdk_version() {
    local result
    local java_cmd=$JAVA_HOME/bin/java
    local IFS=$'\n'
    # remove \r for Cygwin
    local lines=$("$java_cmd" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n')
    if [[ -z $java_cmd ]]
    then
        result=no_java
    else
        for line in $lines; do
            if [[ (-z $result) && ($line = *"version \""*) ]]
            then
                local ver=$(echo $line | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
                # on macOS, sed doesn't support '?'
                if [[ $ver = "1."* ]]
                then
                    result=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
                else
                    result=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
                fi
            fi
        done
    fi
    echo "$result"
}

jvm_arch() {
    march=`uname -m`
    case $march in
      aarch64 | arm64)
        jvm_arch=aarch64
        ;;
      *)
        jvm_arch=amd64
        ;;
    esac
    echo $jvm_arch
}

read_var_from_conf() {
    local var_name=$1
    local conf_file=$2
    local var_line=`grep $var_name $conf_file | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | grep ^$var_name=`
    if [[ $var_line == *"="* ]]; then
        eval "$var_line"
    fi
}

export_env_from_conf() {
    while read line; do
        envline=`echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`
        envline=`eval "echo $envline"`
        if [[ $envline == *"="* ]]; then
            eval 'export "$envline"'
        fi
    done < $1
}

export_shared_envvars() {
    # compatible with DORIS_HOME: DORIS_HOME still be using in config on the user side, so set DORIS_HOME to the meaningful value in case of wrong envs.
    export DORIS_HOME="$STARROCKS_HOME"

    # ===================================================================================
    # initialization of environment variables before exporting env variables from be.conf
    # For most cases, you should put default environment variables in this section.
    #
    # UDF_RUNTIME_DIR
    # LOG_DIR
    # PID_DIR
    export UDF_RUNTIME_DIR=${STARROCKS_HOME}/lib/udf-runtime
    export LOG_DIR=${STARROCKS_HOME}/log
    export PID_DIR=`cd "$curdir"; pwd`

    # https://github.com/aws/aws-cli/issues/5623
    # https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html
    export AWS_EC2_METADATA_DISABLED=${AWS_EC2_METADATA_DISABLED:-false}
    # ===================================================================================
}

update_submodules()
{
    pushd ${STARROCKS_HOME} &>/dev/null
    git submodule update --init --recursive
    popd
}

# If $FE_ENABLE_AUTO_JVM_XMX_DETECT=true, auto detect the memory limit MEM_LIMIT of the container
# - get $FE_JVM_XMX_PERCENTAGE, if not set, assume it is 70.
# - the -Xmx value will be $MEM_LIMIT * $FE_JVM_XMX_PERCENTAGE / 100 / 1024 / 1024
# - output string, e.g. -Xmx4096m
#
# NOTE: Make sure the implementation is equivalent to
# https://github.com/StarRocks/starrocks/blob/main/be/src/util/mem_info.cpp#L116
detect_jvm_xmx() {
    if [[ "$FE_ENABLE_AUTO_JVM_XMX_DETECT" != "true" ]]; then
        return
    fi

    # check if application is in docker container or not via /.dockerenv file
    if [[ ! -f "/.dockerenv" ]]; then
        return
    fi

    mem_limit_procfile=
    # https://kubernetes.io/docs/concepts/architecture/cgroups/#check-cgroup-version
    fstype=$(stat -fc %T /sys/fs/cgroup)
    case $fstype in
      tmpfs)
        mem_limit_procfile=/sys/fs/cgroup/memory/memory.limit_in_bytes
        ;;
      cgroup2fs)
        mem_limit_procfile=/sys/fs/cgroup/memory.max
        if ! test -e $mem_limit_procfile ; then
            mem_limit_procfile=/sys/fs/cgroup/kubepods/memory.max
        fi
        ;;
      *)
        return
        ;;
    esac

    # if resource limit is not set, $MEM_LIMIT will be "max"
    MEM_LIMIT=max
    if ! test -e $mem_limit_procfile ; then
        return
    else
        MEM_LIMIT=$(cat $mem_limit_procfile)
    fi
    if [[ ! "$MEM_LIMIT" =~ ^[0-9]+$ ]]; then
        return
    fi

    if [[ -z "$FE_JVM_XMX_PERCENTAGE" ]]; then
        FE_JVM_XMX_PERCENTAGE=70
    fi

    # shellcheck disable=SC1102
    let "MX = MEM_LIMIT * FE_JVM_XMX_PERCENTAGE / 100 / 1024 / 1024"
    echo "-Xmx${MX}m"
}

check_and_update_max_processes() {
    if [ $(ulimit -u) != "unlimited" ] && [ $(ulimit -u) -lt 65535 ]; then
        ulimit -u 65535
        if [ $? -ne 0 ]; then
            echo "Warn: update max user processes failed, please refer to https://docs.starrocks.io/docs/deployment/environment_configurations/#max-user-processes"
        fi
    fi
}

