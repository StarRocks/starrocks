#!/usr/bin/env bash
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

export_env_from_conf() {
    while read line; do
        envline=`echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`
        envline=`eval "echo $envline"`
        if [[ $envline == *"="* ]]; then
            eval 'export "$envline"'
        fi
    done < $1
}

# Read the config [mem_limit] from configuration file of BE and set the upper limit of TCMalloc memory allocation
# if mem_limit is not set, use 90% of machine memory
export_mem_limit_from_conf() {
    mem_limit_is_set=false;
    while read line; do
        envline=`echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^mem_limit=*"`
        if [[ $envline == *"="* ]]; then
            value=`echo $envline | sed 's/mem_limit=//g'`
            mem_limit_is_set=true
        fi
    done < $1

    cgroup_version=$(stat -fc %T /sys/fs/cgroup)
    if [ -f /.dockerenv ] && [ "$cgroup_version" == "tmpfs" ]; then
        mem_limit=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes | awk '{printf $1}')
        if [ "$mem_limit" == "" ]; then
            echo "can't get mem info from /sys/fs/cgroup/memory/memory.limit_in_bytes"
            return 1
        fi
        mem_limit=`expr $mem_limit / 1024`
    fi

    if [ -f /.dockerenv ] && [ "$cgroup_version" == "cgroup2fs" ]; then
        mem_limit=$(cat /sys/fs/cgroup/memory.max | awk '{printf $1}')
        if [ "$mem_limit" == "" ]; then
            echo "can't get mem info from /sys/fs/cgroup/memory.max"
            return 1
        fi
        if [ "$mem_limit" != "max" ]; then
            mem_limit=`expr $mem_limit / 1024`
        fi
    fi

    # read /proc/meminfo to fetch total memory of machine
    mem_total=$(cat /proc/meminfo |grep 'MemTotal' |awk -F : '{print $2}' |sed 's/^[ \t]*//g' | awk '{printf $1}')
    if [ "$mem_total" == "" ]; then
        echo "can't get mem info from /proc/meminfo"
        return 1
    fi

    if [[ (-v mem_limit) && ("$mem_limit" != "max") && ($mem_limit -le $mem_total) ]]; then
      mem_total=$mem_limit
    fi

    if [ "$mem_limit_is_set" == "false" ]; then
        # if not set, the mem limit if 90% of total memory
        mem=`expr $mem_total \* 90 / 100 / 1024`
    else
        final=${value: -1}
        case $final in
            t|T)
                value=`echo ${value%?}`
                mem=`expr $value \* 1024 \* 1024`
                ;;
            g|G)
                value=`echo ${value%?}`
                mem=`expr $value \* 1024`
                ;;
            m|M)
                value=`echo ${value%?}`
                mem=`expr $value`
                ;;
            k|K)
                value=`echo ${value%?}`
                mem=`expr $value / 1024`
                ;;
            b|B)
                value=`echo ${value%?}`
                mem=`expr $value / 1024 / 1024`
                ;;
            %)
                value=`echo ${value%?}`
                mem=`expr $mem_total \* $value / 100 / 1024`
                ;;
            *)
                mem=`expr $value / 1024 / 1024`
                ;;
            esac
    fi

    if [ $mem -le 0 ]; then
        echo "invalid mem limit: mem_limit<=0M"
        return 1
    elif [ $mem -gt `expr $mem_total / 1024` ]; then
        echo "mem_limit is larger then machine memory"
        return 1
    fi

    export TCMALLOC_HEAP_LIMIT_MB=${mem}
    return 0
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
    export AWS_EC2_METADATA_DISABLED=false
    # ===================================================================================
}

# Export cachelib libraries
export_cachelib_lib_path() {
    CACHELIB_DIR=$STARROCKS_HOME/lib/cachelib
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CACHELIB_DIR/lib64
}

# if $FE_ENABLE_AUTO_JVM_XMX_DETECT=true, auto detect the memory limit MEM_LIMIT of the container
# get $FE_JVM_XMX_PERCENTAGE, if not set, assume it is 70.
# the xmx value will be $MEM_LIMIT * $FE_JVM_XMX_PERCENTAGE / 100 / 1024 / 1024
# output string, e.g. -Xmx4096m
detect_jvm_xmx() {
    if [[ "$FE_ENABLE_AUTO_JVM_XMX_DETECT" != "true" ]]; then
        return
    fi

    # check if application is in docker container or not via /.dockerenv file
    if [[ ! -f "/.dockerenv" ]]; then
        return
    fi

    # if resource limit is not set, $MEM_LIMIT will be "max"
    MEM_LIMIT=max
    # get the cgroup version from /proc/filesystems
    # if cgroup v2 is enabled, the output will contain "cgroup2"
    if grep -q cgroup2 /proc/filesystems &>/dev/null; then
        MEM_LIMIT=$(cat /sys/fs/cgroup/memory.max)
    else
        MEM_LIMIT=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
    fi

    # check if $MEM_LIMIT is a number.
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
    if [ $(ulimit -u) -lt 65535 ]; then
        ulimit -u 65535
        if [ $? -ne 0 ]; then
            echo "Warn: update max user processes failed, please refer to https://docs.starrocks.io/docs/deployment/environment_configurations/#max-user-processes"
        fi
    fi
}

