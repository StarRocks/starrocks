#!/bin/bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

get_fe_http_port()
{
    source $SR_HOME/fe/bin/common.sh
    export_env_from_conf $SR_HOME/fe/conf/fe.conf
    echo ${http_port:-8030}
}

update_feproxy_config()
{
    # process fe http_port from a sub shell to avoid env var escalation
    fehttpport=`get_fe_http_port`
    cat $SR_HOME/feproxy/feproxy.conf.template | sed -e "s|{{feproxyhome}}|$SR_HOME/feproxy|g" -e "s|{{fewebport}}|${fehttpport}|g" > $SR_HOME/feproxy/feproxy.conf
}

setup_priority_networks()
{
    echo "priority_networks = 127.0.0.1/32" >> $SR_HOME/fe/conf/fe.conf
    echo "priority_networks = 127.0.0.1/32" >> $SR_HOME/be/conf/be.conf
}

loginfo()
{
    log_stderr "INFO $@"
}

log_stderr()
{
    echo "`date --rfc-3339=seconds` $@" >&2
}

logerror()
{
    log_stderr "ERROR $@"
}

# Function to detect CPU cores and set default_unpartitioned_table_bucket_num
setup_cpu_based_config()
{
    # Ensure fe.conf directory exists
    mkdir -p $SR_HOME/fe/conf
    
    # Detect CPU cores with Docker cgroup support
    CPU_CORES=8  # Default fallback value
    
    # Method 1: Check Docker cgroup v2 CPU limit
    if [ -f /sys/fs/cgroup/cpu.max ]; then
        CPU_QUOTA=$(cat /sys/fs/cgroup/cpu.max | cut -d' ' -f1)
        if [ "$CPU_QUOTA" != "max" ] && [ -n "$CPU_QUOTA" ]; then
            CPU_CORES=$(echo "$CPU_QUOTA / 100000" | bc -l | cut -d'.' -f1)
            loginfo "Detected CPU cores from cgroup v2: $CPU_CORES"
        fi
    # Method 2: Check Docker cgroup v1 CPU limit
    elif [ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us ] && [ -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]; then
        CPU_QUOTA=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
        CPU_PERIOD=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
        if [ "$CPU_QUOTA" != "-1" ] && [ -n "$CPU_QUOTA" ] && [ -n "$CPU_PERIOD" ]; then
            CPU_CORES=$(echo "$CPU_QUOTA / $CPU_PERIOD" | bc -l | cut -d'.' -f1)
            loginfo "Detected CPU cores from cgroup v1: $CPU_CORES"
        fi
    fi
    
    # Method 3: If cgroup detection failed, try nproc (but this might show host CPU count)
    if [ "$CPU_CORES" = "8" ] || [ "$CPU_CORES" = "0" ]; then
        if command -v nproc >/dev/null 2>&1; then
            CPU_CORES=$(nproc)
            loginfo "Using nproc fallback: $CPU_CORES cores (may be host CPU count)"
        elif [ -f /proc/cpuinfo ]; then
            CPU_CORES=$(grep -c ^processor /proc/cpuinfo)
            loginfo "Using /proc/cpuinfo fallback: $CPU_CORES cores (may be host CPU count)"
        else
            CPU_CORES=8
            loginfo "Using default fallback: $CPU_CORES cores"
        fi
    fi
    
    # Ensure minimum value
    if [ "$CPU_CORES" -lt 1 ]; then
        CPU_CORES=1
        loginfo "Adjusted CPU cores to minimum value: $CPU_CORES"
    fi
    
    # Calculate bucket number (CPU cores * 2)
    BUCKET_NUM=$((CPU_CORES * 2))
    
    loginfo "Final CPU cores: $CPU_CORES, setting default_unpartitioned_table_bucket_num to $BUCKET_NUM"
    
    # Append the configuration to fe.conf
    echo "default_unpartitioned_table_bucket_num = $BUCKET_NUM" >> $SR_HOME/fe/conf/fe.conf
    
    # Verify the configuration was written successfully
    if [ $? -eq 0 ]; then
        loginfo "Successfully configured default_unpartitioned_table_bucket_num = $BUCKET_NUM"
    else
        logerror "Failed to write default_unpartitioned_table_bucket_num configuration"
        return 1
    fi
}

update_fe_conf_if_run_in_shared_data_mode()
{
    if [ ! -f $SR_HOME/fe/meta/image/VERSION ]; then
        run_mode="${RUN_MODE,,}"
        if [ "$run_mode" == "shared_data" ]; then # if run mode is shared_data, we need to update fe.conf
            loginfo "envVar RUN_MODE is set to $RUN_MODE, running in shared_data mode ..."
            echo "# config for shared_data mode
run_mode = shared_data
cloud_native_meta_port = 6090
# Whether volume can be created from conf. If it is enabled, a builtin storage volume may be created.
enable_load_volume_from_conf = false" >> $SR_HOME/fe/conf/fe.conf
        else
            loginfo "running in shared_nothing mode ..."
        fi
    else
        # If $SR_HOME/fe/meta/image/VERSION file exists, it should have a line like "runMode=shared_nothing" or "runMode=shared_data"
        previous_run_mode=`cat $SR_HOME/fe/meta/image/VERSION | grep runMode | cut -d '=' -f 2`
        previous_run_mode=${previous_run_mode,,} # to lower case'
        if [ -z "$previous_run_mode" ]; then
          # this can happen in v2.5 release, where we don't have runMode in VERSION file
          previous_run_mode="shared_nothing"
        fi
        run_mode=${RUN_MODE,,} # to lower case

        if [ -n "$run_mode" ] && [ "$previous_run_mode" != "$run_mode" ]; then
            logerror "Error: runMode=$previous_run_mode in $SR_HOME/fe/meta/image/VERSION and environment RUN_MODE=$RUN_MODE must be the same"
            return 1
        fi
    fi
}

# print banner
if [ -f $SR_HOME/../banner.txt ] ; then
    cat $SR_HOME/../banner.txt
fi

# setup log directories
mkdir -p $SR_HOME/{supervisor,fe,be,feproxy}/log

update_feproxy_config
# use 127.0.0.1 for all the services, include fe/be
setup_priority_networks
setup_cpu_based_config
update_fe_conf_if_run_in_shared_data_mode

# setup supervisor and start
SUPERVISORD_HOME=$SR_HOME/supervisor
# allow supervisorctl to find the correct supervisord.conf
ln -sfT $SUPERVISORD_HOME/supervisord.conf /etc/supervisord.conf

cd $SUPERVISORD_HOME
exec supervisord -n -c $SUPERVISORD_HOME/supervisord.conf
