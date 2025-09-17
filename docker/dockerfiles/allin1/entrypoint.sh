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

# Function to detect CPU cores with Docker cgroup support
detect_cpu_cores()
{
    # Initialize variables
    local cpu_cores=""
    local cgroup_detected=false
    
    # Method 1: Check Docker cgroup v2 CPU limit
    if [ -f /sys/fs/cgroup/cpu.max ]; then
        local cpu_quota=$(cat /sys/fs/cgroup/cpu.max | cut -d' ' -f1)
        loginfo "cgroup v2 cpu.max: $cpu_quota"
        if [ "$cpu_quota" != "max" ] && [ -n "$cpu_quota" ] && [ "$cpu_quota" != "" ]; then
            cpu_cores=$((cpu_quota / 100000))
            cgroup_detected=true
            loginfo "Detected CPU cores from cgroup v2: $cpu_cores"
        fi
    # Method 2: Check Docker cgroup v1 CPU limit
    elif [ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us ] && [ -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]; then
        local cpu_quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
        local cpu_period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
        loginfo "cgroup v1 cpu.cfs_quota_us: $cpu_quota, cpu.cfs_period_us: $cpu_period"
        if [ "$cpu_quota" != "-1" ] && [ -n "$cpu_quota" ] && [ -n "$cpu_period" ] && [ "$cpu_quota" != "" ] && [ "$cpu_period" != "" ]; then
            cpu_cores=$((cpu_quota / cpu_period))
            cgroup_detected=true
            loginfo "Detected CPU cores from cgroup v1: $cpu_cores"
        fi
    fi
    
    # Method 3: If cgroup detection failed, try nproc (but this might show host CPU count)
    if [ "$cgroup_detected" = "false" ] || [ -z "$cpu_cores" ] || [ "$cpu_cores" = "0" ]; then
        if command -v nproc >/dev/null 2>&1; then
            cpu_cores=$(nproc)
            loginfo "Using nproc fallback: $cpu_cores cores (may be host CPU count)"
        elif [ -f /proc/cpuinfo ]; then
            cpu_cores=$(grep -c ^processor /proc/cpuinfo)
            loginfo "Using /proc/cpuinfo fallback: $cpu_cores cores (may be host CPU count)"
        else
            cpu_cores=8
            loginfo "Using default fallback: $cpu_cores cores"
        fi
    fi
    
    # Ensure minimum value and valid integer
    if [ -z "$cpu_cores" ] || [ "$cpu_cores" -lt 1 ] 2>/dev/null; then
        cpu_cores=1
        loginfo "Adjusted CPU cores to minimum value: $cpu_cores"
    fi
    
    # Return the detected CPU cores
    echo "$cpu_cores"
}

# Function to setup CPU-based configuration
setup_cpu_based_config()
{
    # Ensure fe.conf directory exists
    mkdir -p $SR_HOME/fe/conf
    
    # Detect CPU cores
    local cpu_cores=$(detect_cpu_cores)
    
    # Calculate bucket number (CPU cores * 2)
    local bucket_num=$((cpu_cores * 2))
    
    loginfo "Final CPU cores: $cpu_cores, setting default_unpartitioned_table_bucket_num to $bucket_num"
    echo "default_unpartitioned_table_bucket_num = $bucket_num" >> $SR_HOME/fe/conf/fe.conf
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
