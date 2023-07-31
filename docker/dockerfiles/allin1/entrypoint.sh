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

# print banner
if [ -f $SR_HOME/../banner.txt ] ; then
    cat $SR_HOME/../banner.txt
fi

# setup log directories
mkdir -p $SR_HOME/{supervisor,fe,be,apache_hdfs_broker,feproxy}/log

update_feproxy_config
# use 127.0.0.1 for all the services, include fe/be/broker
setup_priority_networks

# setup supervisor and start
SUPERVISORD_HOME=$SR_HOME/supervisor
# allow supervisorctl to find the correct supervisord.conf
ln -sfT $SUPERVISORD_HOME/supervisord.conf /etc/supervisord.conf

cd $SUPERVISORD_HOME
exec supervisord -n -c $SUPERVISORD_HOME/supervisord.conf
