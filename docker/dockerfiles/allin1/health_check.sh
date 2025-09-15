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

BOOTSTRAP_DONE_FILE=$SR_HOME/bootstrap_done
FE_DEFAULT_HTTP_PORT=8030
BE_DEFAULT_HTTP_PORT=8040

get_fe_http_port()
{
    export_env_from_conf $SR_HOME/fe/conf/fe.conf
    echo ${http_port:-$FE_DEFAULT_HTTP_PORT}
}

get_be_http_port()
{
    export_env_from_conf $SR_HOME/be/conf/be.conf
    echo ${be_http_port:-$BE_DEFAULT_HTTP_PORT}
}

test -f $BOOTSTRAP_DONE_FILE || exit 1

source $SR_HOME/fe/bin/common.sh
fe_http_port=`get_fe_http_port`
curl -f -s http://localhost:$fe_http_port/api/health

be_http_port=`get_be_http_port`
curl -f -s http://localhost:$be_http_port/api/health
