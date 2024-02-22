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

export COREDUMP_PATH=/opt/starrocks/be/storage/coredumps

if [ ! -d ${COREDUMP_PATH} ]; then
    mkdir -p ${COREDUMP_PATH}
fi


# Calculate physical memory size in bytes
physical_memory=$(free -b | awk '/^Mem:/{print $2}')

# Calculate the core dump size limit (3 times the physical memory size)
export core_dump_limit=$((3 * physical_memory))

# Set the core dump size limit for the current shell session
ulimit -c "$core_dump_limit"


chmod +x /opt/starrocks/s3sync

# start inotifywait loop daemon to monitor core dump generation
${STARROCKS_HOME}/bin/upload_coredump.sh &

# Loop to monitor and restart the daemon child process if it's dead
while true; do
    # Start starrocks_be process
    bash ${STARROCKS_HOME}/bin/start_backend.sh --be $@

    # Print a message indicating the failure
    echo "starrocks_be process exited with status: $?"
    echo "Restarting..."

    # Wait for a few seconds before restarting
    sleep 5
done
