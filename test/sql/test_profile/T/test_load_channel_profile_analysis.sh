#!/bin/bash

#
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
#

function check_keywords() {
    profile=$1
    keywords=$2
    expect_num=$3

    actual_num=$(echo -e "${profile}" | grep -c "${keywords}")
    if [ "${expect_num}" -eq "${actual_num}" ]; then
        echo "Analyze profile succeeded"
    else
        echo "Analyze profile failed, keywords: ${keywords}, expect_num: ${expect_num}, actual_num: ${actual_num}"
        echo -e "${profile}"
    fi
}

sql=$(cat << EOF
SET enable_profile=true;
SET enable_async_profile=false;
INSERT INTO t0 SELECT * FROM t0;
SELECT get_query_profile(last_query_id());
EOF
)

output=$(${mysql_cmd} -e "$sql")
check_keywords "$output" "LoadChannel" "1"
check_keywords "$output" "Index (id=" "3"