#!/usr/bin/env python
# -- coding: utf-8 --
###########################################################################
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
###########################################################################
import json
from enum import Enum

from cup import log
from timeout_decorator import timeout, TimeoutError

RESULT_FLAG = "-- result:"
RESULT_END_FLAT = "-- !result"
SHELL_FLAG = "shell: "
TRINO_FLAG = "trino: "
SPARK_FLAG = "spark: "
HIVE_FLAG = "hive: "
ARROW_FLAG = "arrow: "
FUNCTION_FLAG = "function: "
NAME_FLAG = "-- name: "
UNCHECK_FLAG = "[UC]"
ORDER_FLAG = "[ORDER]"
REGEX_FLAG = "[REGEX]"

# loop -- end loop
LOOP_FLAG = "LOOP"
END_LOOP_FLAG = "END LOOP"
CHECK_FLAG = "CHECK: "
PROPERTY_FLAG = "PROPERTY: "

# concurrency -- end concurrency
CONCURRENCY_FLAG = "CONCURRENCY"
END_CONCURRENCY_FLAG = "END CONCURRENCY"


def close_conn(conn, conn_type):
    log.info(f"Try to close {conn_type} connection...")

    try:
        __close_conn(conn)
    except TimeoutError as e:
        log.warning("[WARN] Close %s connection timeout!" % conn_type)

    log.info(f"Close {conn_type} connection success!")


@timeout(10)
def __close_conn(conn):
    conn.close()


class ColorEnum(Enum):
    """ text color """

    RED = 31
    GREEN = 32
    YELLOW = 33
    BLUE = 34
    MAGENTA = 35
    CYAN = 36


def to_array(ori_str, line_separator="\n", separator="\t"):
    """ to array """
    res_list = []
    if ori_str:
        line_list = ori_str.split(line_separator)
        for each_line in line_list:
            res_list.append(each_line.split(separator))

    return res_list


def to_json(ori_str):
    """ to json """
    json_res = {}
    if ori_str:
        json_res = json.loads(ori_str)

    return json_res
