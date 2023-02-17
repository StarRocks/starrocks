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
"""
mysql_lib.py

@Time : 2022/11/03 10:34
@Author : Brook.Ye
"""

import pymysql as _mysql
from pymysql.constants import CLIENT


class MysqlLib(object):
    """MysqlLib class"""

    def __init__(self):
        self.connector = ""

    def connect(self, query_dict):
        if "database" not in query_dict:
            self.connector = _mysql.connect(
                host=query_dict["host"],
                user=query_dict["user"],
                port=int(query_dict["port"]),
                passwd=query_dict["password"],
                client_flag=CLIENT.MULTI_STATEMENTS,
            )
        else:
            self.connector = _mysql.connect(
                host=query_dict["host"],
                user=query_dict["user"],
                passwd=query_dict["password"],
                db=query_dict["database"],
                client_flag=CLIENT.MULTI_STATEMENTS,
            )

    def close(self):
        if self.connector != "":
            self.connector.close()
