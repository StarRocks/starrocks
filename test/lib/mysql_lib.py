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
import pymysql
import pymysql as _mysql
from dbutils.pooled_db import PooledDB
from pymysql.constants import CLIENT

from cup import log

from lib import close_conn
from lib.connection_base_lib import BaseConnectionLib, SQLRawResult


class MysqlLib(BaseConnectionLib):
    """MysqlLib class"""

    def __init__(self, conn=""):
        self.connector = conn

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
            close_conn(self.connector, "MySQL")
            self.connector = ""

    def execute(self, sql) -> SQLRawResult:
        with self.connector.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()

            if isinstance(result, tuple):
                result = list(result)
                for row_i, row in enumerate(result):
                    row = list(row)
                    # type to str
                    for col_i, col_data in enumerate(row):
                        if isinstance(col_data, bytes):
                            try:
                                row[col_i] = col_data.decode()
                            except UnicodeDecodeError as e:
                                log.info("decode sql result by utf-8 error, try str")
                                row[col_i] = str(col_data)

                    result[row_i] = tuple(row)
                result = tuple(result)

            return SQLRawResult(status=True, result=result, msg=cursor._result.message, desc=cursor.description)

    def wrapper(self, conn):
        return MysqlLib(conn)

    def create_pool(self, host: str, mysql_port: int, arrow_port: int, user: str, password: str) -> PooledDB:
        return PooledDB(
            creator=pymysql,
            mincached=3,
            blocking=True,
            host=host,
            port=mysql_port,
            user=user,
            password=password,
        )
