#!/usr/bin/env python
# -- coding: utf-8 --
###########################################################################
#
# Copyright (c) 2020 Copyright (c) 2020, Dingshi Inc.  All rights reserved.
#
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
