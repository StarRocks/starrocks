# !/usr/bin/env python
# -*- coding: utf-8 -*-
###########################################################################
# 
# Copyright (c) 2020, Dingshi Inc.
# All rights reserved.
#
###########################################################################

"""
data_delete_lib
@Time : 2022/11/14 16:10
@Author : Brook.Ye
"""


class DataDeleteLib(object):
    """handle data inserting"""

    def __init__(self):
        self.sql = ""
        self.args = {}

    def set_table_schema(self, args_dict):
        self.args = args_dict

    def get_delete_schema(self):
        """return delete sql"""
        sql = "delete from "
        if "database_name" in self.args:
            sql += "%s." % (self.args["database_name"])
        if "table_name" in self.args:
            sql += "%s " % (self.args["table_name"])
        if "partition_desc" in self.args:
            sql += "partition (%s) " % (", ".join(self.args["partition_desc"]))
        if "query" in self.args and self.args["query"]:
            sql += "where " + self.args["query"]
        sql += ";"
        return sql
