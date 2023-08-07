# !/usr/bin/env python
# -*- coding: utf-8 -*-
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
